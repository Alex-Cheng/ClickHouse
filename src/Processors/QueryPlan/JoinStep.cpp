#include <Processors/QueryPlan/JoinStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JoinStep::JoinStep(
    const DataStream & left_stream_,
    const DataStream & right_stream_,
    JoinPtr join_,
    size_t max_block_size_,
    size_t max_streams_,
    bool keep_left_read_in_order_,
    size_t shuffle_optimize_buckets_,
    size_t shuffle_optimize_max_)
    : join(std::move(join_)), max_block_size(max_block_size_), max_streams(max_streams_), keep_left_read_in_order(keep_left_read_in_order_), shuffle_optimize_buckets(shuffle_optimize_buckets_), shuffle_optimize_max(shuffle_optimize_max_)
{
    updateInputStreams(DataStreams{left_stream_, right_stream_});
}

QueryPipelineBuilderPtr JoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStep expect two input steps");

    if (join->pipelineType() == JoinPipelineType::YShaped)
    {
        std::unique_ptr<QueryPipelineBuilder> joined_pipeline;
        if (shuffle_optimize_buckets > 1)
        {
            // 例如如果分四个桶，最大值应该至少为3（0,1,2,3）
            if (shuffle_optimize_max < shuffle_optimize_buckets - 1)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Max key value must be at least number of shuffle buckets - 1");
            }

            joined_pipeline = QueryPipelineBuilder::joinPipelinesYShapedWithShuffle(
                std::move(pipelines[0]),
                std::move(pipelines[1]),
                join,
                output_stream->header,
                max_block_size,
                &processors);
            joined_pipeline->resize(max_streams);
        }
        else
        {
            joined_pipeline = QueryPipelineBuilder::joinPipelinesYShaped(
                std::move(pipelines[0]), std::move(pipelines[1]), join, output_stream->header, max_block_size, &processors);
        }
        joined_pipeline->resize(max_streams);
        return joined_pipeline;
    }

    return QueryPipelineBuilder::joinPipelinesRightLeft(
        std::move(pipelines[0]),
        std::move(pipelines[1]),
        join,
        output_stream->header,
        max_block_size,
        max_streams,
        keep_left_read_in_order,
        &processors);
}

bool JoinStep::allowPushDownToRight() const
{
    return join->pipelineType() == JoinPipelineType::YShaped || join->pipelineType() == JoinPipelineType::FillRightFirst;
}

void JoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void JoinStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    const auto & table_join = join->getTableJoin();
    settings.out << prefix << "Type: " << toString(table_join.kind()) << '\n';
    settings.out << prefix << "Strictness: " << toString(table_join.strictness()) << '\n';
    settings.out << prefix << "Algorithm: " << join->getName() << '\n';

    if (table_join.strictness() == JoinStrictness::Asof)
        settings.out << prefix << "ASOF inequality: " << toString(table_join.getAsofInequality()) << '\n';

    if (!table_join.getClauses().empty())
        settings.out << prefix << "Clauses: " << table_join.formatClauses(table_join.getClauses(), true /*short_format*/) << '\n';
}

void JoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    const auto & table_join = join->getTableJoin();
    map.add("Type", toString(table_join.kind()));
    map.add("Strictness", toString(table_join.strictness()));
    map.add("Algorithm", join->getName());

    if (table_join.strictness() == JoinStrictness::Asof)
        map.add("ASOF inequality", toString(table_join.getAsofInequality()));

    if (!table_join.getClauses().empty())
        map.add("Clauses", table_join.formatClauses(table_join.getClauses(), true /*short_format*/));
}

void JoinStep::updateOutputStream()
{
    output_stream = DataStream
    {
        .header = JoiningTransform::transformHeader(input_streams[0].header, join),
    };
}

static ITransformingStep::Traits getStorageJoinTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

FilledJoinStep::FilledJoinStep(const DataStream & input_stream_, JoinPtr join_, size_t max_block_size_)
    : ITransformingStep(
        input_stream_,
        JoiningTransform::transformHeader(input_stream_.header, join_),
        getStorageJoinTraits())
    , join(std::move(join_))
    , max_block_size(max_block_size_)
{
    if (!join->isFilled())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FilledJoinStep expects Join to be filled");
}

void FilledJoinStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    bool default_totals = false;
    if (!pipeline.hasTotals() && join->getTotals())
    {
        pipeline.addDefaultTotals();
        default_totals = true;
    }

    auto finish_counter = std::make_shared<JoiningTransform::FinishCounter>(pipeline.getNumStreams());

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        auto counter = on_totals ? nullptr : finish_counter;
        return std::make_shared<JoiningTransform>(header, output_stream->header, join, max_block_size, on_totals, default_totals, counter);
    });
}

void FilledJoinStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(), JoiningTransform::transformHeader(input_streams.front().header, join), getDataStreamTraits());
}

}
