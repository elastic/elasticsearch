/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.RollupJobCaps;
import org.elasticsearch.xpack.core.rollup.action.RollupSearchAction;
import org.elasticsearch.xpack.core.rollup.job.DateHistoGroupConfig;
import org.elasticsearch.xpack.rollup.Rollup;
import org.elasticsearch.xpack.rollup.RollupJobIdentifierUtils;
import org.elasticsearch.xpack.rollup.RollupRequestTranslator;
import org.elasticsearch.xpack.rollup.RollupResponseTranslator;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportRollupSearchAction extends TransportAction<SearchRequest, SearchResponse> {

    private final Client client;
    private final NamedWriteableRegistry registry;
    private final BigArrays bigArrays;
    private final ScriptService scriptService;
    private final ClusterService clusterService;
    private static final Logger logger = Loggers.getLogger(RollupSearchAction.class);

    @Inject
    public TransportRollupSearchAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                 ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                 Client client, NamedWriteableRegistry registry, BigArrays bigArrays,
                                 ScriptService scriptService, ClusterService clusterService) {
        super(settings, RollupSearchAction.NAME, threadPool, actionFilters, indexNameExpressionResolver, transportService.getTaskManager());
        this.client = client;
        this.registry = registry;
        this.bigArrays = bigArrays;
        this.scriptService = scriptService;
        this.clusterService = clusterService;

        transportService.registerRequestHandler(actionName, ThreadPool.Names.SAME, false, true, SearchRequest::new,
                new TransportRollupSearchAction.TransportHandler());
    }

    @Override
    protected void doExecute(SearchRequest request, ActionListener<SearchResponse> listener) {
        RollupSearchContext rollupSearchContext = separateIndices(request.indices(),
                clusterService.state().getMetaData().indices());

        MultiSearchRequest msearch = createMSearchRequest(request, registry, rollupSearchContext);

        client.multiSearch(msearch, ActionListener.wrap(msearchResponse -> {
            InternalAggregation.ReduceContext context
                    = new InternalAggregation.ReduceContext(bigArrays, scriptService, false);
            listener.onResponse(processResponses(rollupSearchContext, msearchResponse, context));
        }, listener::onFailure));
    }

    static SearchResponse processResponses(RollupSearchContext rollupContext, MultiSearchResponse msearchResponse,
                                           InternalAggregation.ReduceContext reduceContext) {
        if (rollupContext.hasLiveIndices() && rollupContext.hasRollupIndices()) {
            // Both
            return RollupResponseTranslator.combineResponses(msearchResponse.getResponses(), reduceContext);
        } else if (rollupContext.hasLiveIndices()) {
            // Only live
            assert msearchResponse.getResponses().length == 1;
            return RollupResponseTranslator.verifyResponse(msearchResponse.getResponses()[0]);
        } else if (rollupContext.hasRollupIndices()) {
            // Only rollup
            return RollupResponseTranslator.translateResponse(msearchResponse.getResponses(), reduceContext);
        }
        throw new RuntimeException("MSearch response was empty, cannot unroll RollupSearch results");
    }

    static MultiSearchRequest createMSearchRequest(SearchRequest request, NamedWriteableRegistry registry, RollupSearchContext context) {

        if (context.hasLiveIndices() == false && context.hasRollupIndices() == false) {
            // Don't support _all on everything right now, for code simplicity
            throw new IllegalArgumentException("Must specify at least one rollup index in _rollup_search API");
        } else if (context.hasLiveIndices() && context.hasRollupIndices() == false) {
            // not best practice, but if the user accidentally only sends "normal" indices we can support that
            logger.debug("Creating msearch with only normal request");
            final SearchRequest originalRequest = new SearchRequest(context.getLiveIndices(), request.source());
            return new MultiSearchRequest().add(originalRequest);
        }

        // Rollup only supports a limited subset of the search API, validate and make sure
        // nothing is set that we can't support
        validateSearchRequest(request);

        // The original request is added as-is (if normal indices exist), minus the rollup indices
        final SearchRequest originalRequest = new SearchRequest(context.getLiveIndices(), request.source());
        MultiSearchRequest msearch = new MultiSearchRequest();
        if (context.hasLiveIndices()) {
            msearch.add(originalRequest);
        }

        SearchSourceBuilder rolledSearchSource = new SearchSourceBuilder();
        rolledSearchSource.size(0);
        AggregatorFactories.Builder sourceAgg = request.source().aggregations();

        // Find our list of "best" job caps
        Set<RollupJobCaps> validatedCaps = new HashSet<>();
        sourceAgg.getAggregatorFactories()
                .forEach(agg -> validatedCaps.addAll(RollupJobIdentifierUtils.findBestJobs(agg, context.getJobCaps())));
        List<String> jobIds = validatedCaps.stream().map(RollupJobCaps::getJobID).collect(Collectors.toList());

        for (AggregationBuilder agg : sourceAgg.getAggregatorFactories()) {

            List<QueryBuilder> filterConditions = new ArrayList<>(5);

            // Translate the agg tree, and collect any potential filtering clauses
            List<AggregationBuilder> translatedAgg = RollupRequestTranslator.translateAggregation(agg, filterConditions, registry);

            BoolQueryBuilder boolQuery = new BoolQueryBuilder();
            filterConditions.forEach(boolQuery::must);
            FilterAggregationBuilder filterAgg = new FilterAggregationBuilder(RollupField.FILTER + "_" + agg.getName(),
                    boolQuery);
            translatedAgg.forEach(filterAgg::subAggregation);
            rolledSearchSource.aggregation(filterAgg);
        }

        // Rewrite the user's query to our internal conventions, checking against the validated job caps
        QueryBuilder rewritten = rewriteQuery(request.source().query(), validatedCaps);

        for (String id : jobIds) {
            SearchSourceBuilder copiedSource;
            try {
                copiedSource = copyWriteable(rolledSearchSource, registry, SearchSourceBuilder::new);
            } catch (IOException e) {
                throw new RuntimeException("Encountered IO exception while trying to build rollup request.", e);
            }

            // filter the rewritten query by JobID
            copiedSource.query(new BoolQueryBuilder()
                    .must(rewritten)
                    .filter(new TermQueryBuilder(RollupField.formatMetaField(RollupField.ID.getPreferredName()), id))
                    .filter(new TermQueryBuilder(RollupField.formatMetaField(RollupField.VERSION_FIELD), Rollup.ROLLUP_VERSION)));

            // And add a new msearch per JobID
            msearch.add(new SearchRequest(context.getRollupIndices(), copiedSource).types(request.types()));
        }

        return msearch;
    }

    /**
     * Lifted from ESTestCase :s  Don't reuse this anywhere!
     *
     * Create a copy of an original {@link SearchSourceBuilder} object by running it through a {@link BytesStreamOutput} and
     * reading it in again using a {@link Writeable.Reader}. The stream that is wrapped around the {@link StreamInput}
     * potentially need to use a {@link NamedWriteableRegistry}, so this needs to be provided too
     */
    private static SearchSourceBuilder copyWriteable(SearchSourceBuilder original, NamedWriteableRegistry namedWriteableRegistry,
                                                        Writeable.Reader<SearchSourceBuilder> reader) throws IOException {
        Writeable.Writer<SearchSourceBuilder> writer = (out, value) -> value.writeTo(out);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.CURRENT);
            writer.write(output, original);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                in.setVersion(Version.CURRENT);
                return reader.read(in);
            }
        }
    }

    static void validateSearchRequest(SearchRequest request) {
        // Rollup does not support hits at the moment
        if (request.source().size() != 0) {
            throw new IllegalArgumentException("Rollup does not support returning search hits, please try again " +
                    "with [size: 0].");
        }

        if (request.source().postFilter() != null) {
            throw new IllegalArgumentException("Rollup search does not support post filtering.");
        }

        if (request.source().suggest() != null) {
            throw new IllegalArgumentException("Rollup search does not support suggestors.");
        }

        if (request.source().highlighter() != null) {
            throw new IllegalArgumentException("Rollup search does not support highlighting.");
        }

        if (request.source().profile()) {
            throw new IllegalArgumentException("Rollup search does not support profiling at the moment.");
        }

        if (request.source().explain() != null && request.source().explain()) {
            throw new IllegalArgumentException("Rollup search does not support explaining.");
        }

        // Rollup is only useful if aggregations are set, throw an exception otherwise
        if (request.source().aggregations() == null) {
            throw new IllegalArgumentException("Rollup requires at least one aggregation to be set.");
        }
    }

    static QueryBuilder rewriteQuery(QueryBuilder builder, Set<RollupJobCaps> jobCaps) {
        if (builder == null) {
            return new MatchAllQueryBuilder();
        }
        if (builder.getWriteableName().equals(BoolQueryBuilder.NAME)) {
            BoolQueryBuilder rewrittenBool = new BoolQueryBuilder();
            ((BoolQueryBuilder)builder).must().forEach(query -> rewrittenBool.must(rewriteQuery(query, jobCaps)));
            ((BoolQueryBuilder)builder).mustNot().forEach(query -> rewrittenBool.mustNot(rewriteQuery(query, jobCaps)));
            ((BoolQueryBuilder)builder).should().forEach(query -> rewrittenBool.should(rewriteQuery(query, jobCaps)));
            ((BoolQueryBuilder)builder).filter().forEach(query -> rewrittenBool.filter(rewriteQuery(query, jobCaps)));
            return rewrittenBool;
        } else if (builder.getWriteableName().equals(ConstantScoreQueryBuilder.NAME)) {
            return new ConstantScoreQueryBuilder(rewriteQuery(((ConstantScoreQueryBuilder)builder).innerQuery(), jobCaps));
        } else if (builder.getWriteableName().equals(BoostingQueryBuilder.NAME)) {
            return new BoostingQueryBuilder(rewriteQuery(((BoostingQueryBuilder)builder).negativeQuery(), jobCaps),
                    rewriteQuery(((BoostingQueryBuilder)builder).positiveQuery(), jobCaps));
        } else if (builder.getWriteableName().equals(DisMaxQueryBuilder.NAME)) {
            DisMaxQueryBuilder rewritten = new DisMaxQueryBuilder();
            ((DisMaxQueryBuilder) builder).innerQueries().forEach(query -> rewritten.add(rewriteQuery(query, jobCaps)));
            return rewritten;
        } else if (builder.getWriteableName().equals(RangeQueryBuilder.NAME)) {
            RangeQueryBuilder range = (RangeQueryBuilder) builder;
            String fieldName = range.fieldName();
            // Many range queries don't include the timezone because the default is UTC, but the query
            // builder will return null so we need to set it here
            String timeZone = range.timeZone() == null ? DateTimeZone.UTC.toString() : range.timeZone();

            String rewrittenFieldName = rewriteFieldName(jobCaps, RangeQueryBuilder.NAME, fieldName, timeZone);
            RangeQueryBuilder rewritten = new RangeQueryBuilder(rewrittenFieldName)
                .from(range.from())
                .to(range.to())
                .includeLower(range.includeLower())
                .includeUpper(range.includeUpper());
            if (range.timeZone() != null) {
                rewritten.timeZone(range.timeZone());
            }
            if (range.format() != null) {
                rewritten.format(range.format());
            }
            return rewritten;
        } else if (builder.getWriteableName().equals(TermQueryBuilder.NAME)) {
            TermQueryBuilder term = (TermQueryBuilder) builder;
            String fieldName = term.fieldName();
            String rewrittenFieldName =  rewriteFieldName(jobCaps, TermQueryBuilder.NAME, fieldName, null);
            return new TermQueryBuilder(rewrittenFieldName, term.value());
        } else if (builder.getWriteableName().equals(TermsQueryBuilder.NAME)) {
            TermsQueryBuilder terms = (TermsQueryBuilder) builder;
            String fieldName = terms.fieldName();
            String rewrittenFieldName =  rewriteFieldName(jobCaps, TermQueryBuilder.NAME, fieldName, null);
            return new TermsQueryBuilder(rewrittenFieldName, terms.values());
        } else if (builder.getWriteableName().equals(MatchAllQueryBuilder.NAME)) {
            // no-op
            return builder;
        } else {
            throw new IllegalArgumentException("Unsupported Query in search request: [" + builder.getWriteableName() + "]");
        }
    }

    private static String rewriteFieldName(Set<RollupJobCaps> jobCaps,
                                           String builderName,
                                           String fieldName,
                                           String timeZone) {
        List<String> incompatibleTimeZones = timeZone == null ? Collections.emptyList() : new ArrayList<>();
        List<String> rewrittenFieldNames = jobCaps.stream()
            // We only care about job caps that have the query's target field
            .filter(caps -> caps.getFieldCaps().keySet().contains(fieldName))
            .map(caps -> {
                RollupJobCaps.RollupFieldCaps fieldCaps = caps.getFieldCaps().get(fieldName);
                return fieldCaps.getAggs().stream()
                    // For now, we only allow filtering on grouping fields
                    .filter(agg -> {
                        String type = (String)agg.get(RollupField.AGG);

                        // If the cap is for a date_histo, and the query is a range, the timezones need to match
                        if (type.equals(DateHistogramAggregationBuilder.NAME) && timeZone != null) {
                            boolean matchingTZ = ((String)agg.get(DateHistoGroupConfig.TIME_ZONE.getPreferredName()))
                                .equalsIgnoreCase(timeZone);
                            if (matchingTZ == false) {
                                incompatibleTimeZones.add((String)agg.get(DateHistoGroupConfig.TIME_ZONE.getPreferredName()));
                            }
                            return matchingTZ;
                        }
                        // Otherwise just make sure it's one of the three groups
                        return type.equals(TermsAggregationBuilder.NAME)
                            || type.equals(DateHistogramAggregationBuilder.NAME)
                            || type.equals(HistogramAggregationBuilder.NAME);
                    })
                    // Rewrite the field name to our convention (e.g. "foo" -> "date_histogram.foo.timestamp")
                    .map(agg -> {
                        if (agg.get(RollupField.AGG).equals(DateHistogramAggregationBuilder.NAME)) {
                            return RollupField.formatFieldName(fieldName, (String)agg.get(RollupField.AGG), RollupField.TIMESTAMP);
                        } else {
                            return RollupField.formatFieldName(fieldName, (String)agg.get(RollupField.AGG), RollupField.VALUE);
                        }
                    })
                    .collect(Collectors.toList());
            })
            .distinct()
            .collect(ArrayList::new, List::addAll, List::addAll);
        if (rewrittenFieldNames.isEmpty()) {
            if (incompatibleTimeZones.isEmpty()) {
                throw new IllegalArgumentException("Field [" + fieldName + "] in [" + builderName
                    + "] query is not available in selected rollup indices, cannot query.");
            } else {
                throw new IllegalArgumentException("Field [" + fieldName + "] in [" + builderName
                    + "] query was found in rollup indices, but requested timezone is not compatible. Options include: "
                    + incompatibleTimeZones);
            }
        } else if (rewrittenFieldNames.size() > 1) {
            throw new IllegalArgumentException("Ambiguous field name resolution when mapping to rolled fields.  Field name [" +
                fieldName + "] was mapped to: [" + Strings.collectionToDelimitedString(rewrittenFieldNames, ",") + "].");
        } else {
            return rewrittenFieldNames.get(0);
        }
    }

    static RollupSearchContext separateIndices(String[] indices, ImmutableOpenMap<String, IndexMetaData> indexMetaData) {

        if (indices.length == 0) {
            throw new IllegalArgumentException("Must specify at least one concrete index.");
        }

        List<String> rollup = new ArrayList<>();
        List<String> normal = new ArrayList<>();
        Set<RollupJobCaps>  jobCaps = new HashSet<>();
        Arrays.stream(indices).forEach(i -> {
            if (i.equals(MetaData.ALL)) {
                throw new IllegalArgumentException("Searching _all via RollupSearch endpoint is not supported at this time.");
            }
            Optional<RollupIndexCaps> caps = TransportGetRollupCapsAction.findRollupIndexCaps(i, indexMetaData.get(i));
            if (caps.isPresent()) {
                rollup.add(i);
                jobCaps.addAll(caps.get().getJobCaps());
            } else {
                normal.add(i);
            }
        });
        assert normal.size() + rollup.size() > 0;
        if (rollup.size() > 1) {
            throw new IllegalArgumentException("RollupSearch currently only supports searching one rollup index at a time.");
        }
        return new RollupSearchContext(normal.toArray(new String[normal.size()]), rollup.toArray(new String[rollup.size()]), jobCaps);
    }

    class TransportHandler implements TransportRequestHandler<SearchRequest> {

        @Override
        public final void messageReceived(SearchRequest request, TransportChannel channel) throws Exception {
            throw new UnsupportedOperationException("the task parameter is required for this operation");
        }

        @Override
        public final void messageReceived(final SearchRequest request, final TransportChannel channel, Task task) throws Exception {
            // We already got the task created on the network layer - no need to create it again on the transport layer
            execute(task, request, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    try {
                        channel.sendResponse(response);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn(
                                (org.apache.logging.log4j.util.Supplier<?>)
                                        () -> new ParameterizedMessage(
                                                "Failed to send error response for action [{}] and request [{}]",
                                                actionName,
                                                request),
                                e1);
                    }
                }
            });
        }
    }

    static class RollupSearchContext {
        private final String[] liveIndices;
        private final String[] rollupIndices;
        private final Set<RollupJobCaps> jobCaps;

        RollupSearchContext(String[] liveIndices, String[] rollupIndices, Set<RollupJobCaps> jobCaps) {
            this.liveIndices = Objects.requireNonNull(liveIndices);
            this.rollupIndices = Objects.requireNonNull(rollupIndices);
            this.jobCaps = Objects.requireNonNull(jobCaps);
        }

        boolean hasLiveIndices() {
            return liveIndices.length != 0;
        }

        boolean hasRollupIndices() {
            return rollupIndices.length != 0;
        }

        String[] getLiveIndices() {
            return liveIndices;
        }

        String[] getRollupIndices() {
            return rollupIndices;
        }

        Set<RollupJobCaps> getJobCaps() {
            return jobCaps;
        }

    }
}

