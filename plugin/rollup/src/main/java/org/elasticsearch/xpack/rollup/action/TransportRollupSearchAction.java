/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
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
import org.elasticsearch.xpack.core.rollup.action.RollupJobCaps;
import org.elasticsearch.xpack.core.rollup.action.RollupSearchAction;
import org.elasticsearch.xpack.rollup.Rollup;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.rollup.RollupRequestTranslator;
import org.elasticsearch.xpack.rollup.RollupResponseTranslator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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
        Triple<String[], String[], List<RollupJobCaps>> indices = separateIndices(request.indices(),
                clusterService.state().getMetaData().indices());

        MultiSearchRequest msearch = createMSearchRequest(request, registry, indices.v1(), indices.v2(), indices.v3());

        client.multiSearch(msearch, ActionListener.wrap(msearchResponse -> {
            InternalAggregation.ReduceContext context
                    = new InternalAggregation.ReduceContext(bigArrays, scriptService, false);
            listener.onResponse(processResponses(indices, msearchResponse, context));
        }, listener::onFailure));
    }

    static SearchResponse processResponses(Triple<String[], String[], List<RollupJobCaps>> indices, MultiSearchResponse msearchResponse,
                                           InternalAggregation.ReduceContext context) {
        if (indices.v1.length > 0 && indices.v2.length > 0) {
            // Both
            assert(msearchResponse.getResponses().length == 2);
            return RollupResponseTranslator.combineResponses(msearchResponse.getResponses()[0], msearchResponse.getResponses()[1], context);
        } else if (indices.v1.length > 0) {
            // Only live
            assert(msearchResponse.getResponses().length == 1);
            return RollupResponseTranslator.verifyResponse(msearchResponse.getResponses()[0]);
        } else {
            // Only rollup
            assert(msearchResponse.getResponses().length == 1);
            return RollupResponseTranslator.translateResponse(msearchResponse.getResponses()[0], context);
        }
    }

    static MultiSearchRequest createMSearchRequest(SearchRequest request, NamedWriteableRegistry registry,
                                                   String[] normalIndices, String[] rollupIndices,
                                                   List<RollupJobCaps> jobCaps) {

        if (normalIndices.length == 0 && rollupIndices.length == 0) {
            // Don't support _all on everything right now, for code simplicity
            throw new IllegalArgumentException("Must specify at least one rollup index in _rollup_search API");
        } else if (rollupIndices.length == 0) {
            // not best practice, but if the user accidentally only sends "normal" indices we can support that
            logger.debug("Creating msearch with only normal request");
            final SearchRequest originalRequest = new SearchRequest(normalIndices, request.source());
            return new MultiSearchRequest().add(originalRequest);
        }

        // Rollup only supports a limited subset of the search API, validate and make sure
        // nothing is set that we can't support
        validateSearchRequest(request);
        QueryBuilder rewritten = rewriteQuery(request.source().query(), jobCaps);

        // The original request is added as-is (if normal indices exist), minus the rollup indices
        final SearchRequest originalRequest = new SearchRequest(normalIndices, request.source());
        MultiSearchRequest msearch = new MultiSearchRequest();
        if (normalIndices.length != 0) {
            msearch.add(originalRequest);
        }

        SearchSourceBuilder rolledSearchSource = new SearchSourceBuilder();
        rolledSearchSource.query(rewritten);
        rolledSearchSource.size(0);
        AggregatorFactories.Builder sourceAgg = request.source().aggregations();

        for (AggregationBuilder agg : sourceAgg.getAggregatorFactories()) {

            List<QueryBuilder> filterConditions = new ArrayList<>(5);
            filterConditions.addAll(mandatoryFilterConditions());

            // Translate the agg tree, and collect any potential filtering clauses
            List<AggregationBuilder> translatedAgg = RollupRequestTranslator.translateAggregation(agg, filterConditions, registry, jobCaps);

            BoolQueryBuilder boolQuery = new BoolQueryBuilder();
            filterConditions.forEach(boolQuery::must);
            FilterAggregationBuilder filterAgg = new FilterAggregationBuilder(RollupField.FILTER + "_" + agg.getName(),
                    boolQuery);
            translatedAgg.forEach(filterAgg::subAggregation);
            rolledSearchSource.aggregation(filterAgg);
        }
        SearchRequest rolledSearch = new SearchRequest(rollupIndices, rolledSearchSource)
                .types(request.types());
        msearch.add(rolledSearch);

        logger.debug("Original query\n" + request.toString());
        logger.debug("Translated rollup query:\n" + rolledSearch.toString());
        return msearch;
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

    static QueryBuilder rewriteQuery(QueryBuilder builder, List<RollupJobCaps> jobCaps) {
        if (builder == null) {
            return null;
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
            ((DisMaxQueryBuilder)builder).innerQueries().forEach(query -> rewritten.add(rewriteQuery(query, jobCaps)));
            return rewritten;
        } else if (builder.getWriteableName().equals(RangeQueryBuilder.NAME) || builder.getWriteableName().equals(TermQueryBuilder.NAME)) {

            String fieldName = builder.getWriteableName().equals(RangeQueryBuilder.NAME)
                    ? ((RangeQueryBuilder)builder).fieldName()
                    : ((TermQueryBuilder)builder).fieldName();

            List<String> rewrittenFieldName = jobCaps.stream()
                    // We only care about job caps that have the query's target field
                    .filter(caps -> caps.getFieldCaps().keySet().contains(fieldName))
                    .map(caps -> {
                        RollupJobCaps.RollupFieldCaps fieldCaps = caps.getFieldCaps().get(fieldName);
                        return fieldCaps.getAggs().stream()
                            // For now, we only allow filtering on grouping fields
                            .filter(agg -> {
                                String type = (String)agg.get("agg");
                                return type.equals(TermsAggregationBuilder.NAME)
                                        || type.equals(DateHistogramAggregationBuilder.NAME)
                                        || type.equals(HistogramAggregationBuilder.NAME);
                            })
                            // Rewrite the field name to our convention (e.g. "foo" -> "date_histogram.foo.value")
                            .map(agg -> RollupField.formatFieldName(fieldName, (String)agg.get("agg"), RollupField.VALUE))
                            .collect(Collectors.toList());
                    }).collect(ArrayList::new, List::addAll, List::addAll);

            if (rewrittenFieldName.isEmpty()) {
                throw new IllegalArgumentException("Field [" + fieldName + "] in [" + builder.getWriteableName()
                        + "] query is not available in selected rollup indices, cannot query.");
            }

            if (rewrittenFieldName.size() > 1) {
                throw new IllegalArgumentException("Ambiguous field name resolution when mapping to rolled fields.  Field name [" +
                        fieldName + "] was mapped to: [" + Strings.collectionToDelimitedString(rewrittenFieldName, ",") + "].");
            }

            //Note: instanceof here to make casting checks happier
            if (builder instanceof RangeQueryBuilder) {
                RangeQueryBuilder rewritten = new RangeQueryBuilder(rewrittenFieldName.get(0));
                RangeQueryBuilder original = (RangeQueryBuilder)builder;
                rewritten.from(original.from());
                rewritten.to(original.to());
                if (original.timeZone() != null) {
                    rewritten.timeZone(original.timeZone());
                }
                rewritten.includeLower(original.includeLower());
                rewritten.includeUpper(original.includeUpper());
                return rewritten;
            } else {
                return new TermQueryBuilder(rewrittenFieldName.get(0), ((TermQueryBuilder)builder).value());
            }

        } else if (builder.getWriteableName().equals(MatchAllQueryBuilder.NAME)) {
            // no-op
            return builder;
        } else {
            throw new IllegalArgumentException("Unsupported Query in search request: [" + builder.getWriteableName() + "]");
        }
    }

    static Triple<String[], String[], List<RollupJobCaps>> separateIndices(String[] indices,
                                                                                   ImmutableOpenMap<String, IndexMetaData> indexMetaData) {

        if (indices.length == 0) {
            throw new IllegalArgumentException("Must specify at least one concrete index.");
        }

        List<String> rollup = new ArrayList<>();
        List<String> normal = new ArrayList<>();
        List<RollupJobCaps>  jobCaps = new ArrayList<>();
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
        assert(normal.size() + rollup.size() > 0);
        return new Triple<>(normal.toArray(new String[normal.size()]), rollup.toArray(new String[rollup.size()]), jobCaps);
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

    /**
     * Adds various filter conditions that apply to the entire rollup query, such
     * as the rollup hash and rollup version
     */
    private static List<QueryBuilder> mandatoryFilterConditions() {
        List<QueryBuilder> conditions = new ArrayList<>(1);
        conditions.add(new TermQueryBuilder(RollupField.ROLLUP_META + "." + RollupField.VERSION_FIELD, Rollup.ROLLUP_VERSION));
        return conditions;
    }


    static class Triple<V1, V2, V3> {

        public static <V1, V2, V3> Triple<V1, V2, V3> triple(V1 v1, V2 v2, V3 v3) {
            return new Triple<>(v1, v2, v3);
        }

        private final V1 v1;
        private final V2 v2;
        private final V3 v3;

        Triple(V1 v1, V2 v2, V3 v3) {
            this.v1 = v1;
            this.v2 = v2;
            this.v3 = v3;
        }

        public V1 v1() {
            return v1;
        }

        public V2 v2() {
            return v2;
        }

        public V3 v3() {
            return v3;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Triple<?,?,?> triple = (Triple) o;

            if (v1 != null ? !v1.equals(triple.v1) : triple.v1 != null) return false;
            if (v2 != null ? !v2.equals(triple.v2) : triple.v2 != null) return false;
            if (v3 != null ? !v3.equals(triple.v3) : triple.v3 != null) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = v1 != null ? v1.hashCode() : 0;
            result = 31 * result + (v2 != null ? v2.hashCode() : 0) + (v3 != null ? v3.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Tuple [v1=" + v1 + ", v2=" + v2 + ", v3=" + v3 + "]";
        }
    }
}

