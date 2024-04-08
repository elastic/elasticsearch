/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.ql.execution.search.extractor.AbstractFieldHitExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.ComputingExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.ConstantExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.TotalHitsExtractor;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.AggExtractorInput;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.AggPathInput;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.HitExtractorInput;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.ReferenceInput;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.type.Schema;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.execution.search.extractor.CompositeKeyExtractor;
import org.elasticsearch.xpack.sql.execution.search.extractor.FieldHitExtractor;
import org.elasticsearch.xpack.sql.execution.search.extractor.MetricAggExtractor;
import org.elasticsearch.xpack.sql.execution.search.extractor.PivotExtractor;
import org.elasticsearch.xpack.sql.execution.search.extractor.TopHitsAggExtractor;
import org.elasticsearch.xpack.sql.planner.PlanningException;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;
import org.elasticsearch.xpack.sql.querydsl.container.ComputedRef;
import org.elasticsearch.xpack.sql.querydsl.container.GlobalCountRef;
import org.elasticsearch.xpack.sql.querydsl.container.GroupByRef;
import org.elasticsearch.xpack.sql.querydsl.container.MetricAggRef;
import org.elasticsearch.xpack.sql.querydsl.container.PivotColumnRef;
import org.elasticsearch.xpack.sql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.sql.querydsl.container.ScriptFieldRef;
import org.elasticsearch.xpack.sql.querydsl.container.SearchHitFieldRef;
import org.elasticsearch.xpack.sql.querydsl.container.TopHitsAggRef;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.ListCursor;
import org.elasticsearch.xpack.sql.session.RowSet;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;
import org.elasticsearch.xpack.sql.session.SqlSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.ql.execution.search.extractor.AbstractFieldHitExtractor.MultiValueSupport.LENIENT;
import static org.elasticsearch.xpack.ql.execution.search.extractor.AbstractFieldHitExtractor.MultiValueSupport.NONE;
import static org.elasticsearch.xpack.ql.index.VersionCompatibilityChecks.INTRODUCING_UNSIGNED_LONG;

// TODO: add retry/back-off
public class Querier {
    private static final Logger log = LogManager.getLogger(Querier.class);

    private final SqlConfiguration cfg;
    private final Client client;
    private final PlanExecutor planExecutor;

    public Querier(SqlSession session) {
        this.client = session.client();
        this.planExecutor = session.planExecutor();
        this.cfg = session.configuration();
    }

    public void query(List<Attribute> output, QueryContainer query, String index, ActionListener<Page> listener) {
        // prepare the request
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(query, cfg.filter(), cfg.pageSize());

        if (this.cfg.runtimeMappings() != null) {
            sourceBuilder.runtimeMappings(this.cfg.runtimeMappings());
        }

        if (log.isTraceEnabled()) {
            log.trace("About to execute query {} on {}", StringUtils.toString(sourceBuilder), index);
        }

        SearchRequest search = prepareRequest(
            sourceBuilder,
            cfg,
            query.shouldIncludeFrozen(),
            Strings.commaDelimitedListToStringArray(index)
        );

        @SuppressWarnings("rawtypes")
        List<Tuple<Integer, Comparator>> sortingColumns = query.sortingColumns();
        listener = sortingColumns.isEmpty() ? listener : new LocalAggregationSorterListener(listener, sortingColumns, query.limit());

        if (cfg.task() != null && cfg.task().isCancelled()) {
            listener.onFailure(new TaskCancelledException("cancelled"));
        } else if (query.isAggsOnly()) {
            if (query.aggs().useImplicitGroupBy()) {
                client.search(search, new ImplicitGroupActionListener(listener, client, cfg, output, query, search));
            } else {
                searchWithPointInTime(search, new CompositeActionListener(listener, client, cfg, output, query, search));
            }
        } else {
            searchWithPointInTime(search, new SearchHitActionListener(listener, client, cfg, output, query, sourceBuilder));
        }
    }

    private void searchWithPointInTime(SearchRequest search, ActionListener<SearchResponse> listener) {
        final OpenPointInTimeRequest openPitRequest = new OpenPointInTimeRequest(search.indices()).indicesOptions(search.indicesOptions())
            .keepAlive(cfg.pageTimeout());

        client.execute(
            TransportOpenPointInTimeAction.TYPE,
            openPitRequest,
            listener.delegateFailureAndWrap((delegate, openPointInTimeResponse) -> {
                String pitId = openPointInTimeResponse.getPointInTimeId();
                search.indicesOptions(SearchRequest.DEFAULT_INDICES_OPTIONS);
                search.indices(Strings.EMPTY_ARRAY);
                search.source().pointInTimeBuilder(new PointInTimeBuilder(pitId));
                ActionListener<SearchResponse> closePitOnErrorListener = wrap(searchResponse -> {
                    try {
                        delegate.onResponse(searchResponse);
                    } catch (Exception e) {
                        closePointInTimeAfterError(client, pitId, e, delegate);
                    }
                }, searchError -> closePointInTimeAfterError(client, pitId, searchError, delegate));
                client.search(search, closePitOnErrorListener);
            })
        );
    }

    private static void closePointInTimeAfterError(Client client, String pointInTimeId, Exception e, ActionListener<?> listener) {
        closePointInTime(client, pointInTimeId, wrap(r -> listener.onFailure(e), closeError -> {
            e.addSuppressed(closeError);
            listener.onFailure(e);
        }));
    }

    public static void closePointInTime(Client client, String pointInTimeId, ActionListener<Boolean> listener) {
        if (pointInTimeId != null) {
            // request should not be made with the parent task assigned because the parent task might already be canceled
            client = client instanceof ParentTaskAssigningClient wrapperClient ? wrapperClient.unwrap() : client;

            client.execute(
                TransportClosePointInTimeAction.TYPE,
                new ClosePointInTimeRequest(pointInTimeId),
                listener.delegateFailureAndWrap((l, clearPointInTimeResponse) -> l.onResponse(clearPointInTimeResponse.isSucceeded()))
            );
        } else {
            listener.onResponse(true);
        }
    }

    public static SearchRequest prepareRequest(SearchSourceBuilder source, SqlConfiguration cfg, boolean includeFrozen, String... indices) {
        source.timeout(cfg.requestTimeout());

        SearchRequest searchRequest = new SearchRequest(INTRODUCING_UNSIGNED_LONG);
        if (source.pointInTimeBuilder() == null) {
            searchRequest.indices(indices);
            searchRequest.indicesOptions(
                includeFrozen ? IndexResolver.FIELD_CAPS_FROZEN_INDICES_OPTIONS : IndexResolver.FIELD_CAPS_INDICES_OPTIONS
            );
        }
        searchRequest.source(source);
        searchRequest.allowPartialSearchResults(cfg.allowPartialSearchResults());
        return searchRequest;
    }

    protected static void logSearchResponse(SearchResponse response, Logger logger) {
        List<InternalAggregation> aggs = Collections.emptyList();
        if (response.getAggregations() != null) {
            aggs = response.getAggregations().asList();
        }
        StringBuilder aggsNames = new StringBuilder();
        for (int i = 0; i < aggs.size(); i++) {
            aggsNames.append(aggs.get(i).getName() + (i + 1 == aggs.size() ? "" : ", "));
        }

        var totalHits = response.getHits().getTotalHits();
        var hits = totalHits != null ? "hits " + totalHits.relation + " " + totalHits.value + ", " : "";
        logger.trace(
            "Got search response [{}{} aggregations: [{}], {} failed shards, {} skipped shards, "
                + "{} successful shards, {} total shards, took {}, timed out [{}]]",
            hits,
            aggs.size(),
            aggsNames,
            response.getFailedShards(),
            response.getSkippedShards(),
            response.getSuccessfulShards(),
            response.getTotalShards(),
            response.getTook(),
            response.isTimedOut()
        );
    }

    /**
     * Deserializes the search source from a byte array.
     */
    public static SearchSourceBuilder deserializeQuery(NamedWriteableRegistry registry, byte[] source) throws IOException {
        try (NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(source), registry)) {
            return new SearchSourceBuilder(in);
        }
    }

    /**
     * Serializes the search source to a byte array.
     */
    public static byte[] serializeQuery(SearchSourceBuilder source) throws IOException {
        if (source == null) {
            return new byte[0];
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            source.writeTo(out);
            return BytesReference.toBytes(out.bytes());
        }
    }

    /**
     * Listener used for local sorting (typically due to aggregations used inside `ORDER BY`).
     * <p>
     * This listener consumes the whole result set, sorts it in memory then sends the paginated
     * results back to the client.
     */
    @SuppressWarnings("rawtypes")
    class LocalAggregationSorterListener extends DelegatingActionListener<Page, Page> {
        // keep the top N entries.
        private final AggSortingQueue data;
        private final AtomicInteger counter = new AtomicInteger();
        private volatile Schema schema;

        // Note: when updating this value propagate it to the limitations.asciidoc page as well.
        private static final int MAXIMUM_SIZE = MultiBucketConsumerService.DEFAULT_MAX_BUCKETS;
        private final boolean noLimit;

        LocalAggregationSorterListener(ActionListener<Page> listener, List<Tuple<Integer, Comparator>> sortingColumns, int limit) {
            super(listener);

            int size = MAXIMUM_SIZE;
            if (limit < 0) {
                noLimit = true;
            } else {
                noLimit = false;
                if (limit > MAXIMUM_SIZE) {
                    throw new PlanningException("The maximum LIMIT for aggregate sorting is [{}], received [{}]", MAXIMUM_SIZE, limit);
                } else {
                    size = limit;
                }
            }

            this.data = new AggSortingQueue(size, sortingColumns);
        }

        @Override
        public void onResponse(Page page) {
            // schema is set on the first page (as the rest don't hold the schema anymore)
            if (schema == null) {
                RowSet rowSet = page.rowSet();
                if (rowSet instanceof SchemaRowSet schemaRowSet) {
                    schema = schemaRowSet.schema();
                } else {
                    onFailure(new SqlIllegalArgumentException("No schema found inside {}", rowSet.getClass()));
                    return;
                }
            }

            // 1. consume all pages received
            if (consumeRowSet(page.rowSet()) == false) {
                return;
            }

            Cursor cursor = page.next();
            // 1a. trigger a next call if there's still data
            if (cursor != Cursor.EMPTY) {
                // trigger a next call
                planExecutor.nextPageInternal(cfg, cursor, this);
                // make sure to bail out afterwards as we'll get called by a different thread
                return;
            }

            // no more data available, the last thread sends the response
            // 2. send the in-memory view to the client
            sendResponse();
        }

        private boolean consumeRowSet(RowSet rowSet) {
            ResultRowSet<?> rrs = (ResultRowSet<?>) rowSet;
            for (boolean hasRows = rrs.hasCurrentRow(); hasRows; hasRows = rrs.advanceRow()) {
                List<Object> row = new ArrayList<>(rrs.columnCount());
                rrs.forEachResultColumn(row::add);
                // if the queue overflows and no limit was specified, throw an error
                if (data.insertWithOverflow(new Tuple<>(row, counter.getAndIncrement())) != null && noLimit) {
                    onFailure(
                        new SqlIllegalArgumentException(
                            "The default limit [{}] for aggregate sorting has been reached; please specify a LIMIT",
                            MAXIMUM_SIZE
                        )
                    );
                    return false;
                }
            }
            return true;
        }

        private void sendResponse() {
            delegate.onResponse(ListCursor.of(schema, data.asList(), cfg.pageSize()));
        }
    }

    /**
     * Dedicated listener for implicit/default group-by queries that return only _one_ result.
     */
    static class ImplicitGroupActionListener extends BaseAggActionListener {

        private static final List<? extends Bucket> EMPTY_BUCKET = singletonList(new Bucket() {

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                throw new SqlIllegalArgumentException("No group-by/aggs defined");
            }

            @Override
            public Object getKey() {
                throw new SqlIllegalArgumentException("No group-by/aggs defined");
            }

            @Override
            public String getKeyAsString() {
                throw new SqlIllegalArgumentException("No group-by/aggs defined");
            }

            @Override
            public long getDocCount() {
                throw new SqlIllegalArgumentException("No group-by/aggs defined");
            }

            @Override
            public InternalAggregations getAggregations() {
                throw new SqlIllegalArgumentException("No group-by/aggs defined");
            }
        });

        ImplicitGroupActionListener(
            ActionListener<Page> listener,
            Client client,
            SqlConfiguration cfg,
            List<Attribute> output,
            QueryContainer query,
            SearchRequest request
        ) {
            super(listener, client, cfg, output, query, request);
        }

        @Override
        protected void handleResponse(SearchResponse response, ActionListener<Page> listener) {
            if (log.isTraceEnabled()) {
                logSearchResponse(response, log);
            }

            InternalAggregations aggs = response.getAggregations();
            if (aggs != null) {
                InternalAggregation agg = aggs.get(Aggs.ROOT_GROUP_NAME);
                if (agg instanceof Filters filters) {
                    handleBuckets(filters.getBuckets(), response);
                } else {
                    throw new SqlIllegalArgumentException("Unrecognized root group found; {}", agg.getClass());
                }
            }
            // can happen when only a count is requested which is derived from the response
            else {
                handleBuckets(EMPTY_BUCKET, response);
            }
        }

        private void handleBuckets(List<? extends Bucket> buckets, SearchResponse response) {
            if (buckets.size() == 1) {
                Bucket implicitGroup = buckets.get(0);
                List<BucketExtractor> extractors = initBucketExtractors(response);

                Object[] values = new Object[mask.cardinality()];

                int index = 0;
                for (int i = mask.nextSetBit(0); i >= 0; i = mask.nextSetBit(i + 1)) {
                    values[index++] = extractors.get(i).extract(implicitGroup);
                }
                delegate.onResponse(Page.last(Rows.singleton(schema, values)));

            } else if (buckets.isEmpty()) {
                delegate.onResponse(Page.last(Rows.empty(schema)));
            } else {
                throw new SqlIllegalArgumentException(
                    "Too many groups returned by the implicit group; expected 1, received {}",
                    buckets.size()
                );
            }
        }
    }

    /**
     * Dedicated listener for composite aggs/group-by results.
     */
    static class CompositeActionListener extends BaseAggActionListener {

        private final boolean isPivot;

        CompositeActionListener(
            ActionListener<Page> listener,
            Client client,
            SqlConfiguration cfg,
            List<Attribute> output,
            QueryContainer query,
            SearchRequest request
        ) {
            super(listener, client, cfg, output, query, request);

            isPivot = query.fields().stream().anyMatch(t -> t.extraction() instanceof PivotColumnRef);
        }

        @Override
        protected void handleResponse(SearchResponse response, ActionListener<Page> listener) {
            CompositeAggregationBuilder aggregation = CompositeAggCursor.getCompositeBuilder(request.source());
            boolean mightProducePartialPages = CompositeAggCursor.couldProducePartialPages(aggregation);

            Supplier<CompositeAggRowSet> makeRowSet = isPivot
                ? () -> new PivotRowSet(
                    schema,
                    initBucketExtractors(response),
                    mask,
                    response,
                    aggregation.size(),
                    query.sortingColumns().isEmpty() ? query.limit() : -1,
                    null,
                    mightProducePartialPages
                )
                : () -> new SchemaCompositeAggRowSet(
                    schema,
                    initBucketExtractors(response),
                    mask,
                    response,
                    aggregation.size(),
                    query.sortingColumns().isEmpty() ? query.limit() : -1,
                    mightProducePartialPages
                );

            BiFunction<SearchSourceBuilder, CompositeAggRowSet, CompositeAggCursor> makeCursor = isPivot ? (q, r) -> {
                Map<String, Object> lastAfterKey = r instanceof PivotRowSet ? ((PivotRowSet) r).lastAfterKey() : null;
                return new PivotCursor(
                    lastAfterKey,
                    q,
                    r.extractors(),
                    r.mask(),
                    r.remainingData(),
                    query.shouldIncludeFrozen(),
                    request.indices()
                );
            }
                : (q, r) -> new CompositeAggCursor(
                    q,
                    r.extractors(),
                    r.mask(),
                    r.remainingData,
                    query.shouldIncludeFrozen(),
                    request.indices()
                );

            CompositeAggCursor.handle(
                client,
                response,
                request.source(),
                makeRowSet,
                makeCursor,
                () -> client.search(request, this),
                listener,
                mightProducePartialPages
            );
        }
    }

    abstract static class BaseAggActionListener extends BaseActionListener {
        final QueryContainer query;
        final SearchRequest request;
        final BitSet mask;

        BaseAggActionListener(
            ActionListener<Page> listener,
            Client client,
            SqlConfiguration cfg,
            List<Attribute> output,
            QueryContainer query,
            SearchRequest request
        ) {
            super(listener, client, cfg, output);

            this.query = query;
            this.request = request;
            this.mask = query.columnMask(output);
        }

        protected List<BucketExtractor> initBucketExtractors(SearchResponse response) {
            // create response extractors for the first time
            List<QueryContainer.FieldInfo> refs = query.fields();

            List<BucketExtractor> exts = new ArrayList<>(refs.size());
            TotalHits totalHits = response.getHits().getTotalHits();
            ConstantExtractor totalCount = new TotalHitsExtractor(totalHits == null ? -1L : totalHits.value);
            for (QueryContainer.FieldInfo ref : refs) {
                exts.add(createExtractor(ref.extraction(), totalCount));
            }
            return exts;
        }

        private BucketExtractor createExtractor(FieldExtraction ref, ConstantExtractor totalCount) {
            if (ref instanceof GroupByRef r) {
                return new CompositeKeyExtractor(r.key(), r.property(), cfg.zoneId(), r.dataType());
            }

            if (ref instanceof MetricAggRef r) {
                return new MetricAggExtractor(r.name(), r.property(), r.innerKey(), cfg.zoneId(), r.dataType());
            }

            if (ref instanceof TopHitsAggRef r) {
                return new TopHitsAggExtractor(r.name(), r.fieldDataType(), cfg.zoneId());
            }

            if (ref instanceof PivotColumnRef r) {
                return new PivotExtractor(createExtractor(r.pivot(), totalCount), createExtractor(r.agg(), totalCount), r.value());
            }

            if (ref == GlobalCountRef.INSTANCE) {
                return totalCount;
            }

            if (ref instanceof ComputedRef computedRef) {
                Pipe proc = computedRef.processor();

                // wrap only agg inputs
                proc = proc.transformDown(AggPathInput.class, l -> {
                    BucketExtractor be = createExtractor(l.context(), totalCount);
                    return new AggExtractorInput(l.source(), l.expression(), l.action(), be);
                });

                return new ComputingExtractor(proc.asProcessor());
            }

            throw new SqlIllegalArgumentException("Unexpected value reference {}", ref.getClass());
        }
    }

    /**
     * Dedicated listener for column retrieval/non-grouped queries (search hits).
     */
    static class SearchHitActionListener extends BaseActionListener {
        private final QueryContainer query;
        private final BitSet mask;
        private final AbstractFieldHitExtractor.MultiValueSupport multiValueFieldLeniency;
        private final SearchSourceBuilder source;

        SearchHitActionListener(
            ActionListener<Page> listener,
            Client client,
            SqlConfiguration cfg,
            List<Attribute> output,
            QueryContainer query,
            SearchSourceBuilder source
        ) {
            super(listener, client, cfg, output);
            this.query = query;
            this.mask = query.columnMask(output);
            this.multiValueFieldLeniency = cfg.multiValueFieldLeniency() ? LENIENT : NONE;
            this.source = source;
        }

        @Override
        protected void handleResponse(SearchResponse response, ActionListener<Page> listener) {
            // create response extractors for the first time
            List<QueryContainer.FieldInfo> refs = query.fields();

            List<HitExtractor> exts = new ArrayList<>(refs.size());
            for (QueryContainer.FieldInfo ref : refs) {
                exts.add(createExtractor(ref.extraction()));
            }

            SearchHitCursor.handle(
                client,
                response,
                source,
                () -> new SchemaSearchHitRowSet(schema, exts, mask, source.size(), query.limit(), response),
                listener,
                query.shouldIncludeFrozen(),
                query.allowPartialSearchResults()
            );
        }

        private HitExtractor createExtractor(FieldExtraction ref) {
            if (ref instanceof SearchHitFieldRef f) {
                return new FieldHitExtractor(f.name(), f.getDataType(), cfg.zoneId(), f.hitName(), multiValueFieldLeniency);
            }

            if (ref instanceof ScriptFieldRef f) {
                FieldHitExtractor fieldHitExtractor = new FieldHitExtractor(f.name(), null, cfg.zoneId(), multiValueFieldLeniency);
                return fieldHitExtractor;
            }

            if (ref instanceof ComputedRef computedRef) {
                Pipe proc = computedRef.processor();
                // collect hitNames
                Set<String> hitNames = new LinkedHashSet<>();
                proc = proc.transformDown(ReferenceInput.class, l -> {
                    HitExtractor he = createExtractor(l.context());
                    hitNames.add(he.hitName());

                    if (hitNames.size() > 1) {
                        throw new SqlIllegalArgumentException("Multi-level nested fields [{}] not supported yet", hitNames);
                    }

                    return new HitExtractorInput(l.source(), l.expression(), he);
                });
                String hitName = null;
                if (hitNames.size() == 1) {
                    hitName = hitNames.iterator().next();
                }
                return new ComputingExtractor(proc.asProcessor(), hitName);
            }

            throw new SqlIllegalArgumentException("Unexpected value reference {}", ref.getClass());
        }
    }

    /**
     * Base listener class providing clean-up and exception handling.
     * Handles both search hits and composite-aggs queries.
     */
    abstract static class BaseActionListener extends DelegatingActionListener<SearchResponse, Page> {

        private static final int MAX_WARNING_HEADERS = 20;

        final Client client;
        final SqlConfiguration cfg;
        final Schema schema;

        BaseActionListener(ActionListener<Page> listener, Client client, SqlConfiguration cfg, List<Attribute> output) {
            super(listener);

            this.client = client;
            this.cfg = cfg;
            this.schema = Rows.schema(output);
        }

        @Override
        public void onResponse(final SearchResponse response) {
            if (cfg.allowPartialSearchResults() && response.getFailedShards() > 0) {
                handleShardFailures(response);
            }
            handleResponse(response, delegate);
        }

        protected abstract void handleResponse(SearchResponse response, ActionListener<Page> listener);

        private static void handleShardFailures(SearchResponse response) {
            int count = 0;
            int shardFailuresCount = response.getShardFailures().length;
            for (ShardSearchFailure shardFailure : response.getShardFailures()) {
                HeaderWarning.addWarning(shardFailure.getCause().toString());
                if (++count >= MAX_WARNING_HEADERS - 1 && shardFailuresCount > count) {
                    int remaining = shardFailuresCount - count;
                    HeaderWarning.addWarning(remaining + " remaining shard failure" + (remaining > 1 ? "s" : "") + " suppressed");
                    break;
                }
            }
        }
    }

    @SuppressWarnings("rawtypes")
    static class AggSortingQueue extends PriorityQueue<Tuple<List<?>, Integer>> {

        private List<Tuple<Integer, Comparator>> sortingColumns;

        AggSortingQueue(int maxSize, List<Tuple<Integer, Comparator>> sortingColumns) {
            super(maxSize);
            this.sortingColumns = sortingColumns;
        }

        /**
         * Compare row based on the received attribute sort
         * <ul>
         *     <li>
         *         If a tuple in {@code sortingColumns} has a null comparator, it is assumed the sorting
         *         happened in ES and the results are left as is (by using the row ordering), otherwise it is
         *         sorted based on the given criteria.
         *     </li>
         *     <li>
         *         If no tuple exists in {@code sortingColumns} for an output column, it means this column
         *         is not included at all in the ORDER BY
         *     </li>
         * </ul>
         * <p>
         * Take for example ORDER BY a, x, b, y
         * a, b - are sorted in ES
         * x, y - need to be sorted client-side
         * sorting on x kicks in only if the values for a are equal.
         * sorting on y kicks in only if the values for a, x and b are all equal
         */
        // thanks to @jpountz for the row ordering idea as a way to preserve ordering
        @SuppressWarnings("unchecked")
        @Override
        protected boolean lessThan(Tuple<List<?>, Integer> l, Tuple<List<?>, Integer> r) {
            for (Tuple<Integer, Comparator> tuple : sortingColumns) {
                int columnIdx = tuple.v1().intValue();
                Comparator comparator = tuple.v2();

                // Get the values for left and right rows at the current column index
                Object vl = l.v1().get(columnIdx);
                Object vr = r.v1().get(columnIdx);
                if (comparator != null) {
                    int result = comparator.compare(vl, vr);
                    // if things are not equal: return the comparison result,
                    // otherwise: move to the next comparator to solve the tie.
                    if (result != 0) {
                        return result > 0;
                    }
                }
                // no comparator means the rows are pre-ordered by ES for the column at
                // the current index and the existing order needs to be preserved
                else {
                    // check the values - if they are not equal return the row order
                    // otherwise: move to the next comparator to solve the tie.
                    if (Objects.equals(vl, vr) == false) {
                        return l.v2().compareTo(r.v2()) > 0;
                    }
                }
            }
            // everything is equal, fall-back to the row order
            return l.v2().compareTo(r.v2()) > 0;
        }

        List<List<?>> asList() {
            List<List<?>> list = new ArrayList<>(super.size());
            Tuple<List<?>, Integer> pop;
            while ((pop = pop()) != null) {
                list.add(0, pop.v1());
            }
            return list;
        }
    }
}
