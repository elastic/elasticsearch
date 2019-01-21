/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.sql.execution.search.extractor.CompositeKeyExtractor;
import org.elasticsearch.xpack.sql.execution.search.extractor.ComputingExtractor;
import org.elasticsearch.xpack.sql.execution.search.extractor.ConstantExtractor;
import org.elasticsearch.xpack.sql.execution.search.extractor.FieldHitExtractor;
import org.elasticsearch.xpack.sql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.sql.execution.search.extractor.MetricAggExtractor;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.AggExtractorInput;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.AggPathInput;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.HitExtractorInput;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.ReferenceInput;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;
import org.elasticsearch.xpack.sql.querydsl.container.ComputedRef;
import org.elasticsearch.xpack.sql.querydsl.container.GlobalCountRef;
import org.elasticsearch.xpack.sql.querydsl.container.GroupByRef;
import org.elasticsearch.xpack.sql.querydsl.container.MetricAggRef;
import org.elasticsearch.xpack.sql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.sql.querydsl.container.ScriptFieldRef;
import org.elasticsearch.xpack.sql.querydsl.container.SearchHitFieldRef;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.type.Schema;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;
// TODO: add retry/back-off
public class Querier {

    private final Logger log = LogManager.getLogger(getClass());

    private final TimeValue keepAlive, timeout;
    private final int size;
    private final Client client;
    @Nullable
    private final QueryBuilder filter;

    public Querier(Client client, Configuration cfg) {
        this(client, cfg.requestTimeout(), cfg.pageTimeout(), cfg.filter(), cfg.pageSize());
    }

    public Querier(Client client, TimeValue keepAlive, TimeValue timeout, QueryBuilder filter, int size) {
        this.client = client;
        this.keepAlive = keepAlive;
        this.timeout = timeout;
        this.filter = filter;
        this.size = size;
    }

    public void query(Schema schema, QueryContainer query, String index, ActionListener<SchemaRowSet> listener) {
        // prepare the request
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(query, filter, size);
        // set query timeout
        if (timeout.getSeconds() > 0) {
            sourceBuilder.timeout(timeout);
        }

        if (log.isTraceEnabled()) {
            log.trace("About to execute query {} on {}", StringUtils.toString(sourceBuilder), index);
        }

        SearchRequest search = prepareRequest(client, sourceBuilder, timeout, Strings.commaDelimitedListToStringArray(index));

        ActionListener<SearchResponse> l;
        if (query.isAggsOnly()) {
            if (query.aggs().useImplicitGroupBy()) {
                l = new ImplicitGroupActionListener(listener, client, timeout, schema, query, search);
            } else {
                l = new CompositeActionListener(listener, client, timeout, schema, query, search);
            }
        } else {
            search.scroll(keepAlive);
            l = new ScrollActionListener(listener, client, timeout, schema, query);
        }

        client.search(search, l);
    }

    public static SearchRequest prepareRequest(Client client, SearchSourceBuilder source, TimeValue timeout, String... indices) {
        SearchRequest search = client.prepareSearch(indices).setSource(source).setTimeout(timeout).request();
        search.allowPartialSearchResults(false);
        return search;
    }

    /**
     * Dedicated listener for implicit/default group-by queries that return only _one_ result.
     */
    static class ImplicitGroupActionListener extends BaseAggActionListener {

        private static List<? extends Bucket> EMPTY_BUCKET = singletonList(new Bucket() {

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
            public Aggregations getAggregations() {
                throw new SqlIllegalArgumentException("No group-by/aggs defined");
            }
        });

        ImplicitGroupActionListener(ActionListener<SchemaRowSet> listener, Client client, TimeValue keepAlive, Schema schema,
                QueryContainer query, SearchRequest request) {
            super(listener, client, keepAlive, schema, query, request);
        }

        @Override
        protected void handleResponse(SearchResponse response, ActionListener<SchemaRowSet> listener) {
            Aggregations aggs = response.getAggregations();
            if (aggs != null) {
                Aggregation agg = aggs.get(Aggs.ROOT_GROUP_NAME);
                if (agg instanceof Filters) {
                    handleBuckets(((Filters) agg).getBuckets(), response);
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
                Object[] values = new Object[extractors.size()];
                for (int i = 0; i < values.length; i++) {
                    values[i] = extractors.get(i).extract(implicitGroup);
                }
                listener.onResponse(Rows.singleton(schema, values));

            } else if (buckets.isEmpty()) {
                listener.onResponse(Rows.empty(schema));

            } else {
                throw new SqlIllegalArgumentException("Too many groups returned by the implicit group; expected 1, received {}",
                        buckets.size());
            }
        }
    }

        
    /**
     * Dedicated listener for composite aggs/group-by results.
     */
    static class CompositeActionListener extends BaseAggActionListener {

        CompositeActionListener(ActionListener<SchemaRowSet> listener, Client client, TimeValue keepAlive,
                Schema schema, QueryContainer query, SearchRequest request) {
            super(listener, client, keepAlive, schema, query, request);
        }


        @Override
        protected void handleResponse(SearchResponse response, ActionListener<SchemaRowSet> listener) {
            // there are some results
            if (response.getAggregations().asList().size() > 0) {

                // retry
                if (CompositeAggregationCursor.shouldRetryDueToEmptyPage(response)) {
                    CompositeAggregationCursor.updateCompositeAfterKey(response, request.source());
                    client.search(request, this);
                    return;
                }

                CompositeAggregationCursor.updateCompositeAfterKey(response, request.source());
                byte[] nextSearch = null;
                try {
                    nextSearch = CompositeAggregationCursor.serializeQuery(request.source());
                } catch (Exception ex) {
                    listener.onFailure(ex);
                    return;
                }

                listener.onResponse(
                        new SchemaCompositeAggsRowSet(schema, initBucketExtractors(response), response, query.limit(),
                                nextSearch,
                                request.indices()));
            }
            // no results
            else {
                listener.onResponse(Rows.empty(schema));
            }
        }
    }

    abstract static class BaseAggActionListener extends BaseActionListener {
        final QueryContainer query;
        final SearchRequest request;

        BaseAggActionListener(ActionListener<SchemaRowSet> listener, Client client, TimeValue keepAlive, Schema schema,
                QueryContainer query, SearchRequest request) {
            super(listener, client, keepAlive, schema);

            this.query = query;
            this.request = request;
        }

        protected List<BucketExtractor> initBucketExtractors(SearchResponse response) {
            // create response extractors for the first time
            List<FieldExtraction> refs = query.columns();

            List<BucketExtractor> exts = new ArrayList<>(refs.size());
            ConstantExtractor totalCount = new ConstantExtractor(response.getHits().getTotalHits().value);
            for (FieldExtraction ref : refs) {
                exts.add(createExtractor(ref, totalCount));
            }
            return exts;
        }

        private BucketExtractor createExtractor(FieldExtraction ref, BucketExtractor totalCount) {
            if (ref instanceof GroupByRef) {
                GroupByRef r = (GroupByRef) ref;
                return new CompositeKeyExtractor(r.key(), r.property(), r.zoneId());
            }

            if (ref instanceof MetricAggRef) {
                MetricAggRef r = (MetricAggRef) ref;
                return new MetricAggExtractor(r.name(), r.property(), r.innerKey());
            }

            if (ref == GlobalCountRef.INSTANCE) {
                return totalCount;
            }

            if (ref instanceof ComputedRef) {
                Pipe proc = ((ComputedRef) ref).processor();

                // wrap only agg inputs
                proc = proc.transformDown(l -> {
                    BucketExtractor be = createExtractor(l.context(), totalCount);
                    return new AggExtractorInput(l.source(), l.expression(), l.action(), be);
                }, AggPathInput.class);

                return new ComputingExtractor(proc.asProcessor());
            }

            throw new SqlIllegalArgumentException("Unexpected value reference {}", ref.getClass());
        }
    }

    /**
     * Dedicated listener for column retrieval/non-grouped queries (scrolls).
     */
    static class ScrollActionListener extends BaseActionListener {
        private final QueryContainer query;

        ScrollActionListener(ActionListener<SchemaRowSet> listener, Client client, TimeValue keepAlive,
                Schema schema, QueryContainer query) {
            super(listener, client, keepAlive, schema);
            this.query = query;
        }

        @Override
        protected void handleResponse(SearchResponse response, ActionListener<SchemaRowSet> listener) {
            SearchHit[] hits = response.getHits().getHits();

            // create response extractors for the first time
            List<FieldExtraction> refs = query.columns();

            List<HitExtractor> exts = new ArrayList<>(refs.size());
            for (FieldExtraction ref : refs) {
                exts.add(createExtractor(ref));
            }

            // there are some results
            if (hits.length > 0) {
                String scrollId = response.getScrollId();
                SchemaSearchHitRowSet hitRowSet = new SchemaSearchHitRowSet(schema, exts, hits, query.limit(), scrollId);
                
                // if there's an id, try to setup next scroll
                if (scrollId != null &&
                        // is all the content already retrieved?
                        (Boolean.TRUE.equals(response.isTerminatedEarly()) 
                                || response.getHits().getTotalHits().value == hits.length
                                || hitRowSet.isLimitReached())) {
                    // if so, clear the scroll
                    clear(response.getScrollId(), ActionListener.wrap(
                            succeeded -> listener.onResponse(new SchemaSearchHitRowSet(schema, exts, hits, query.limit(), null)),
                            listener::onFailure));
                } else {
                    listener.onResponse(hitRowSet);
                }
            }
            // no hits
            else {
                clear(response.getScrollId(), ActionListener.wrap(succeeded -> listener.onResponse(Rows.empty(schema)),
                        listener::onFailure));
            }
        }

        private HitExtractor createExtractor(FieldExtraction ref) {
            if (ref instanceof SearchHitFieldRef) {
                SearchHitFieldRef f = (SearchHitFieldRef) ref;
                return new FieldHitExtractor(f.name(), f.getDataType(), f.useDocValue(), f.hitName());
            }

            if (ref instanceof ScriptFieldRef) {
                ScriptFieldRef f = (ScriptFieldRef) ref;
                return new FieldHitExtractor(f.name(), null, true);
            }

            if (ref instanceof ComputedRef) {
                Pipe proc = ((ComputedRef) ref).processor();
                // collect hitNames
                Set<String> hitNames = new LinkedHashSet<>();
                proc = proc.transformDown(l -> {
                    HitExtractor he = createExtractor(l.context());
                    hitNames.add(he.hitName());

                    if (hitNames.size() > 1) {
                        throw new SqlIllegalArgumentException("Multi-level nested fields [{}] not supported yet", hitNames);
                    }

                    return new HitExtractorInput(l.source(), l.expression(), he);
                }, ReferenceInput.class);
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
     * Handles both scroll queries (scan/scroll) and regular/composite-aggs queries.
     */
    abstract static class BaseActionListener implements ActionListener<SearchResponse> {

        final ActionListener<SchemaRowSet> listener;

        final Client client;
        final TimeValue keepAlive;
        final Schema schema;

        BaseActionListener(ActionListener<SchemaRowSet> listener, Client client, TimeValue keepAlive, Schema schema) {
            this.listener = listener;

            this.client = client;
            this.keepAlive = keepAlive;
            this.schema = schema;
        }

        // TODO: need to handle rejections plus check failures (shard size, etc...)
        @Override
        public void onResponse(final SearchResponse response) {
            try {
                ShardSearchFailure[] failure = response.getShardFailures();
                if (!CollectionUtils.isEmpty(failure)) {
                    cleanup(response, new SqlIllegalArgumentException(failure[0].reason(), failure[0].getCause()));
                } else {
                    handleResponse(response, ActionListener.wrap(listener::onResponse, e -> cleanup(response, e)));
                }
            } catch (Exception ex) {
                cleanup(response, ex);
            }
        }

        protected abstract void handleResponse(SearchResponse response, ActionListener<SchemaRowSet> listener);

        // clean-up the scroll in case of exception
        protected final void cleanup(SearchResponse response, Exception ex) {
            if (response != null && response.getScrollId() != null) {
                client.prepareClearScroll().addScrollId(response.getScrollId())
                    // in case of failure, report the initial exception instead of the one resulting from cleaning the scroll
                    .execute(ActionListener.wrap(r -> listener.onFailure(ex), e -> {
                        ex.addSuppressed(e);
                        listener.onFailure(ex);
                    }));
            } else {
                listener.onFailure(ex);
            }
        }

        protected final void clear(String scrollId, ActionListener<Boolean> listener) {
            if (scrollId != null) {
                client.prepareClearScroll().addScrollId(scrollId).execute(
                        ActionListener.wrap(
                                clearScrollResponse -> listener.onResponse(clearScrollResponse.isSucceeded()),
                                listener::onFailure));
            } else {
                listener.onResponse(false);
            }
        }

        @Override
        public final void onFailure(Exception ex) {
            listener.onFailure(ex);
        }
    }
}
