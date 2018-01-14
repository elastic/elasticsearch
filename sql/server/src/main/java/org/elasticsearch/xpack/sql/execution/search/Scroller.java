/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.execution.ExecutionException;
import org.elasticsearch.xpack.sql.execution.search.extractor.ComputingHitExtractor;
import org.elasticsearch.xpack.sql.execution.search.extractor.FieldHitExtractor;
import org.elasticsearch.xpack.sql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.AggPathInput;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.AggValueInput;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.HitExtractorInput;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ReferenceInput;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;
import org.elasticsearch.xpack.sql.querydsl.agg.AggPath;
import org.elasticsearch.xpack.sql.querydsl.container.AggRef;
import org.elasticsearch.xpack.sql.querydsl.container.ComputedRef;
import org.elasticsearch.xpack.sql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.sql.querydsl.container.ScriptFieldRef;
import org.elasticsearch.xpack.sql.querydsl.container.SearchHitFieldRef;
import org.elasticsearch.xpack.sql.querydsl.container.TotalCountRef;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.type.Schema;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
// TODO: add retry/back-off
public class Scroller {

    private final Logger log = Loggers.getLogger(getClass());

    private final TimeValue keepAlive, timeout;
    private final int size;
    private final Client client;
    @Nullable
    private final QueryBuilder filter;

    public Scroller(Client client, Configuration cfg) {
        this(client, cfg.requestTimeout(), cfg.pageTimeout(), cfg.filter(), cfg.pageSize());
    }

    public Scroller(Client client, TimeValue keepAlive, TimeValue timeout, QueryBuilder filter, int size) {
        this.client = client;
        this.keepAlive = keepAlive;
        this.timeout = timeout;
        this.filter = filter;
        this.size = size;
    }

    public void scroll(Schema schema, QueryContainer query, String index, ActionListener<SchemaRowSet> listener) {
        // prepare the request
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(query, filter, size);

        if (log.isTraceEnabled()) {
            log.trace("About to execute query {} on {}", StringUtils.toString(sourceBuilder), index);
        }

        SearchRequest search = client.prepareSearch(index).setSource(sourceBuilder).request();

        ScrollerActionListener l;
        if (query.isAggsOnly()) {
            l = new AggsScrollActionListener(listener, client, timeout, schema, query);
        } else {
            search.scroll(keepAlive).source().timeout(timeout);
            l = new HandshakeScrollActionListener(listener, client, timeout, schema, query);
        }
        client.search(search, l);
    }

    // dedicated scroll used for aggs-only/group-by results
    static class AggsScrollActionListener extends ScrollerActionListener {

        private final QueryContainer query;

        AggsScrollActionListener(ActionListener<SchemaRowSet> listener, Client client, TimeValue keepAlive,
                Schema schema, QueryContainer query) {
            super(listener, client, keepAlive, schema);
            this.query = query;
        }

        @Override
        protected void handleResponse(SearchResponse response, ActionListener<SchemaRowSet> listener) {

            final List<Object[]> extractedAggs = new ArrayList<>();
            AggValues aggValues = new AggValues(extractedAggs);
            List<Supplier<Object>> aggColumns = new ArrayList<>(query.columns().size());

            // this method assumes the nested aggregation are all part of the same tree (the SQL group-by)
            int maxDepth = -1;

            List<FieldExtraction> cols = query.columns();
            for (int index = 0; index < cols.size(); index++) {
                FieldExtraction col = cols.get(index);
                Supplier<Object> supplier = null;

                if (col instanceof ComputedRef) {
                    ComputedRef pRef = (ComputedRef) col;

                    Processor processor = pRef.processor().transformUp(a -> {
                        Object[] value = extractAggValue(new AggRef(a.context()), response);
                        extractedAggs.add(value);
                        final int aggPosition = extractedAggs.size() - 1;
                        Supplier<Object> action = null;
                        if (a.action() != null) {
                            action = () -> a.action().process(aggValues.column(aggPosition));
                        }
                        else {
                            action = () -> aggValues.column(aggPosition);
                        }
                        return new AggValueInput(a.expression(), action, a.innerKey());
                    }, AggPathInput.class).asProcessor();
                    // the input is provided through the value input above
                    supplier = () -> processor.process(null);
                }
                else {
                    extractedAggs.add(extractAggValue(col, response));
                    final int aggPosition = extractedAggs.size() - 1;
                    supplier = () -> aggValues.column(aggPosition);
                }

                aggColumns.add(supplier);
                if (col.depth() > maxDepth) {
                    maxDepth = col.depth();
                }
            }

            aggValues.init(maxDepth, query.limit());
            clearScroll(response.getScrollId(), ActionListener.wrap(
                    succeeded -> listener.onResponse(new AggsRowSet(schema, aggValues, aggColumns)),
                    listener::onFailure));
        }

        private Object[] extractAggValue(FieldExtraction col, SearchResponse response) {
            if (col == TotalCountRef.INSTANCE) {
                return new Object[] { Long.valueOf(response.getHits().getTotalHits()) };
            }
            else if (col instanceof AggRef) {
                Object[] arr;

                String path = ((AggRef) col).path();
                // yup, this is instance equality to make sure we only check the path used by the code
                if (path == TotalCountRef.PATH) {
                    arr = new Object[] { Long.valueOf(response.getHits().getTotalHits()) };
                }
                else {
                    // workaround for elastic/elasticsearch/issues/23056
                    boolean formattedKey = AggPath.isBucketValueFormatted(path);
                    if (formattedKey) {
                        path = AggPath.bucketValueWithoutFormat(path);
                    }
                    Object value = getAggProperty(response.getAggregations(), path);

                    //                // FIXME: this can be tabular in nature
                    //                if (ref instanceof MappedAggRef) {
                    //                    Map<String, Object> map = (Map<String, Object>) value;
                    // Object extractedValue = map.get(((MappedAggRef)
                    // ref).fieldName());
                    //                }

                    if (formattedKey) {
                        List<? extends Bucket> buckets = ((MultiBucketsAggregation) value).getBuckets();
                        arr = new Object[buckets.size()];
                        for (int i = 0; i < buckets.size(); i++) {
                            arr[i] = buckets.get(i).getKeyAsString();
                        }
                    } else {
                        arr = value instanceof Object[] ? (Object[]) value : new Object[] { value };
                    }
                }

                return arr;
            }
            throw new SqlIllegalArgumentException("Unexpected non-agg/grouped column specified; %s", col.getClass());
        }

        private static Object getAggProperty(Aggregations aggs, String path) {
            List<String> list = AggregationPath.parse(path).getPathElementsAsStringList();
            String aggName = list.get(0);
            InternalAggregation agg = aggs.get(aggName);
            if (agg == null) {
                throw new ExecutionException("Cannot find an aggregation named %s", aggName);
            }
            return agg.getProperty(list.subList(1, list.size()));
        }
    }

    // initial scroll used for parsing search hits (handles possible aggs)
    static class HandshakeScrollActionListener extends ScrollerActionListener {
        private final QueryContainer query;

        HandshakeScrollActionListener(ActionListener<SchemaRowSet> listener, Client client, TimeValue keepAlive,
                Schema schema, QueryContainer query) {
            super(listener, client, keepAlive, schema);
            this.query = query;
        }


        @Override
        protected void handleResponse(SearchResponse response, ActionListener<SchemaRowSet> listener) {
            SearchHit[] hits = response.getHits().getHits();
            List<HitExtractor> exts = getExtractors();

            // there are some results
            if (hits.length > 0) {
                String scrollId = response.getScrollId();

                // if there's an id, try to setup next scroll
                if (scrollId != null &&
                        // is all the content already retrieved?
                        (Boolean.TRUE.equals(response.isTerminatedEarly()) || response.getHits().getTotalHits() == hits.length
                        // or maybe the limit has been reached
                        || (hits.length >= query.limit() && query.limit() > -1))) {
                    // if so, clear the scroll
                    clearScroll(response.getScrollId(), ActionListener.wrap(
                            succeeded -> listener.onResponse(new InitialSearchHitRowSet(schema, exts, hits, query.limit(), null)),
                            listener::onFailure));
                } else {
                    listener.onResponse(new InitialSearchHitRowSet(schema, exts, hits, query.limit(), scrollId));
                }
            }
            // no hits
            else {
                clearScroll(response.getScrollId(), ActionListener.wrap(succeeded -> listener.onResponse(Rows.empty(schema)),
                        listener::onFailure));
            }
        }

        private List<HitExtractor> getExtractors() {
            // create response extractors for the first time
            List<FieldExtraction> refs = query.columns();

            List<HitExtractor> exts = new ArrayList<>(refs.size());

            for (FieldExtraction ref : refs) {
                exts.add(createExtractor(ref));
            }
            return exts;
        }

        private HitExtractor createExtractor(FieldExtraction ref) {
            if (ref instanceof SearchHitFieldRef) {
                SearchHitFieldRef f = (SearchHitFieldRef) ref;
                return new FieldHitExtractor(f.name(), f.useDocValue(), f.hitName());
            }

            if (ref instanceof ScriptFieldRef) {
                ScriptFieldRef f = (ScriptFieldRef) ref;
                return new FieldHitExtractor(f.name(), true);
            }

            if (ref instanceof ComputedRef) {
                ProcessorDefinition proc = ((ComputedRef) ref).processor();
                proc = proc.transformDown(l -> new HitExtractorInput(l.expression(),
                        createExtractor(l.context())), ReferenceInput.class);
                return new ComputingHitExtractor(proc.asProcessor());
            }

            throw new SqlIllegalArgumentException("Unexpected ValueReference %s", ref.getClass());
        }
    }

    abstract static class ScrollerActionListener implements ActionListener<SearchResponse> {

        final ActionListener<SchemaRowSet> listener;

        final Client client;
        final TimeValue keepAlive;
        final Schema schema;

        ScrollerActionListener(ActionListener<SchemaRowSet> listener, Client client, TimeValue keepAlive, Schema schema) {
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
                    cleanupScroll(response, new ExecutionException(failure[0].reason(), failure[0].getCause()));
                } else {
                    handleResponse(response, ActionListener.wrap(listener::onResponse, e -> cleanupScroll(response, e)));
                }
            } catch (Exception ex) {
                cleanupScroll(response, ex);
            }
        }

        protected abstract void handleResponse(SearchResponse response, ActionListener<SchemaRowSet> listener);

        // clean-up the scroll in case of exception
        protected final void cleanupScroll(SearchResponse response, Exception ex) {
            if (response != null && response.getScrollId() != null) {
                client.prepareClearScroll().addScrollId(response.getScrollId())
                    // in case of failure, report the initial exception instead of the one resulting from cleaning the scroll
                    .execute(ActionListener.wrap(r -> listener.onFailure(ex), e -> {
                        ex.addSuppressed(e);
                        listener.onFailure(ex);
                    }));
            }
        }

        protected final void clearScroll(String scrollId, ActionListener<Boolean> listener) {
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
