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
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.execution.ExecutionException;
import org.elasticsearch.xpack.sql.expression.function.scalar.ColumnProcessor;
import org.elasticsearch.xpack.sql.querydsl.agg.AggPath;
import org.elasticsearch.xpack.sql.querydsl.container.AggRef;
import org.elasticsearch.xpack.sql.querydsl.container.NestedFieldRef;
import org.elasticsearch.xpack.sql.querydsl.container.ProcessingRef;
import org.elasticsearch.xpack.sql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.sql.querydsl.container.Reference;
import org.elasticsearch.xpack.sql.querydsl.container.ScriptFieldRef;
import org.elasticsearch.xpack.sql.querydsl.container.SearchHitFieldRef;
import org.elasticsearch.xpack.sql.querydsl.container.TotalCountRef;
import org.elasticsearch.xpack.sql.session.RowSetCursor;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.type.Schema;
import org.elasticsearch.xpack.sql.util.ObjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

// TODO: add retry/back-off
public class Scroller {

    private final Logger log = Loggers.getLogger(getClass());

    private final TimeValue keepAlive, timeout;
    private final int size;
    private final Client client;

    public Scroller(Client client) {
        // TODO: use better defaults?
        this(client, TimeValue.timeValueSeconds(90), TimeValue.timeValueSeconds(45), 100);
    }

    public Scroller(Client client, TimeValue keepAlive, TimeValue timeout, int size) {
        this.client = client;
        this.keepAlive = keepAlive;
        this.timeout = timeout;
        this.size = size;
    }

    public void scroll(Schema schema, QueryContainer query, String index, String type, ActionListener<RowSetCursor> listener) {
        // prepare the request
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(query);

        log.trace("About to execute query {}", sourceBuilder);

        SearchRequest search = client.prepareSearch(index).setTypes(type).setSource(sourceBuilder).request();
        search.scroll(keepAlive).source().timeout(timeout);

        // set the size only if it hasn't been specified (aggs only queries set the size to 0)
        if (search.source().size() == -1) {
            int sz = query.limit() > 0 ? Math.min(query.limit(), size) : size;
            search.source().size(sz);
        }

        ScrollerActionListener l = query.isAggsOnly() ? new AggsScrollActionListener(listener, client, timeout, schema, query) : new HandshakeScrollActionListener(listener, client, timeout, schema, query);
        client.search(search, l);
    }


    static void from(ActionListener<RowSetCursor> listener, SearchHitsActionListener previous, String scrollId, List<HitExtractor> ext) {
        ScrollerActionListener l = new SessionScrollActionListener(listener, previous.client, previous.keepAlive, previous.schema, ext, previous.limit, previous.docsRead);
        previous.client.searchScroll(new SearchScrollRequest(scrollId).scroll(previous.keepAlive), l);
    }


    // dedicated scroll used for aggs-only/group-by results
    static class AggsScrollActionListener extends ScrollerActionListener {

        private final QueryContainer query;

        AggsScrollActionListener(ActionListener<RowSetCursor> listener, Client client, TimeValue keepAlive, Schema schema, QueryContainer query) {
            super(listener, client, keepAlive, schema);
            this.query = query;
        }

        @Override
        protected RowSetCursor handleResponse(SearchResponse response) {
            Aggregations aggs = response.getAggregations();

            List<Object[]> columns = new ArrayList<>();

            // this method assumes the nested aggregation are all part of the same tree (the SQL group-by)
            int maxDepth = -1;

            for (Reference ref : query.refs()) {
                Object[] arr = null;

                ColumnProcessor processor = null;

                if (ref instanceof ProcessingRef) {
                    ProcessingRef pRef = (ProcessingRef) ref;
                    processor = pRef.processor();
                    ref = pRef.ref();
                }

                if (ref == TotalCountRef.INSTANCE) {
                    arr = new Object[] { processIfNeeded(processor, Long.valueOf(response.getHits().getTotalHits())) };
                    columns.add(arr);
                }
                else if (ref instanceof AggRef) {
                    // workaround for elastic/elasticsearch/issues/23056
                    String path = ((AggRef) ref).path();
                    boolean formattedKey = AggPath.isBucketValueFormatted(path);
                    if (formattedKey) {
                        path = AggPath.bucketValueWithoutFormat(path);
                    }
                    Object value = getAggProperty(aggs, path);

                    //                // FIXME: this can be tabular in nature
                    //                if (ref instanceof MappedAggRef) {
                    //                    Map<String, Object> map = (Map<String, Object>) value;
                    //                    Object extractedValue = map.get(((MappedAggRef) ref).fieldName());
                    //                }

                    if (formattedKey) {
                        List<? extends Bucket> buckets = ((MultiBucketsAggregation) value).getBuckets();
                        arr = new Object[buckets.size()];
                        for (int i = 0; i < buckets.size(); i++) {
                            arr[i] = buckets.get(i).getKeyAsString();
                        }
                    }
                    else {
                        arr = value instanceof Object[] ? (Object[]) value : new Object[] { value };
                    }

                    // process if needed
                    for (int i = 0; i < arr.length; i++) {
                        arr[i] = processIfNeeded(processor, arr[i]);
                    }
                    columns.add(arr);
                }
                // aggs without any grouping
                else {
                    throw new SqlIllegalArgumentException("Unexpected non-agg/grouped column specified; %s", ref.getClass());
                }

                if (ref.depth() > maxDepth) {
                    maxDepth = ref.depth();
                }
            }

            clearScroll(response.getScrollId());
            return new AggsRowSetCursor(schema, columns, maxDepth, query.limit());
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

        private Object processIfNeeded(ColumnProcessor processor, Object value) {
            return processor != null ? processor.apply(value) : value;
        }
    }

    // initial scroll used for parsing search hits (handles possible aggs)
    class HandshakeScrollActionListener extends SearchHitsActionListener {

        private final QueryContainer query;

        HandshakeScrollActionListener(ActionListener<RowSetCursor> listener, Client client, TimeValue keepAlive, Schema schema, QueryContainer query) {
            super(listener, client, keepAlive, schema, query.limit(), 0);
            this.query = query;
        }

        @Override
        public void onResponse(SearchResponse response) {
            super.onResponse(response);
        }

        @Override
        protected List<HitExtractor> getExtractors() {
            // create response extractors for the first time
            List<Reference> refs = query.refs();

            List<HitExtractor> exts = new ArrayList<>(refs.size());

            for (Reference ref : refs) {
                exts.add(createExtractor(ref));
            }
            return exts;
        }

        private HitExtractor createExtractor(Reference ref) {
            if (ref instanceof SearchHitFieldRef) {
                SearchHitFieldRef f = (SearchHitFieldRef) ref;
                return f.useDocValue() ? new DocValueExtractor(f.name()) : new SourceExtractor(f.name());
            }

            if (ref instanceof NestedFieldRef) {
                NestedFieldRef f = (NestedFieldRef) ref;
                return new InnerHitExtractor(f.parent(), f.name(), f.useDocValue());
            }

            if (ref instanceof ScriptFieldRef) {
                ScriptFieldRef f = (ScriptFieldRef) ref;
                return new DocValueExtractor(f.name());
            }

            if (ref instanceof ProcessingRef) {
                ProcessingRef pRef = (ProcessingRef) ref;
                return new ProcessingHitExtractor(createExtractor(pRef.ref()), pRef.processor());
            }

            throw new SqlIllegalArgumentException("Unexpected ValueReference %s", ref.getClass());
        }
    }

    // listener used for streaming the rest of the results after the handshake has been used
    static class SessionScrollActionListener extends SearchHitsActionListener {

        private List<HitExtractor> exts;

        SessionScrollActionListener(ActionListener<RowSetCursor> listener, Client client, TimeValue keepAlive, Schema schema, List<HitExtractor> ext, int limit, int docCount) {
            super(listener, client, keepAlive, schema, limit, docCount);
            this.exts = ext;
        }

        @Override
        protected List<HitExtractor> getExtractors() {
            return exts;
        }
    }

    abstract static class SearchHitsActionListener extends ScrollerActionListener {

        final int limit;
        int docsRead;

        SearchHitsActionListener(ActionListener<RowSetCursor> listener, Client client, TimeValue keepAlive, Schema schema, int limit, int docsRead) {
            super(listener, client, keepAlive, schema);
            this.limit = limit;
            this.docsRead = docsRead;
        }

        protected RowSetCursor handleResponse(SearchResponse response) {
            SearchHit[] hits = response.getHits().getHits();
            List<HitExtractor> exts = getExtractors();

            // there are some results
            if (hits.length > 0) {
                String scrollId = response.getScrollId();
                Consumer<ActionListener<RowSetCursor>> next = null;

                docsRead += hits.length;

                // if there's an id, try to setup next scroll
                if (scrollId != null) {
                    // is all the content already retrieved?
                    if (Boolean.TRUE.equals(response.isTerminatedEarly()) || response.getHits().getTotalHits() == hits.length
                    // or maybe the limit has been reached
                            || docsRead >= limit) {
                        // if so, clear the scroll
                        clearScroll(scrollId);
                        // and remove it to indicate no more data is expected
                        scrollId = null;
                    }
                    else {
                        next = l -> Scroller.from(l, this, response.getScrollId(), exts);
                    }
                }
                int limitHits = limit > 0 && docsRead >= limit ? limit : -1;
                return new SearchHitRowSetCursor(schema, exts, hits, limitHits, scrollId, next);
            }
            // no hits
            else {
                clearScroll(response.getScrollId());
                // typically means last page but might be an aggs only query
                return  needsHit(exts) ? Rows.empty(schema) : new SearchHitRowSetCursor(schema, exts);
            }
        }

        private static boolean needsHit(List<HitExtractor> exts) {
            for (HitExtractor ext : exts) {
                if (ext instanceof DocValueExtractor || ext instanceof ProcessingHitExtractor) {
                    return true;
                }
            }
            return false;
        }

        protected abstract List<HitExtractor> getExtractors();
    }

    abstract static class ScrollerActionListener implements ActionListener<SearchResponse> {

        final ActionListener<RowSetCursor> listener;

        final Client client;
        final TimeValue keepAlive;
        final Schema schema;

        ScrollerActionListener(ActionListener<RowSetCursor> listener, Client client, TimeValue keepAlive, Schema schema) {
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
                if (!ObjectUtils.isEmpty(failure)) {
                    onFailure(new ExecutionException(failure[0].reason(), failure[0].getCause()));
                }
                listener.onResponse(handleResponse(response));
            } catch (Exception ex) {
                onFailure(ex);
            }
        }

        protected abstract RowSetCursor handleResponse(SearchResponse response);

        protected final void clearScroll(String scrollId) {
            if (scrollId != null) {
                // fire and forget
                client.prepareClearScroll().addScrollId(scrollId).execute();
            }
        }

        @Override
        public final void onFailure(Exception ex) {
            listener.onFailure(ex);
        }
    }
}
