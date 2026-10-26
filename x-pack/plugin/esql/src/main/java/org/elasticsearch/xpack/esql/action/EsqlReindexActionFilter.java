/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Flow;
import java.util.function.Supplier;

/**
 * Runs {@code _reindex} requests whose source is an ES|QL query. The query is streamed page by page
 * and every page is written to the destination with one bulk request before the next page is requested.
 */
public final class EsqlReindexActionFilter implements MappedActionFilter {

    private static final int BATCH_SIZE = 1000;

    private final Supplier<Client> client;

    public EsqlReindexActionFilter(Supplier<Client> client) {
        this.client = client;
    }

    @Override
    public String actionName() {
        return ReindexAction.NAME;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (request instanceof ReindexRequest reindexRequest && reindexRequest.getEsqlQuery() != null) {
            new EsqlReindex(client.get(), reindexRequest, (ActionListener<BulkByPaginatedSearchResponse>) listener).start();
            return;
        }
        chain.proceed(task, action, request, listener);
    }

    private static final class EsqlReindex implements Flow.Subscriber<Page> {
        private final Client client;
        private final ReindexRequest request;
        private final ActionListener<BulkByPaginatedSearchResponse> listener;
        private final long startNanos = System.nanoTime();
        private final List<BulkItemResponse.Failure> failures = new ArrayList<>();
        private final BytesRef scratch = new BytesRef();
        private List<ColumnInfoImpl> columns;
        private boolean[] skipColumns;
        private ZoneId zoneId;
        private Flow.Subscription subscription;
        private long created;
        private long updated;
        private long versionConflicts;
        private int batches;

        EsqlReindex(Client client, ReindexRequest request, ActionListener<BulkByPaginatedSearchResponse> listener) {
            this.client = client;
            this.request = request;
            // Restore the caller's context so headers added while the query runs (such as ES|QL warnings) are not returned.
            this.listener = ActionListener.notifyOnce(
                new ContextPreservingActionListener<>(client.threadPool().getThreadContext().newRestorableContext(false), listener)
            );
        }

        void start() {
            EsqlStreamQueryRequest streamRequest = new EsqlStreamQueryRequest(
                EsqlQueryRequest.syncEsqlQueryRequest(request.getEsqlQuery()),
                ActionListener.wrap(this::onResultStream, listener::onFailure),
                false,
                BATCH_SIZE
            );
            streamRequest.unboundedResults(true);
            client.execute(EsqlStreamQueryAction.INSTANCE, streamRequest, ActionListener.wrap(r -> {}, listener::onFailure));
        }

        private void onResultStream(EsqlStreamQueryAction.ResultStream resultStream) {
            columns = resultStream.columns();
            skipColumns = multiFieldColumns(columns);
            zoneId = resultStream.zoneId();
            resultStream.publisher().subscribe(this);
        }

        /**
         * A field can't be both a leaf and an object, so a column {@code a.b} next to a column {@code a} is a multi-field
         * of {@code a}. Writing it would conflict with {@code a}; the destination mapping derives it from {@code a} instead.
         */
        private static boolean[] multiFieldColumns(List<ColumnInfoImpl> columns) {
            Set<String> names = new HashSet<>();
            for (ColumnInfoImpl column : columns) {
                names.add(column.name());
            }
            boolean[] skip = new boolean[columns.size()];
            for (int c = 0; c < skip.length; c++) {
                String name = columns.get(c).name();
                for (int dot = name.indexOf('.'); dot > 0 && skip[c] == false; dot = name.indexOf('.', dot + 1)) {
                    skip[c] = names.contains(name.substring(0, dot));
                }
            }
            return skip;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(Page page) {
            BulkRequest bulkRequest = new BulkRequest();
            try {
                PositionToXContent[] converters = new PositionToXContent[columns.size()];
                for (int c = 0; c < converters.length; c++) {
                    converters[c] = PositionToXContent.positionToXContent(columns.get(c), page.getBlock(c), zoneId, scratch);
                }
                for (int position = 0; position < page.getPositionCount(); position++) {
                    bulkRequest.add(indexRequest(page, converters, position));
                }
            } catch (IOException e) {
                subscription.cancel();
                listener.onFailure(e);
                return;
            } finally {
                page.releaseBlocks();
            }
            batches++;
            client.bulk(bulkRequest, ActionListener.wrap(this::onBulkResponse, e -> {
                subscription.cancel();
                listener.onFailure(e);
            }));
        }

        private IndexRequest indexRequest(Page page, PositionToXContent[] converters, int position) throws IOException {
            IndexRequest destination = request.getDestination();
            XContentBuilder source = XContentFactory.jsonBuilder().startObject();
            for (int c = 0; c < converters.length; c++) {
                Block block = page.getBlock(c);
                if (skipColumns[c] || block.isNull(position)) {
                    continue;
                }
                source.field(columns.get(c).name());
                converters[c].positionToXContent(source, ToXContent.EMPTY_PARAMS, position);
            }
            source.endObject();
            return new IndexRequest(destination.index()).opType(destination.opType())
                .setPipeline(destination.getPipeline())
                .setRequireAlias(destination.isRequireAlias())
                .source(source);
        }

        private void onBulkResponse(BulkResponse response) {
            for (BulkItemResponse item : response.getItems()) {
                if (item.isFailed() == false) {
                    if (item.getResponse().getResult() == DocWriteResponse.Result.CREATED) {
                        created++;
                    } else {
                        updated++;
                    }
                } else if (item.getFailure().getStatus() == RestStatus.CONFLICT) {
                    versionConflicts++;
                    if (request.isAbortOnVersionConflict()) {
                        failures.add(item.getFailure());
                    }
                } else {
                    failures.add(item.getFailure());
                }
            }
            if (failures.isEmpty() == false) {
                subscription.cancel();
                finish();
                return;
            }
            subscription.request(1);
        }

        @Override
        public void onError(Throwable throwable) {
            listener.onFailure(throwable instanceof Exception e ? e : new ElasticsearchException(throwable));
        }

        @Override
        public void onComplete() {
            finish();
        }

        private void finish() {
            BulkByPaginatedSearchTask.Status status = new BulkByPaginatedSearchTask.Status(
                null,
                created + updated + versionConflicts + failures.size(),
                updated,
                created,
                0,
                batches,
                versionConflicts,
                0,
                0,
                0,
                TimeValue.ZERO,
                request.getRequestsPerSecond(),
                null,
                TimeValue.ZERO
            );
            TimeValue took = TimeValue.timeValueNanos(System.nanoTime() - startNanos);
            listener.onResponse(new BulkByPaginatedSearchResponse(took, status, failures, List.of(), false));
        }
    }
}
