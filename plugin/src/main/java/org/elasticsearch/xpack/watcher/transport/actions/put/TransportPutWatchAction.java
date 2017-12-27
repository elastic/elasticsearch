/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.transport.actions.WatcherTransportAction;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchParser;
import org.joda.time.DateTime;

import java.time.Clock;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.ClientHelper.WATCHER_ORIGIN;
import static org.elasticsearch.xpack.ClientHelper.executeAsyncWithOrigin;
import static org.joda.time.DateTimeZone.UTC;

public class TransportPutWatchAction extends WatcherTransportAction<PutWatchRequest, PutWatchResponse> {

    private final Clock clock;
    private final WatchParser parser;
    private final Client client;

    @Inject
    public TransportPutWatchAction(Settings settings, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver, Clock clock, XPackLicenseState licenseState,
                                   WatchParser parser, Client client) {
        super(settings, PutWatchAction.NAME, transportService, threadPool, actionFilters, indexNameExpressionResolver,
                licenseState, PutWatchRequest::new);
        this.clock = clock;
        this.parser = parser;
        this.client = client;
    }

    @Override
    protected void doExecute(PutWatchRequest request, ActionListener<PutWatchResponse> listener) {
        try {
            DateTime now = new DateTime(clock.millis(), UTC);
            Watch watch = parser.parseWithSecrets(request.getId(), false, request.getSource(), now, request.xContentType());
            watch.setState(request.isActive(), now);

            // ensure we only filter for the allowed headers
            Map<String, String> filteredHeaders = threadPool.getThreadContext().getHeaders().entrySet().stream()
                    .filter(e -> Watcher.HEADER_FILTERS.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            watch.status().setHeaders(filteredHeaders);

            try (XContentBuilder builder = jsonBuilder()) {
                Payload.XContent.Params params = WatcherParams.builder()
                        .hideSecrets(false)
                        .hideHeaders(false)
                        .put(Watch.INCLUDE_STATUS_KEY, "true")
                        .build();
                watch.toXContent(builder, params);
                final BytesReference bytesReference = builder.bytes();

                IndexRequest indexRequest = new IndexRequest(Watch.INDEX).type(Watch.DOC_TYPE).id(request.getId());
                indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                indexRequest.source(bytesReference, XContentType.JSON);

                executeAsyncWithOrigin(client.threadPool().getThreadContext(), WATCHER_ORIGIN, indexRequest,
                        ActionListener.<IndexResponse>wrap(indexResponse -> {
                            boolean created = indexResponse.getResult() == DocWriteResponse.Result.CREATED;
                            listener.onResponse(new PutWatchResponse(indexResponse.getId(), indexResponse.getVersion(), created));
                        }, listener::onFailure),
                        client::index);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
