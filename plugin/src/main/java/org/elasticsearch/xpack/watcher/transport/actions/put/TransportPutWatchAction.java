/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.transport.actions.WatcherTransportAction;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.joda.time.DateTime;

import java.time.Clock;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.joda.time.DateTimeZone.UTC;

public class TransportPutWatchAction extends WatcherTransportAction<PutWatchRequest, PutWatchResponse> {

    private final Clock clock;
    private final Watch.Parser parser;
    private final InternalClient client;
    private final TriggerService triggerService;

    @Inject
    public TransportPutWatchAction(Settings settings, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver, Clock clock, XPackLicenseState licenseState,
                                   Watch.Parser parser, InternalClient client, ClusterService clusterService,
                                   TriggerService triggerService) {
        super(settings, PutWatchAction.NAME, transportService, threadPool, actionFilters, indexNameExpressionResolver,
                licenseState, clusterService, PutWatchRequest::new, PutWatchResponse::new);
        this.clock = clock;
        this.parser = parser;
        this.client = client;
        this.triggerService = triggerService;
    }

    @Override
    protected void masterOperation(PutWatchRequest request, ClusterState state,
                                   ActionListener<PutWatchResponse> listener) throws Exception {
        try {
            DateTime now = new DateTime(clock.millis(), UTC);
            Watch watch = parser.parseWithSecrets(request.getId(), false, request.getSource(), now, request.xContentType());
            watch.setState(request.isActive(), now);

            try (XContentBuilder builder = jsonBuilder()) {
                Payload.XContent.Params params = WatcherParams.builder().hideSecrets(false).put(Watch.INCLUDE_STATUS_KEY, "true").build();
                watch.toXContent(builder, params);
                final BytesReference bytesReference = builder.bytes();

                IndexRequest indexRequest = new IndexRequest(Watch.INDEX).type(Watch.DOC_TYPE).id(request.getId());
                indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                indexRequest.source(bytesReference, XContentType.JSON);

                client.index(indexRequest, ActionListener.wrap(indexResponse -> {
                    boolean created = indexResponse.getResult() == DocWriteResponse.Result.CREATED;
                    if (localExecute(request) == false && watch.status().state().isActive()) {
                        triggerService.add(watch);
                    }
                    listener.onResponse(new PutWatchResponse(indexResponse.getId(), indexResponse.getVersion(), created));
                }, listener::onFailure));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
