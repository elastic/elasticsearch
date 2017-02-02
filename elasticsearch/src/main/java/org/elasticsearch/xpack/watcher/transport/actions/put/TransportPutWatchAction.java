/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.put;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.watcher.transport.actions.WatcherTransportAction;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.joda.time.DateTime;

import java.time.Clock;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.joda.time.DateTimeZone.UTC;

public class TransportPutWatchAction extends WatcherTransportAction<PutWatchRequest, PutWatchResponse> {

    private final Clock clock;
    private final TriggerService triggerService;
    private final Watch.Parser parser;
    private final InternalClient client;

    @Inject
    public TransportPutWatchAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver, Clock clock, XPackLicenseState licenseState,
                                   TriggerService triggerService, Watch.Parser parser, InternalClient client) {
        super(settings, PutWatchAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, licenseState, PutWatchRequest::new);
        this.clock = clock;
        this.triggerService = triggerService;
        this.parser = parser;
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected PutWatchResponse newResponse() {
        return new PutWatchResponse();
    }

    @Override
    protected void masterOperation(PutWatchRequest request, ClusterState state, ActionListener<PutWatchResponse> listener) throws
            ElasticsearchException {

        try {
            DateTime now = new DateTime(clock.millis(), UTC);
            Watch watch = parser.parseWithSecrets(request.getId(), false, request.getSource(), now, request.xContentType());
            watch.setState(request.isActive(), now);

            try (XContentBuilder builder = jsonBuilder()) {
                Payload.XContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(Watch.INCLUDE_STATUS_KEY, "true"));
                watch.toXContent(builder, params);
                BytesReference bytesReference = builder.bytes();

                IndexRequest indexRequest = new IndexRequest(Watch.INDEX).type(Watch.DOC_TYPE).id(request.getId());
                indexRequest.source(bytesReference, XContentType.JSON);

                client.index(indexRequest, ActionListener.wrap(indexResponse -> {
                    boolean created = indexResponse.getResult() == DocWriteResponse.Result.CREATED;
                    if (request.isActive()) {
                        triggerService.add(watch);
                    } else {
                        triggerService.remove(request.getId());
                    }
                    listener.onResponse(new PutWatchResponse(indexResponse.getId(), indexResponse.getVersion(), created));
                }, listener::onFailure));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PutWatchRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, Watch.INDEX);
    }

}
