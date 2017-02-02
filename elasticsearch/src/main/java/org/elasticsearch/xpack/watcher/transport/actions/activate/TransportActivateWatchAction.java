/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.activate;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.transport.actions.WatcherTransportAction;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStatus;
import org.joda.time.DateTime;

import java.io.IOException;
import java.time.Clock;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.support.WatcherDateTimeUtils.writeDate;
import static org.joda.time.DateTimeZone.UTC;

/**
 * Performs the watch de/activation operation.
 */
public class TransportActivateWatchAction extends WatcherTransportAction<ActivateWatchRequest, ActivateWatchResponse> {

    private final Clock clock;
    private final TriggerService triggerService;
    private final Watch.Parser parser;
    private final WatcherClientProxy client;

    @Inject
    public TransportActivateWatchAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                        ThreadPool threadPool, ActionFilters actionFilters,
                                        IndexNameExpressionResolver indexNameExpressionResolver, Clock clock,
                                        XPackLicenseState licenseState, TriggerService triggerService, Watch.Parser parser,
                                        WatcherClientProxy client) {
        super(settings, ActivateWatchAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                licenseState, ActivateWatchRequest::new);
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
    protected ActivateWatchResponse newResponse() {
        return new ActivateWatchResponse();
    }

    @Override
    protected void masterOperation(ActivateWatchRequest request, ClusterState state, ActionListener<ActivateWatchResponse> listener)
            throws ElasticsearchException {
        try {
            // if this is about deactivation, remove this immediately from the trigger service, no need to wait for all those async calls
            if (request.isActivate() == false) {
                triggerService.remove(request.getWatchId());
            }

            DateTime now = new DateTime(clock.millis(), UTC);
            UpdateRequest updateRequest = new UpdateRequest(Watch.INDEX, Watch.DOC_TYPE, request.getWatchId());
            XContentBuilder builder = activateWatchBuilder(request.isActivate(), now);
            updateRequest.doc(builder);

            client.update(updateRequest, ActionListener.wrap(updateResponse -> {
                client.getWatch(request.getWatchId(), ActionListener.wrap(getResponse -> {
                    if (getResponse.isExists()) {
                        Watch watch = parser.parseWithSecrets(request.getWatchId(), true, getResponse.getSourceAsBytesRef(), now,
                                XContentType.JSON);
                        watch.version(getResponse.getVersion());
                        watch.status().version(getResponse.getVersion());

                        if (request.isActivate()) {
                            triggerService.add(watch);
                        }
                        listener.onResponse(new ActivateWatchResponse(watch.status()));
                    } else {
                        listener.onFailure(new ResourceNotFoundException("Watch with id [{}] does not exist", request.getWatchId()));
                    }
                }, listener::onFailure));
            }, listener::onFailure));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    private XContentBuilder activateWatchBuilder(boolean active, DateTime now) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject()
                .startObject(Watch.Field.STATUS.getPreferredName())
                .startObject(WatchStatus.Field.STATE.getPreferredName())
                .field(WatchStatus.Field.ACTIVE.getPreferredName(), active);

        writeDate(WatchStatus.Field.TIMESTAMP.getPreferredName(), builder, now);
        builder.endObject().endObject().endObject();
        return builder;
    }

    @Override
    protected ClusterBlockException checkBlock(ActivateWatchRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, Watch.INDEX);
    }
}
