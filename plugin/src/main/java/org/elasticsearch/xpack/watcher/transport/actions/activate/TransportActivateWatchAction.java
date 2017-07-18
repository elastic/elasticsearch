/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.activate;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.security.InternalClient;
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
    private final Watch.Parser parser;
    private final Client client;
    private final TriggerService triggerService;

    @Inject
    public TransportActivateWatchAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                        ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Clock clock,
                                        XPackLicenseState licenseState, Watch.Parser parser, ClusterService clusterService,
                                        InternalClient client, TriggerService triggerService) {
        super(settings, ActivateWatchAction.NAME, transportService, threadPool, actionFilters, indexNameExpressionResolver,
                licenseState, clusterService, ActivateWatchRequest::new, ActivateWatchResponse::new);
        this.clock = clock;
        this.parser = parser;
        this.client = client;
        this.triggerService = triggerService;
    }

    @Override
    protected void masterOperation(ActivateWatchRequest request, ClusterState state, ActionListener<ActivateWatchResponse> listener)
            throws Exception {

        try {
            DateTime now = new DateTime(clock.millis(), UTC);
            UpdateRequest updateRequest = new UpdateRequest(Watch.INDEX, Watch.DOC_TYPE, request.getWatchId());
            updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            XContentBuilder builder = activateWatchBuilder(request.isActivate(), now);
            updateRequest.doc(builder);
            // a watch execution updates the status in between, we still want this want to override the active state
            // two has been chosen arbitrary, maybe one would make more sense, as a watch would not execute more often than
            // once per second?
            updateRequest.retryOnConflict(2);

            client.update(updateRequest, ActionListener.wrap(updateResponse -> {
                GetRequest getRequest = new GetRequest(Watch.INDEX, Watch.DOC_TYPE, request.getWatchId())
                        .preference(Preference.LOCAL.type()).realtime(true);
                client.get(getRequest, ActionListener.wrap(getResponse -> {
                    if (getResponse.isExists()) {
                        Watch watch = parser.parseWithSecrets(request.getWatchId(), true, getResponse.getSourceAsBytesRef(), now,
                                XContentType.JSON);
                        watch.version(getResponse.getVersion());
                        watch.status().version(getResponse.getVersion());
                        if (localExecute(request)) {
                            if (watch.status().state().isActive()) {
                                triggerService.add(watch);
                            } else {
                                triggerService.remove(watch.id());
                            }
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
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject()
                    .startObject(Watch.Field.STATUS.getPreferredName())
                    .startObject(WatchStatus.Field.STATE.getPreferredName())
                    .field(WatchStatus.Field.ACTIVE.getPreferredName(), active);

            writeDate(WatchStatus.Field.TIMESTAMP.getPreferredName(), builder, now);
            builder.endObject().endObject().endObject();
            return builder;
        }
    }

}
