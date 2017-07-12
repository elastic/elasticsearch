/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.ack;

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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.watcher.transport.actions.WatcherTransportAction;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.joda.time.DateTime;

import java.time.Clock;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.joda.time.DateTimeZone.UTC;

public class TransportAckWatchAction extends WatcherTransportAction<AckWatchRequest, AckWatchResponse> {

    private final Clock clock;
    private final Watch.Parser parser;
    private final Client client;

    @Inject
    public TransportAckWatchAction(Settings settings, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver, Clock clock, XPackLicenseState licenseState,
                                   Watch.Parser parser, InternalClient client, ClusterService clusterService) {
        super(settings, AckWatchAction.NAME, transportService, threadPool, actionFilters, indexNameExpressionResolver,
                licenseState, clusterService, AckWatchRequest::new, AckWatchResponse::new);
        this.clock = clock;
        this.parser = parser;
        this.client = client;
    }

    @Override
    protected void masterOperation(AckWatchRequest request, ClusterState state,
                                   ActionListener<AckWatchResponse> listener) throws Exception {
        GetRequest getRequest = new GetRequest(Watch.INDEX, Watch.DOC_TYPE, request.getWatchId())
                .preference(Preference.LOCAL.type()).realtime(true);

        client.get(getRequest, ActionListener.wrap((response) -> {
            if (response.isExists() == false) {
                listener.onFailure(new ResourceNotFoundException("Watch with id [{}] does not exist", request.getWatchId()));
            } else {
                DateTime now = new DateTime(clock.millis(), UTC);
                Watch watch = parser.parseWithSecrets(request.getWatchId(), true, response.getSourceAsBytesRef(), now, XContentType.JSON);
                watch.version(response.getVersion());
                watch.status().version(response.getVersion());
                String[] actionIds = request.getActionIds();
                if (actionIds == null || actionIds.length == 0) {
                    actionIds = new String[]{Watch.ALL_ACTIONS_ID};
                }

                // exit early in case nothing changes
                boolean isChanged = watch.ack(now, actionIds);
                if (isChanged == false) {
                    listener.onResponse(new AckWatchResponse(watch.status()));
                    return;
                }

                UpdateRequest updateRequest = new UpdateRequest(Watch.INDEX, Watch.DOC_TYPE, request.getWatchId());
                // this may reject this action, but prevents concurrent updates from a watch execution
                updateRequest.version(response.getVersion());
                updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                XContentBuilder builder = jsonBuilder();
                builder.startObject()
                        .startObject(Watch.Field.STATUS.getPreferredName())
                        .startObject("actions");

                List<String> actionIdsAsList = Arrays.asList(actionIds);
                boolean updateAll = actionIdsAsList.contains("_all");
                for (ActionWrapper actionWrapper : watch.actions()) {
                    if (updateAll || actionIdsAsList.contains(actionWrapper.id())) {
                        builder.startObject(actionWrapper.id())
                                .field("ack", watch.status().actionStatus(actionWrapper.id()).ackStatus(), ToXContent.EMPTY_PARAMS)
                                .endObject();
                    }
                }

                builder.endObject().endObject().endObject();
                updateRequest.doc(builder);

                client.update(updateRequest, ActionListener.wrap(
                        (updateResponse) -> listener.onResponse(new AckWatchResponse(watch.status())),
                        listener::onFailure));
            }
        }, listener::onFailure));
    }
}
