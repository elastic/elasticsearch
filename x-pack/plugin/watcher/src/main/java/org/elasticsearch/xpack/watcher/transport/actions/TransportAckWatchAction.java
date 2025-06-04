/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transport.actions;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsRequest;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;
import org.elasticsearch.xpack.watcher.ClockHolder;
import org.elasticsearch.xpack.watcher.watch.WatchParser;

import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportAckWatchAction extends WatcherTransportAction<AckWatchRequest, AckWatchResponse> {

    private final Clock clock;
    private final WatchParser parser;
    private final Client client;

    @Inject
    public TransportAckWatchAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClockHolder clockHolder,
        XPackLicenseState licenseState,
        WatchParser parser,
        Client client
    ) {
        super(AckWatchAction.NAME, transportService, actionFilters, licenseState, AckWatchRequest::new);
        this.clock = clockHolder.clock;
        this.parser = parser;
        this.client = client;
    }

    @Override
    protected void doExecute(AckWatchRequest request, ActionListener<AckWatchResponse> listener) {
        WatcherStatsRequest watcherStatsRequest = new WatcherStatsRequest();
        watcherStatsRequest.includeCurrentWatches(true);

        executeAsyncWithOrigin(client, WATCHER_ORIGIN, WatcherStatsAction.INSTANCE, watcherStatsRequest, ActionListener.wrap(response -> {
            boolean isWatchRunning = response.getNodes()
                .stream()
                .anyMatch(node -> node.getSnapshots().stream().anyMatch(snapshot -> snapshot.watchId().equals(request.getWatchId())));
            if (isWatchRunning) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "watch[{}] is running currently, cannot ack until finished",
                        RestStatus.CONFLICT,
                        request.getWatchId()
                    )
                );
            } else {
                GetRequest getRequest = new GetRequest(Watch.INDEX, request.getWatchId()).preference(Preference.LOCAL.type())
                    .realtime(true);

                executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    WATCHER_ORIGIN,
                    getRequest,
                    ActionListener.<GetResponse>wrap(getResponse -> {
                        if (getResponse.isExists() == false) {
                            listener.onFailure(new ResourceNotFoundException("Watch with id [{}] does not exist", request.getWatchId()));
                        } else {
                            ZonedDateTime now = clock.instant().atZone(ZoneOffset.UTC);
                            Watch watch = parser.parseWithSecrets(
                                request.getWatchId(),
                                true,
                                getResponse.getSourceAsBytesRef(),
                                now,
                                XContentType.JSON,
                                getResponse.getSeqNo(),
                                getResponse.getPrimaryTerm()
                            );
                            watch.status().version(getResponse.getVersion());
                            String[] actionIds = request.getActionIds();
                            if (actionIds == null || actionIds.length == 0) {
                                actionIds = new String[] { WatchField.ALL_ACTIONS_ID };
                            }

                            // exit early in case nothing changes
                            boolean isChanged = watch.ack(now, actionIds);
                            if (isChanged == false) {
                                listener.onResponse(new AckWatchResponse(watch.status()));
                                return;
                            }

                            UpdateRequest updateRequest = new UpdateRequest(Watch.INDEX, request.getWatchId());
                            // this may reject this action, but prevents concurrent updates from a watch execution
                            updateRequest.setIfSeqNo(getResponse.getSeqNo());
                            updateRequest.setIfPrimaryTerm(getResponse.getPrimaryTerm());
                            updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                            XContentBuilder builder = jsonBuilder();
                            builder.startObject().startObject(WatchField.STATUS.getPreferredName()).startObject("actions");

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

                            executeAsyncWithOrigin(
                                client.threadPool().getThreadContext(),
                                WATCHER_ORIGIN,
                                updateRequest,
                                ActionListener.<UpdateResponse>wrap(
                                    (updateResponse) -> listener.onResponse(new AckWatchResponse(watch.status())),
                                    listener::onFailure
                                ),
                                client::update
                            );
                        }
                    }, listener::onFailure),
                    client::get
                );

            }

        }, listener::onFailure));
    }
}
