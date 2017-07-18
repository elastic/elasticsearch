/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.transport.actions.WatcherTransportAction;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.joda.time.DateTime;

import java.time.Clock;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.joda.time.DateTimeZone.UTC;

public class TransportGetWatchAction extends WatcherTransportAction<GetWatchRequest, GetWatchResponse> {

    private final Watch.Parser parser;
    private final Clock clock;
    private final Client client;

    @Inject
    public TransportGetWatchAction(Settings settings, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver, XPackLicenseState licenseState,
                                   Watch.Parser parser, Clock clock, InternalClient client, ClusterService clusterService) {
        super(settings, GetWatchAction.NAME, transportService, threadPool, actionFilters, indexNameExpressionResolver,
                licenseState, clusterService, GetWatchRequest::new, GetWatchResponse::new);
        this.parser = parser;
        this.clock = clock;
        this.client = client;
    }

    @Override
    protected void masterOperation(GetWatchRequest request, ClusterState state,
                                   ActionListener<GetWatchResponse> listener) throws Exception {
        GetRequest getRequest = new GetRequest(Watch.INDEX, Watch.DOC_TYPE, request.getId())
                .preference(Preference.LOCAL.type()).realtime(true);

        client.get(getRequest, ActionListener.wrap(getResponse -> {
            if (getResponse.isExists()) {
                try (XContentBuilder builder = jsonBuilder()) {
                    // When we return the watch via the Get Watch REST API, we want to return the watch as was specified in the put api,
                    // we don't include the status in the watch source itself, but as a separate top level field, so that
                    // it indicates the the status is managed by watcher itself.
                    DateTime now = new DateTime(clock.millis(), UTC);
                    Watch watch = parser.parseWithSecrets(request.getId(), true, getResponse.getSourceAsBytesRef(), now, XContentType.JSON);
                    watch.toXContent(builder, WatcherParams.builder()
                            .hideSecrets(true)
                            .put(Watch.INCLUDE_STATUS_KEY, false)
                            .build());
                    watch.version(getResponse.getVersion());
                    watch.status().version(getResponse.getVersion());
                    listener.onResponse(new GetWatchResponse(watch.id(), watch.status(), builder.bytes(), XContentType.JSON));
                }
            } else {
                listener.onResponse(new GetWatchResponse(request.getId()));
            }
        }, e -> {
            // special case. This API should not care if the index is missing or not, it should respond with the watch not being found
            if (e instanceof IndexNotFoundException) {
                listener.onResponse(new GetWatchResponse(request.getId()));
            } else {
                listener.onFailure(e);
            }
        }));
    }
}
