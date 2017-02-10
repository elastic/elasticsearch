/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
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
    private final WatcherClientProxy client;

    @Inject
    public TransportGetWatchAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver, XPackLicenseState licenseState,
                                   Watch.Parser parser, Clock clock, WatcherClientProxy client) {
        super(settings, GetWatchAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, licenseState, GetWatchRequest::new);
        this.parser = parser;
        this.clock = clock;
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected GetWatchResponse newResponse() {
        return new GetWatchResponse();
    }

    @Override
    protected void masterOperation(GetWatchRequest request, ClusterState state, ActionListener<GetWatchResponse> listener) throws
            ElasticsearchException {
        if (licenseState.isWatcherAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackPlugin.WATCHER));
            return;
        }

        client.getWatch(request.getId(), ActionListener.wrap(getResponse -> {
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
        }, listener::onFailure));
    }

    @Override
    protected ClusterBlockException checkBlock(GetWatchRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, Watch.INDEX);
    }
}
