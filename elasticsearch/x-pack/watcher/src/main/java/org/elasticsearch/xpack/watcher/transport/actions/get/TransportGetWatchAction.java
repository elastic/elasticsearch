/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.get;

import java.io.IOException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.watcher.WatcherService;
import org.elasticsearch.xpack.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.transport.actions.WatcherTransportAction;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStore;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Performs the get operation.
 */
public class TransportGetWatchAction extends WatcherTransportAction<GetWatchRequest, GetWatchResponse> {

    private final WatcherService watcherService;

    @Inject
    public TransportGetWatchAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver, WatcherService watcherService,
                                   XPackLicenseState licenseState) {
        super(settings, GetWatchAction.NAME, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, licenseState, GetWatchRequest::new);
        this.watcherService = watcherService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME; // Super lightweight operation, so don't fork
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

        try {
            Watch watch = watcherService.getWatch(request.getId());
            if (watch == null) {
                listener.onResponse(new GetWatchResponse(request.getId()));
                return;
            }

            try (XContentBuilder builder = jsonBuilder()) {
                // When we return the watch via the get api, we want to return the watch as was specified in the put api,
                // we don't include the status in the watch source itself, but as a separate top level field, so that
                // it indicates the the status is managed by watcher itself.
                watch.toXContent(builder, WatcherParams.builder().hideSecrets(true).build());
                BytesReference watchSource = builder.bytes();
                listener.onResponse(new GetWatchResponse(watch.id(), watch.status(), watchSource, XContentType.JSON));
            } catch (IOException e) {
                listener.onFailure(e);
            }

        } catch (Exception e) {
            logger.error("failed to get watch [{}]", e, request.getId());
            throw e;
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetWatchRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, WatchStore.INDEX);
    }
}
