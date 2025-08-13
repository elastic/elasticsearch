/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transport.actions;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchAction;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.ClockHolder;
import org.elasticsearch.xpack.watcher.watch.WatchParser;

import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * This action internally has two modes of operation - an insert and an update mode
 *
 * The insert mode will simply put a watch and that is it.
 * The update mode is a bit more complex and uses versioning. First this prevents the
 * last-write-wins issue, when two users store the same watch. This could happen due
 * to UI users. To prevent this a version is required to trigger the update mode.
 * This mode has been mainly introduced to deal with updates, where the user does not
 * need to provide secrets like passwords for basic auth or sending emails. If this
 * is an update, the watch will not parse the secrets coming in, and the resulting JSON
 * to store the new watch will not contain a password allowing for updates.
 *
 * Internally both requests result in an update call, albeit with different parameters and
 * use of versioning as well as setting the docAsUpsert boolean.
 */
public class TransportPutWatchAction extends WatcherTransportAction<PutWatchRequest, PutWatchResponse> {

    private final ThreadPool threadPool;
    private final Clock clock;
    private final WatchParser parser;
    private final Client client;
    private final ClusterService clusterService;
    private static final ToXContent.Params DEFAULT_PARAMS = WatcherParams.builder()
        .hideSecrets(false)
        .hideHeaders(false)
        .includeStatus(true)
        .build();

    @Inject
    public TransportPutWatchAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ClockHolder clockHolder,
        XPackLicenseState licenseState,
        WatchParser parser,
        Client client,
        ClusterService clusterService
    ) {
        super(PutWatchAction.NAME, transportService, actionFilters, licenseState, PutWatchRequest::new);
        this.threadPool = threadPool;
        this.clock = clockHolder.clock;
        this.parser = parser;
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(PutWatchRequest request, ActionListener<PutWatchResponse> listener) {
        try {
            ZonedDateTime now = clock.instant().atZone(ZoneOffset.UTC);
            boolean isUpdate = request.getVersion() > 0 || request.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO;
            Watch watch = parser.parseWithSecrets(
                request.getId(),
                false,
                request.getSource(),
                now,
                request.xContentType(),
                isUpdate,
                request.getIfSeqNo(),
                request.getIfPrimaryTerm()
            );

            watch.setState(request.isActive(), now);

            // ensure we only filter for the allowed headers
            Map<String, String> filteredHeaders = ClientHelper.getPersistableSafeSecurityHeaders(
                threadPool.getThreadContext(),
                clusterService.state()
            );
            watch.status().setHeaders(filteredHeaders);

            try (XContentBuilder builder = jsonBuilder()) {
                watch.toXContent(builder, DEFAULT_PARAMS);

                if (isUpdate) {
                    UpdateRequest updateRequest = new UpdateRequest(Watch.INDEX, request.getId());
                    if (request.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                        updateRequest.setIfSeqNo(request.getIfSeqNo());
                        updateRequest.setIfPrimaryTerm(request.getIfPrimaryTerm());
                    } else {
                        updateRequest.version(request.getVersion());
                    }
                    updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                    updateRequest.doc(builder);

                    executeAsyncWithOrigin(
                        client.threadPool().getThreadContext(),
                        WATCHER_ORIGIN,
                        updateRequest,
                        ActionListener.<UpdateResponse>wrap(response -> {
                            boolean created = response.getResult() == DocWriteResponse.Result.CREATED;
                            listener.onResponse(
                                new PutWatchResponse(
                                    response.getId(),
                                    response.getVersion(),
                                    response.getSeqNo(),
                                    response.getPrimaryTerm(),
                                    created
                                )
                            );
                        }, listener::onFailure),
                        client::update
                    );
                } else {
                    IndexRequest indexRequest = new IndexRequest(Watch.INDEX).id(request.getId());
                    indexRequest.source(builder);
                    indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                    executeAsyncWithOrigin(
                        client.threadPool().getThreadContext(),
                        WATCHER_ORIGIN,
                        indexRequest,
                        ActionListener.<DocWriteResponse>wrap(response -> {
                            boolean created = response.getResult() == DocWriteResponse.Result.CREATED;
                            listener.onResponse(
                                new PutWatchResponse(
                                    response.getId(),
                                    response.getVersion(),
                                    response.getSeqNo(),
                                    response.getPrimaryTerm(),
                                    created
                                )
                            );
                        }, listener::onFailure),
                        client::index
                    );
                }
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
