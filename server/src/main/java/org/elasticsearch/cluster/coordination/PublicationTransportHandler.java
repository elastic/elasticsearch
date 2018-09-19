/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;

public class PublicationTransportHandler {

    public static final String PUBLISH_STATE_ACTION_NAME = "internal:cluster/coordination/publish_state";
    public static final String COMMIT_STATE_ACTION_NAME = "internal:cluster/coordination/commit_state";

    private final TransportService transportService;

    public PublicationTransportHandler(TransportService transportService,
                                       Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest,
                                       Consumer<ApplyCommitRequest> handleApplyCommit) {
        this.transportService = transportService;

        transportService.registerRequestHandler(PUBLISH_STATE_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            in -> new PublishRequest(in, transportService.getLocalNode()),
            (request, channel, task) -> channel.sendResponse(handlePublishRequest.apply(request)));

        transportService.registerRequestHandler(COMMIT_STATE_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            ApplyCommitRequest::new,
            (request, channel, task) -> {
                handleApplyCommit.accept(request);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            });
    }

    public void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                      ActionListener<PublishWithJoinResponse> responseActionListener) {
        // TODO: serialize and compress state similar as in PublishClusterStateAction
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STATE).build();
        transportService.sendRequest(destination, PUBLISH_STATE_ACTION_NAME, publishRequest, options,
            new TransportResponseHandler<PublishWithJoinResponse>() {

                @Override
                public PublishWithJoinResponse read(StreamInput in) throws IOException {
                    return new PublishWithJoinResponse(in);
                }

                @Override
                public void handleResponse(PublishWithJoinResponse response) {
                    responseActionListener.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    responseActionListener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }
            });
    }

    public void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommitRequest,
                                   ActionListener<TransportResponse.Empty> responseActionListener) {
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STATE).build();
        transportService.sendRequest(destination, COMMIT_STATE_ACTION_NAME, applyCommitRequest, options,
            new TransportResponseHandler<TransportResponse.Empty>() {

                @Override
                public TransportResponse.Empty read(StreamInput in) {
                    return TransportResponse.Empty.INSTANCE;
                }

                @Override
                public void handleResponse(TransportResponse.Empty response) {
                    responseActionListener.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    responseActionListener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }
            });
    }

}
