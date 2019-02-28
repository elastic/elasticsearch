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

package org.elasticsearch.retention_lease_bwc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;

public class RestAddRetentionLeaseHandler extends BaseRestHandler {

    public RestAddRetentionLeaseHandler(final Settings settings, final RestController restController) {
        super(settings);
        restController.registerHandler(RestRequest.Method.PUT, "/{index}/_add_retention_lease", this);
    }

    @Override
    public String getName() {
        return "add_retention_lease";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String index = request.param("index");
        final String id = request.param("id");
        final long retainingSequenceNumber = Long.parseLong(request.param("retaining_sequence_number"));
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear();
        clusterStateRequest.metaData(true);
        clusterStateRequest.indices(index);
        return channel ->
                client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
                    @Override
                    public void onResponse(final ClusterStateResponse clusterStateResponse) {
                        final IndexMetaData indexMetaData = clusterStateResponse.getState().metaData().index(index);
                        final int numberOfShards = IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(indexMetaData.getSettings());

                        final GroupedActionListener<RetentionLeaseActions.Response> listener = new GroupedActionListener<>(
                                new RestActionListener<Collection<RetentionLeaseActions.Response>>(channel) {

                                    @Override
                                    protected void processResponse(
                                            final Collection<RetentionLeaseActions.Response> responses) throws Exception {
                                        final XContentBuilder builder = channel.newBuilder().startObject().endObject();
                                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                                    }

                                },
                                numberOfShards,
                                Collections.emptyList());
                        for (int i = 0; i < numberOfShards; i++) {
                            final ShardId shardId = new ShardId(indexMetaData.getIndex(), i);
                            client.execute(
                                    RetentionLeaseActions.Add.INSTANCE,
                                    new RetentionLeaseActions.AddRequest(shardId, id, retainingSequenceNumber, "rest"),
                                    listener);
                        }
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        try {
                            channel.sendResponse(new BytesRestResponse(channel, RestStatus.SERVICE_UNAVAILABLE, e));
                        } catch (IOException inner) {
                            inner.addSuppressed(e);
                            throw new UncheckedIOException(inner);
                        }
                    }
                });
    }
}
