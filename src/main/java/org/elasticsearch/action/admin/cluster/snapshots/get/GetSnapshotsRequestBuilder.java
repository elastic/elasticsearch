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

package org.elasticsearch.action.admin.cluster.snapshots.get;

import com.google.common.collect.ObjectArrays;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;

/**
 * Get snapshots request builder
 */
public class GetSnapshotsRequestBuilder extends MasterNodeOperationRequestBuilder<GetSnapshotsRequest, GetSnapshotsResponse, GetSnapshotsRequestBuilder, ClusterAdminClient> {

    /**
     * Constructs the new get snapshot request
     *
     * @param clusterAdminClient cluster admin client
     */
    public GetSnapshotsRequestBuilder(ClusterAdminClient clusterAdminClient) {
        super(clusterAdminClient, new GetSnapshotsRequest());
    }

    /**
     * Constructs the new get snapshot request with specified repository
     *
     * @param clusterAdminClient cluster admin client
     * @param repository         repository name
     */
    public GetSnapshotsRequestBuilder(ClusterAdminClient clusterAdminClient, String repository) {
        super(clusterAdminClient, new GetSnapshotsRequest(repository));
    }

    /**
     * Sets the repository name
     *
     * @param repository repository name
     * @return this builder
     */
    public GetSnapshotsRequestBuilder setRepository(String repository) {
        request.repository(repository);
        return this;
    }

    /**
     * Sets list of snapshots to return
     *
     * @param snapshots list of snapshots
     * @return this builder
     */
    public GetSnapshotsRequestBuilder setSnapshots(String... snapshots) {
        request.snapshots(snapshots);
        return this;
    }

    /**
     * Makes the request to return the current snapshot
     *
     * @return this builder
     */
    public GetSnapshotsRequestBuilder setCurrentSnapshot() {
        request.snapshots(new String[] {GetSnapshotsRequest.CURRENT_SNAPSHOT});
        return this;
    }

    /**
     * Adds additional snapshots to the list of snapshots to return
     *
     * @param snapshots additional snapshots
     * @return this builder
     */
    public GetSnapshotsRequestBuilder addSnapshots(String... snapshots) {
        request.snapshots(ObjectArrays.concat(request.snapshots(), snapshots, String.class));
        return this;
    }

    @Override
    protected void doExecute(ActionListener<GetSnapshotsResponse> listener) {
        client.getSnapshots(request, listener);
    }
}
