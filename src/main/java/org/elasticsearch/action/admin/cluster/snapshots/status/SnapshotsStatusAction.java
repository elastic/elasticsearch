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

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.client.ClusterAdminClient;

/**
 * Snapshots status action
 */
public class SnapshotsStatusAction extends ClusterAction<SnapshotsStatusRequest, SnapshotsStatusResponse, SnapshotsStatusRequestBuilder> {

    public static final SnapshotsStatusAction INSTANCE = new SnapshotsStatusAction();
    public static final String NAME = "cluster:admin/snapshot/status";

    private SnapshotsStatusAction() {
        super(NAME);
    }

    @Override
    public SnapshotsStatusResponse newResponse() {
        return new SnapshotsStatusResponse();
    }

    @Override
    public SnapshotsStatusRequestBuilder newRequestBuilder(ClusterAdminClient client) {
        return new SnapshotsStatusRequestBuilder(client);
    }
}

