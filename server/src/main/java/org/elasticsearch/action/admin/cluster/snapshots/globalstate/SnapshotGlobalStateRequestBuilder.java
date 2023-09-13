/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.globalstate;

import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class SnapshotGlobalStateRequestBuilder extends MasterNodeOperationRequestBuilder<
    SnapshotGlobalStateRequest,
    SnapshotGlobalStateResponse,
    SnapshotGlobalStateRequestBuilder> {

    public SnapshotGlobalStateRequestBuilder(ElasticsearchClient client, SnapshotGlobalStateAction action, String repository, String name) {
        super(client, action, new SnapshotGlobalStateRequest(repository, name));
    }

    public SnapshotGlobalStateRequestBuilder setSnapshot(String snapshot) {
        request.snapshot();
        return this;
    }
}
