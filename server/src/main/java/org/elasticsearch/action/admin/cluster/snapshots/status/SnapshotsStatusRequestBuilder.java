/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.util.ArrayUtils;

/**
 * Snapshots status request builder
 */
public class SnapshotsStatusRequestBuilder extends MasterNodeOperationRequestBuilder<
    SnapshotsStatusRequest,
    SnapshotsStatusResponse,
    SnapshotsStatusRequestBuilder> {

    /**
     * Constructs the new snapshot status request
     */
    public SnapshotsStatusRequestBuilder(ElasticsearchClient client, SnapshotsStatusAction action) {
        super(client, action, new SnapshotsStatusRequest());
    }

    /**
     * Constructs the new snapshot status request with specified repository
     */
    public SnapshotsStatusRequestBuilder(ElasticsearchClient client, SnapshotsStatusAction action, String repository) {
        super(client, action, new SnapshotsStatusRequest(repository));
    }

    /**
     * Sets the repository name
     *
     * @param repository repository name
     * @return this builder
     */
    public SnapshotsStatusRequestBuilder setRepository(String repository) {
        request.repository(repository);
        return this;
    }

    /**
     * Sets list of snapshots to return
     *
     * @param snapshots list of snapshots
     * @return this builder
     */
    public SnapshotsStatusRequestBuilder setSnapshots(String... snapshots) {
        request.snapshots(snapshots);
        return this;
    }

    /**
     * Adds additional snapshots to the list of snapshots to return
     *
     * @param snapshots additional snapshots
     * @return this builder
     */
    public SnapshotsStatusRequestBuilder addSnapshots(String... snapshots) {
        request.snapshots(ArrayUtils.concat(request.snapshots(), snapshots));
        return this;
    }

    /**
     * Set to <code>true</code> to ignore unavailable snapshots, instead of throwing an exception.
     * Defaults to <code>false</code>, which means unavailable snapshots cause an exception to be thrown.
     *
     * @param ignoreUnavailable whether to ignore unavailable snapshots.
     * @return this builder
     */
    public SnapshotsStatusRequestBuilder setIgnoreUnavailable(boolean ignoreUnavailable) {
        request.ignoreUnavailable(ignoreUnavailable);
        return this;
    }
}
