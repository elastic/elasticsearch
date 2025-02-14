/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.client.internal;

/**
 * Administrative actions/operations against the cluster or the indices.
 *
 *
 * @see org.elasticsearch.client.internal.Client#admin()
 */
public class AdminClient {

    protected final ClusterAdminClient clusterAdmin;
    protected final IndicesAdminClient indicesAdmin;

    public AdminClient(ElasticsearchClient client) {
        this.clusterAdmin = new ClusterAdminClient(client);
        this.indicesAdmin = new IndicesAdminClient(client);
    }

    public ClusterAdminClient cluster() {
        return clusterAdmin;
    }

    public IndicesAdminClient indices() {
        return indicesAdmin;
    }
}
