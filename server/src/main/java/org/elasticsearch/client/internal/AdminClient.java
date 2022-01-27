/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.internal;

/**
 * Administrative actions/operations against the cluster or the indices.
 *
 *
 * @see org.elasticsearch.client.internal.Client#admin()
 */
public interface AdminClient {

    /**
     * A client allowing to perform actions/operations against the cluster.
     */
    ClusterAdminClient cluster();

    /**
     * A client allowing to perform actions/operations against the indices.
     */
    IndicesAdminClient indices();
}
