/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

/**
 * Converts from Elasticsearch's internal metadata ({@link ClusterState})
 * into a representation that is compatible with SQL (@{link {@link EsIndex}).
 */
public interface Catalog {
    /**
     * Lookup the information for a table, returning {@code null} if
     * the index is not found.
     * @throws SqlIllegalArgumentException if the index is in some way invalid
     *      for use with SQL
     */
    @Nullable
    EsIndex getIndex(String index) throws SqlIllegalArgumentException;
}
