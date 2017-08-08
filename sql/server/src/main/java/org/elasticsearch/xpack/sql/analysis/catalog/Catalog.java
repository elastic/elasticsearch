/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.util.List;


public interface Catalog {
    /**
     * Lookup the information for a table, returning {@code null} if
     * the index is not found.
     * @throws SqlIllegalArgumentException if the index is in some way invalid for use with sql
     */
    @Nullable
    EsIndex getIndex(String index) throws SqlIllegalArgumentException;

    List<EsIndex> listIndices(String pattern);
}
