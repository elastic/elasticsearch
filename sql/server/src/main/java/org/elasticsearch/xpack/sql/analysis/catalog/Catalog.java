/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import org.elasticsearch.common.Nullable;

import java.util.List;


public interface Catalog {
    /**
     * Check if an index is valid for sql.
     */
    boolean indexIsValid(String index); // NOCOMMIT should probably be merged into EsCatalog's getIndex method.

    /**
     * Lookup the information for a table, returning {@code null} if
     * the index is not found.
     */
    @Nullable
    EsIndex getIndex(String index);

    List<EsIndex> listIndices(String pattern);
    // NOCOMMIT should these be renamed to getTable and listTables? That seems like a name given that this is a SQL Catalog abstraction. 
}
