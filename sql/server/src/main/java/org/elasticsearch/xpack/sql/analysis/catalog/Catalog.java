/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import java.util.List;


public interface Catalog {
    // NOCOMMIT make sure we need all of these methods....

    EsIndex getIndex(String index);

    boolean indexExists(String index);

    List<EsIndex> listIndices();

    List<EsIndex> listIndices(String pattern);

    EsType getType(String index, String type);

    boolean typeExists(String index, String type);

    List<EsType> listTypes(String index);

    List<EsType> listTypes(String index, String pattern);
}
