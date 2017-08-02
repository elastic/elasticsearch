/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import java.util.List;


public interface Catalog {

    boolean indexExists(String index);

    boolean indexIsValid(String index);

    EsIndex getIndex(String index);

    List<EsIndex> listIndices();

    List<EsIndex> listIndices(String pattern);
}
