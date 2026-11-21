/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import java.util.List;

/**
 * What a table commit needs to write the entry of the table in the catalog: the catalog, the name of the table, and the data about the
 * index that goes into the entry.
 */
public record CatalogRegistration(
    RepositoryCatalog catalog,
    List<String> namespace,
    String name,
    String index,
    String indexUuid,
    String clusterUuid
) {}
