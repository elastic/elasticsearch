/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.List;

/**
 * Discovers parallelizable splits for an external data source.
 * File-based sources produce one split per file; connector-based sources
 * may use their own split discovery logic.
 */
public interface SplitProvider {

    List<ExternalSplit> discoverSplits(SplitDiscoveryContext context);

    SplitProvider SINGLE = ctx -> List.of();
}
