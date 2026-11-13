/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.IndexSettings;

/**
 * The slice of shard state that the columnar batch mapper needs, so that
 * {@link ShardBatchMapper#mapColumnBatch} can be driven without a live shard (benchmarks, tests).
 */
public interface BatchMappingShardContext {

    MapperService mapperService();

    IndexSettings indexSettings();

    long getOperationPrimaryTerm();

    long getRelativeTimeInNanos();
}
