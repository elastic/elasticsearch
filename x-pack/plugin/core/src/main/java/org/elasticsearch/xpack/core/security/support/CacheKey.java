/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Interface in ES Security for objects that can contribute to a cache-key
 */
public interface CacheKey {

    void buildCacheKey(StreamOutput out) throws IOException;
}
