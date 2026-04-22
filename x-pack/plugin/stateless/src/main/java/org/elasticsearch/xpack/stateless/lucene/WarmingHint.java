/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.apache.lucene.store.IOContext;

/**
 * A hint that the file is being pre-warmed
 */
enum WarmingHint implements IOContext.FileOpenHint {
    INSTANCE
}
