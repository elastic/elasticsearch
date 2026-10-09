/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.apache.lucene.store.IOContext;

/**
 * Custom ES-specific hints that selectively opt specific use-cases into
 * MADV_RANDOM on the indexing tier. Each enum value represents a validated
 * use-case. Once all use-cases are validated, these can be collapsed into
 * a single broad policy that honors {@code DataAccessHint.RANDOM} on all tiers.
 */
public enum StatelessAdviceHint implements IOContext.FileOpenHint {
    /**
     * Stored fields data is accessed randomly (by doc ID).
     * Enables MADV_RANDOM for the stored fields data file on the indexing tier.
     */
    STORED_FIELDS
}
