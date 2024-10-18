/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache;

/**
 * The places we populate the cache from
 */
public enum CachePopulationSource {
    /**
     * When loading data from the blob-store
     */
    BlobStore,
    /**
     * When fetching data from a peer node
     */
    Peer,
    /**
     * We cannot determine the source (should not be used except in exceptional cases)
     */
    Unknown
}
