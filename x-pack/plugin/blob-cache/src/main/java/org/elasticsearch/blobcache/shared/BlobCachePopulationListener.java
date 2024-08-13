/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

public interface BlobCachePopulationListener {

    BlobCachePopulationListener NOOP = (bc, ctn) -> {};

    /**
     * Notify of a blob cache population that occurred
     *
     * Note that <code>bytesCopied</code> is approximate, there are cases where we write
     * more than we read, due to page alignment, and there are times when we read more
     * than we write, e.g. when filling multiple gaps. Notifiers should try and record
     * the larger of those two numbers when invoking this method.
     *
     * @param bytesCopied The number of bytes copied into the cache
     * @param copyTimeNanos The time in nanoseconds taken to copy those bytes
     */
    void onCachePopulation(int bytesCopied, long copyTimeNanos);
}
