/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.common.unit.ByteSizeUnit;

public class SearchableSnapshotsUtils {

    /**
     * We use {@code long} to represent offsets and lengths of files since they may be larger than 2GB, but {@code int} to represent
     * offsets and lengths of arrays in memory which are limited to 2GB in size. We quite often need to convert from the file-based world
     * of {@code long}s into the memory-based world of {@code int}s, knowing for certain that the result will not overflow. This method
     * should be used to clarify that we're doing this.
     */
    public static int toIntBytes(long l) {
        return ByteSizeUnit.BYTES.toIntBytes(l);
    }
}
