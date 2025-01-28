/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public record CacheStats(long count, long hits, long misses, long evictions, long hitsTimeInMillis, long missesTimeInMillis)
    implements
        Writeable {

    public CacheStats(StreamInput streamInput) throws IOException {
        this(
            streamInput.readLong(),
            streamInput.readLong(),
            streamInput.readLong(),
            streamInput.readLong(),
            streamInput.readLong(),
            streamInput.readLong()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(count);
        out.writeLong(hits);
        out.writeLong(misses);
        out.writeLong(evictions);
        out.writeLong(hitsTimeInMillis);
        out.writeLong(missesTimeInMillis);
    }
}
