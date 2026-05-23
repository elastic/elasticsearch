/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip.stats;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class CacheStatsSerializingTests extends AbstractWireSerializingTestCase<CacheStats> {
    @Override
    protected Writeable.Reader<CacheStats> instanceReader() {
        return CacheStats::new;
    }

    @Override
    protected CacheStats createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected CacheStats mutateInstance(CacheStats instance) throws IOException {
        long count = instance.count();
        long hits = instance.hits();
        long misses = instance.misses();
        long evictions = instance.evictions();
        long hitsTimeInMillis = instance.hitsTimeInMillis();
        long missesTimeInMillis = instance.missesTimeInMillis();
        return switch (between(0, 5)) {
            case 0 -> new CacheStats(
                randomValueOtherThan(count, ESTestCase::randomLong),
                hits,
                misses,
                evictions,
                hitsTimeInMillis,
                missesTimeInMillis
            );
            case 1 -> new CacheStats(
                count,
                randomValueOtherThan(hits, ESTestCase::randomLong),
                misses,
                evictions,
                hitsTimeInMillis,
                missesTimeInMillis
            );
            case 2 -> new CacheStats(
                count,
                hits,
                randomValueOtherThan(misses, ESTestCase::randomLong),
                evictions,
                hitsTimeInMillis,
                missesTimeInMillis
            );
            case 3 -> new CacheStats(
                count,
                hits,
                misses,
                randomValueOtherThan(evictions, ESTestCase::randomLong),
                hitsTimeInMillis,
                missesTimeInMillis
            );
            case 4 -> new CacheStats(
                count,
                hits,
                misses,
                evictions,
                randomValueOtherThan(hitsTimeInMillis, ESTestCase::randomLong),
                missesTimeInMillis
            );
            case 5 -> new CacheStats(
                count,
                hits,
                misses,
                evictions,
                hitsTimeInMillis,
                randomValueOtherThan(missesTimeInMillis, ESTestCase::randomLong)
            );
            default -> throw new IllegalStateException("Unexpected value");
        };
    }

    static CacheStats createRandomInstance() {
        return new CacheStats(randomLong(), randomLong(), randomLong(), randomLong(), randomLong(), randomLong());
    }
}
