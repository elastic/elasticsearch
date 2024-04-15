/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        long storeQueryTimeInMillis = instance.storeQueryTimeInMillis();
        long cachePutTimeInMillis = instance.cachePutTimeInMillis();
        return switch (between(0, 7)) {
            case 0 -> new CacheStats(
                randomValueOtherThan(count, ESTestCase::randomLong),
                hits,
                misses,
                evictions,
                hitsTimeInMillis,
                missesTimeInMillis,
                storeQueryTimeInMillis,
                cachePutTimeInMillis
            );
            case 1 -> new CacheStats(
                count,
                randomValueOtherThan(hits, ESTestCase::randomLong),
                misses,
                evictions,
                hitsTimeInMillis,
                missesTimeInMillis,
                storeQueryTimeInMillis,
                cachePutTimeInMillis
            );
            case 2 -> new CacheStats(
                count,
                hits,
                randomValueOtherThan(misses, ESTestCase::randomLong),
                evictions,
                hitsTimeInMillis,
                missesTimeInMillis,
                storeQueryTimeInMillis,
                cachePutTimeInMillis
            );
            case 3 -> new CacheStats(
                count,
                hits,
                misses,
                randomValueOtherThan(evictions, ESTestCase::randomLong),
                hitsTimeInMillis,
                missesTimeInMillis,
                storeQueryTimeInMillis,
                cachePutTimeInMillis
            );
            case 4 -> new CacheStats(
                count,
                hits,
                misses,
                evictions,
                randomValueOtherThan(hitsTimeInMillis, ESTestCase::randomLong),
                missesTimeInMillis,
                storeQueryTimeInMillis,
                cachePutTimeInMillis
            );
            case 5 -> new CacheStats(
                count,
                hits,
                misses,
                evictions,
                hitsTimeInMillis,
                randomValueOtherThan(missesTimeInMillis, ESTestCase::randomLong),
                storeQueryTimeInMillis,
                cachePutTimeInMillis
            );
            case 6 -> new CacheStats(
                count,
                hits,
                misses,
                evictions,
                hitsTimeInMillis,
                missesTimeInMillis,
                randomValueOtherThan(storeQueryTimeInMillis, ESTestCase::randomLong),
                cachePutTimeInMillis
            );
            case 7 -> new CacheStats(
                count,
                hits,
                misses,
                evictions,
                hitsTimeInMillis,
                missesTimeInMillis,
                storeQueryTimeInMillis,
                randomValueOtherThan(cachePutTimeInMillis, ESTestCase::randomLong)
            );
            default -> throw new IllegalStateException("Unexpected value");
        };
    }

    static CacheStats createRandomInstance() {
        return new CacheStats(
            randomLong(),
            randomLong(),
            randomLong(),
            randomLong(),
            randomLong(),
            randomLong(),
            randomLong(),
            randomLong()
        );
    }
}
