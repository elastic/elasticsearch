/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.metric;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.MetricRecorder;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.junit.After;
import org.junit.Before;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.telemetry.InstrumentType.LONG_ASYNC_COUNTER;
import static org.elasticsearch.telemetry.InstrumentType.LONG_ASYNC_GAUGE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class IndexPrivilegesCacheMetricsTests extends ESTestCase {

    private static final String PREFIX = "es.security.index_privileges.cache";
    private static final String HITS = PREFIX + ".hit.total";
    private static final String MISSES = PREFIX + ".miss.total";
    private static final String EVICTIONS = PREFIX + ".eviction.total";
    private static final String COUNT = PREFIX + ".count.current";
    private static final String MEMORY = PREFIX + ".memory.size";
    private static final String MEMORY_LIMIT = PREFIX + ".memory_limit.size";

    /** The cache is static, so leave it in its default configuration for whoever runs next. */
    @Before
    @After
    public void resetIndexPrivilegeCache() {
        IndexPrivilege.updateConfiguration(Settings.EMPTY);
    }

    /**
     * Fills a small cache past its limit and checks that every instrument reports what the cache reports. After the component closes,
     * the instruments must be gone.
     */
    public void testReportsCacheState() {
        // large enough to hold a few resolved privilege sets, small enough that 100 distinct sets do not fit
        final ByteSizeValue maxSize = ByteSizeValue.ofKb(randomIntBetween(100, 500));
        IndexPrivilege.updateConfiguration(Settings.builder().put(IndexPrivilege.CACHE_SIZE_SETTING.getKey(), maxSize).build());
        final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        final MetricRecorder<Instrument> recorder = meterRegistry.getRecorder();

        try (IndexPrivilegesCacheMetrics ignored = new IndexPrivilegesCacheMetrics(meterRegistry)) {
            collect(recorder);
            assertThat(measure(recorder, LONG_ASYNC_COUNTER, HITS), equalTo(0L));
            assertThat(measure(recorder, LONG_ASYNC_COUNTER, MISSES), equalTo(0L));
            assertThat(measure(recorder, LONG_ASYNC_COUNTER, EVICTIONS), equalTo(0L));
            assertThat(measure(recorder, LONG_ASYNC_GAUGE, COUNT), equalTo(0L));
            assertThat(measure(recorder, LONG_ASYNC_GAUGE, MEMORY), equalTo(0L));
            assertThat(measure(recorder, LONG_ASYNC_GAUGE, MEMORY_LIMIT), equalTo(maxSize.getBytes()));

            final Set<String> names = Set.of("read", "write");
            IndexPrivilege.resolveBySelectorAccess(names);
            IndexPrivilege.resolveBySelectorAccess(names);
            collect(recorder);
            assertThat(measure(recorder, LONG_ASYNC_COUNTER, HITS), equalTo(1L));
            assertThat(measure(recorder, LONG_ASYNC_COUNTER, MISSES), equalTo(1L));
            assertThat(measure(recorder, LONG_ASYNC_COUNTER, EVICTIONS), equalTo(0L));
            assertThat(measure(recorder, LONG_ASYNC_GAUGE, COUNT), equalTo(1L));
            assertThat(measure(recorder, LONG_ASYNC_GAUGE, MEMORY), equalTo(IndexPrivilege.cacheWeightBytes()));
            assertThat(measure(recorder, LONG_ASYNC_GAUGE, MEMORY), greaterThan(0L));

            final List<String> allNames = List.copyOf(IndexPrivilege.names());
            final Set<Set<String>> distinctNames = new HashSet<>();
            while (distinctNames.size() < 100) {
                final Set<String> otherNames = Set.copyOf(randomSubsetOf(randomIntBetween(1, allNames.size()), allNames));
                if (distinctNames.add(otherNames)) {
                    IndexPrivilege.resolveBySelectorAccess(otherNames);
                }
            }
            collect(recorder);
            assertThat(measure(recorder, LONG_ASYNC_COUNTER, EVICTIONS), greaterThan(0L));
            assertThat(measure(recorder, LONG_ASYNC_COUNTER, EVICTIONS), equalTo(IndexPrivilege.cacheStats().getEvictions()));
            assertThat(measure(recorder, LONG_ASYNC_COUNTER, MISSES), equalTo(IndexPrivilege.cacheStats().getMisses()));
            assertThat(measure(recorder, LONG_ASYNC_GAUGE, COUNT), equalTo((long) IndexPrivilege.cacheEntryCount()));
            assertThat(measure(recorder, LONG_ASYNC_GAUGE, MEMORY), equalTo(IndexPrivilege.cacheWeightBytes()));
            assertThat(measure(recorder, LONG_ASYNC_GAUGE, MEMORY), lessThanOrEqualTo(maxSize.getBytes()));

            for (String counter : List.of(HITS, MISSES, EVICTIONS)) {
                assertThat(recorder.getRegisteredMetrics(LONG_ASYNC_COUNTER), hasItem(counter));
            }
            for (String gauge : List.of(COUNT, MEMORY, MEMORY_LIMIT)) {
                assertThat(recorder.getRegisteredMetrics(LONG_ASYNC_GAUGE), hasItem(gauge));
            }
        }

        for (String counter : List.of(HITS, MISSES, EVICTIONS)) {
            assertThat(recorder.getRegisteredMetrics(LONG_ASYNC_COUNTER), not(hasItem(counter)));
        }
        for (String gauge : List.of(COUNT, MEMORY, MEMORY_LIMIT)) {
            assertThat(recorder.getRegisteredMetrics(LONG_ASYNC_GAUGE), not(hasItem(gauge)));
        }
    }

    /** Drops earlier observations so each instrument holds only the measurement taken now. */
    private static void collect(MetricRecorder<Instrument> recorder) {
        recorder.resetCalls();
        recorder.collect();
    }

    private static long measure(MetricRecorder<Instrument> recorder, InstrumentType type, String name) {
        final List<Measurement> measurements = recorder.getMeasurements(type, name);
        assertThat(name, measurements, hasSize(1));
        return measurements.get(0).getLong();
    }
}
