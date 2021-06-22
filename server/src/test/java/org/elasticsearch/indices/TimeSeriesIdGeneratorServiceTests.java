/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.apache.lucene.util.NamedThreadFactory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.TimeSeriesIdGenerator;
import org.elasticsearch.index.TimeSeriesIdGenerator.ObjectComponent;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.indices.TimeSeriesIdGeneratorService.LocalIndex;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class TimeSeriesIdGeneratorServiceTests extends ESTestCase {
    /**
     * Assert that non-timeseries indices don't call any lookups or build
     * anything and never have a generator.
     */
    public void testNonTimeSeries() {
        try (
            TimeSeriesIdGeneratorService genService = genService(
                i -> { throw new AssertionError("shouldn't be called"); },
                im -> { throw new AssertionError("shouldn't be called"); }
            )
        ) {
            Metadata meta = Metadata.builder().put(index("index", false)).build();
            genService.applyClusterState(meta);
            assertNull(genService.apply(meta.index("index")));
            genService.stop();
        }
    }

    /**
     * Assert that a local time series index loads the time series from the local lookup.
     */
    public void testLocalIndex() {
        Metadata meta = Metadata.builder().put(index("index", true)).build();
        IndexMetadata indexMetadata = meta.index("index");
        TimeSeriesIdGenerator gen = mockGenerator();
        try (TimeSeriesIdGeneratorService genService = genService(i -> new LocalIndex() {
            @Override
            public long metadataVersion() {
                return indexMetadata.getVersion() + between(0, Integer.MAX_VALUE);
            }

            @Override
            public TimeSeriesIdGenerator generator() {
                return gen;
            }
        }, im -> { throw new AssertionError("shouldn't be called"); })) {
            genService.applyClusterState(meta);
            assertThat(genService.apply(indexMetadata), sameInstance(gen));
            genService.stop();
        }
    }

    /**
     * Assert that a local time series index will reuse the previous building if
     * the mapping hasn't changed.
     */
    public void testLocalIndexUnchangedMapping() {
        TimeSeriesIdGenerator gen = mockGenerator();
        AtomicLong counter = new AtomicLong();

        Metadata meta = Metadata.builder().put(index("index", true)).build();
        AtomicReference<IndexMetadata> indexMetadata = new AtomicReference<>(meta.index("index"));
        try (TimeSeriesIdGeneratorService genService = genService(i -> {
            counter.incrementAndGet();
            return new LocalIndex() {
                @Override
                public long metadataVersion() {
                    return indexMetadata.get().getVersion() + between(0, Integer.MAX_VALUE);
                }

                @Override
                public TimeSeriesIdGenerator generator() {
                    return gen;
                }
            };
        }, im -> { throw new AssertionError("shouldn't be called"); })) {
            for (int i = 0; i < 1000; i++) {
                genService.applyClusterState(meta);
                assertThat(genService.apply(indexMetadata.get()), sameInstance(gen));
                assertThat(counter.get(), equalTo(1L));
            }

            // Incrementing the mapping version will cause another fetch
            meta = Metadata.builder()
                .put(IndexMetadata.builder(indexMetadata.get()).mappingVersion(indexMetadata.get().getMappingVersion() + 1))
                .build();
            indexMetadata.set(meta.index("index"));
            genService.applyClusterState(meta);
            assertThat(genService.apply(indexMetadata.get()), sameInstance(gen));
            assertThat(counter.get(), equalTo(2L));
            genService.stop();
        }
    }

    /**
     * Assert that a non local time series index will build its {@link TimeSeriesIdGenerator}.
     */
    public void testNonLocalIndex() throws Exception {
        Metadata meta = Metadata.builder().put(index("index", true)).build();
        IndexMetadata indexMetadata = meta.index("index");
        TimeSeriesIdGenerator gen = mockGenerator();
        try (TimeSeriesIdGeneratorService genService = genService(i -> null, im -> gen)) {
            genService.applyClusterState(meta);
            assertBusy(() -> assertThat(genService.apply(indexMetadata), sameInstance(gen)));
            genService.stop();
        }
    }

    /**
     * Assert that a non local time series index will reuse the previous building if
     * the mapping hasn't changed.
     */
    public void testNonLocalIndexUnchangedMapping() throws Exception {
        TimeSeriesIdGenerator gen = mockGenerator();
        AtomicLong counter = new AtomicLong();

        Metadata meta = Metadata.builder().put(index("index", true)).build();
        AtomicReference<IndexMetadata> indexMetadata = new AtomicReference<>(meta.index("index"));
        try (TimeSeriesIdGeneratorService genService = genService(i -> null, im -> {
            counter.incrementAndGet();
            return gen;
        })) {
            for (int i = 0; i < 1000; i++) {
                genService.applyClusterState(meta);
                assertBusy(() -> assertThat(genService.apply(indexMetadata.get()), sameInstance(gen)));
                assertThat(counter.get(), equalTo(1L));
            }

            // Incrementing the mapping version will cause another fetch
            meta = Metadata.builder()
                .put(IndexMetadata.builder(indexMetadata.get()).mappingVersion(indexMetadata.get().getMappingVersion() + 1))
                .build();
            indexMetadata.set(meta.index("index"));
            genService.applyClusterState(meta);
            assertBusy(() -> assertThat(genService.apply(indexMetadata.get()), sameInstance(gen)));
            assertThat(counter.get(), equalTo(2L));
            genService.stop();
        }
    }

    /**
     * Assert that a non local time series index will reuse the previous building if
     * the mapping hasn't changed.
     */
    public void testNonLocalIndexSameMappingAsLocalIndex() throws Exception {
        TimeSeriesIdGenerator gen = mockGenerator();

        Metadata meta = Metadata.builder().put(index("index_1", true)).put(index("index_2", true)).build();
        try (TimeSeriesIdGeneratorService genService = genService(i -> {
            if (i.getName().equals("index_1")) {
                return new LocalIndex() {
                    @Override
                    public long metadataVersion() {
                        return meta.index("index_1").getVersion();
                    }

                    @Override
                    public TimeSeriesIdGenerator generator() {
                        return gen;
                    }
                };
            }
            return null;
        }, im -> { throw new AssertionError("shouldn't be called"); })) {
            genService.applyClusterState(meta);
            assertThat(genService.apply(meta.index("index_1")), sameInstance(gen));
            assertThat(genService.apply(meta.index("index_2")), sameInstance(gen));
        }
    }

    private TimeSeriesIdGeneratorService genService(
        Function<Index, LocalIndex> lookupLocalIndex,
        Function<IndexMetadata, TimeSeriesIdGenerator> buildTimeSeriedIdGenerator
    ) {
        ExecutorService executor = Executors.newSingleThreadExecutor(new NamedThreadFactory(getTestName()));
        TimeSeriesIdGeneratorService genService = new TimeSeriesIdGeneratorService(executor, lookupLocalIndex, buildTimeSeriedIdGenerator);
        genService.start();
        return genService;
    }

    private IndexMetadata.Builder index(String index, boolean timeSeriesMode) {
        Settings.Builder settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
        if (timeSeriesMode) {
            settings.put(IndexSettings.TIME_SERIES_MODE.getKey(), true);
        }
        return IndexMetadata.builder(index)
            .settings(settings)
            .numberOfShards(between(1, 10))
            .numberOfReplicas(randomInt(20))
            .putMapping("{}");
    }

    private TimeSeriesIdGenerator mockGenerator() {
        return new TimeSeriesIdGenerator(new ObjectComponent(Map.of("a", KeywordFieldMapper.timeSeriesIdGenerator(null))));
    }
}
