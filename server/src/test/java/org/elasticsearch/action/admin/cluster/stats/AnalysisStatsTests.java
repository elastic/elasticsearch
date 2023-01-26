/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class AnalysisStatsTests extends AbstractWireSerializingTestCase<AnalysisStats> {

    @Override
    protected Reader<AnalysisStats> instanceReader() {
        return AnalysisStats::new;
    }

    private static IndexFeatureStats randomStats(String name) {
        IndexFeatureStats stats = new IndexFeatureStats(name);
        stats.indexCount = randomIntBetween(1, 5);
        stats.count = randomIntBetween(stats.indexCount, 10);
        return stats;
    }

    @Override
    protected AnalysisStats createTestInstance() {
        Set<IndexFeatureStats> charFilters = new HashSet<>();
        if (randomBoolean()) {
            charFilters.add(randomStats("pattern_replace"));
        }

        Set<IndexFeatureStats> tokenizers = new HashSet<>();
        if (randomBoolean()) {
            tokenizers.add(randomStats("whitespace"));
        }

        Set<IndexFeatureStats> tokenFilters = new HashSet<>();
        if (randomBoolean()) {
            tokenFilters.add(randomStats("stop"));
        }

        Set<IndexFeatureStats> analyzers = new HashSet<>();
        if (randomBoolean()) {
            tokenFilters.add(randomStats("english"));
        }

        Set<IndexFeatureStats> builtInCharFilters = new HashSet<>();
        if (randomBoolean()) {
            builtInCharFilters.add(randomStats("html_strip"));
        }

        Set<IndexFeatureStats> builtInTokenizers = new HashSet<>();
        if (randomBoolean()) {
            builtInTokenizers.add(randomStats("keyword"));
        }

        Set<IndexFeatureStats> builtInTokenFilters = new HashSet<>();
        if (randomBoolean()) {
            builtInTokenFilters.add(randomStats("trim"));
        }

        Set<IndexFeatureStats> builtInAnalyzers = new HashSet<>();
        if (randomBoolean()) {
            builtInAnalyzers.add(randomStats("french"));
        }
        return new AnalysisStats(
            charFilters,
            tokenizers,
            tokenFilters,
            analyzers,
            builtInCharFilters,
            builtInTokenizers,
            builtInTokenFilters,
            builtInAnalyzers
        );
    }

    @Override
    protected AnalysisStats mutateInstance(AnalysisStats instance) {
        switch (randomInt(7)) {
            case 0 -> {
                Set<IndexFeatureStats> charFilters = new HashSet<>(instance.getUsedCharFilterTypes());
                if (charFilters.removeIf(s -> s.getName().equals("pattern_replace")) == false) {
                    charFilters.add(randomStats("pattern_replace"));
                }
                return new AnalysisStats(
                    charFilters,
                    instance.getUsedTokenizerTypes(),
                    instance.getUsedTokenFilterTypes(),
                    instance.getUsedAnalyzerTypes(),
                    instance.getUsedBuiltInCharFilters(),
                    instance.getUsedBuiltInTokenizers(),
                    instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers()
                );
            }
            case 1 -> {
                Set<IndexFeatureStats> tokenizers = new HashSet<>(instance.getUsedTokenizerTypes());
                if (tokenizers.removeIf(s -> s.getName().equals("whitespace")) == false) {
                    tokenizers.add(randomStats("whitespace"));
                }
                return new AnalysisStats(
                    instance.getUsedCharFilterTypes(),
                    tokenizers,
                    instance.getUsedTokenFilterTypes(),
                    instance.getUsedAnalyzerTypes(),
                    instance.getUsedBuiltInCharFilters(),
                    instance.getUsedBuiltInTokenizers(),
                    instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers()
                );
            }
            case 2 -> {
                Set<IndexFeatureStats> tokenFilters = new HashSet<>(instance.getUsedTokenFilterTypes());
                if (tokenFilters.removeIf(s -> s.getName().equals("stop")) == false) {
                    tokenFilters.add(randomStats("stop"));
                }
                return new AnalysisStats(
                    instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(),
                    tokenFilters,
                    instance.getUsedAnalyzerTypes(),
                    instance.getUsedBuiltInCharFilters(),
                    instance.getUsedBuiltInTokenizers(),
                    instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers()
                );
            }
            case 3 -> {
                Set<IndexFeatureStats> analyzers = new HashSet<>(instance.getUsedAnalyzerTypes());
                if (analyzers.removeIf(s -> s.getName().equals("english")) == false) {
                    analyzers.add(randomStats("english"));
                }
                return new AnalysisStats(
                    instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(),
                    instance.getUsedTokenFilterTypes(),
                    analyzers,
                    instance.getUsedBuiltInCharFilters(),
                    instance.getUsedBuiltInTokenizers(),
                    instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers()
                );
            }
            case 4 -> {
                Set<IndexFeatureStats> builtInCharFilters = new HashSet<>(instance.getUsedBuiltInCharFilters());
                if (builtInCharFilters.removeIf(s -> s.getName().equals("html_strip")) == false) {
                    builtInCharFilters.add(randomStats("html_strip"));
                }
                return new AnalysisStats(
                    instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(),
                    instance.getUsedTokenFilterTypes(),
                    instance.getUsedAnalyzerTypes(),
                    builtInCharFilters,
                    instance.getUsedBuiltInTokenizers(),
                    instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers()
                );
            }
            case 5 -> {
                Set<IndexFeatureStats> builtInTokenizers = new HashSet<>(instance.getUsedBuiltInTokenizers());
                if (builtInTokenizers.removeIf(s -> s.getName().equals("keyword")) == false) {
                    builtInTokenizers.add(randomStats("keyword"));
                }
                return new AnalysisStats(
                    instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(),
                    instance.getUsedTokenFilterTypes(),
                    instance.getUsedAnalyzerTypes(),
                    instance.getUsedBuiltInCharFilters(),
                    builtInTokenizers,
                    instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers()
                );
            }
            case 6 -> {
                Set<IndexFeatureStats> builtInTokenFilters = new HashSet<>(instance.getUsedBuiltInTokenFilters());
                if (builtInTokenFilters.removeIf(s -> s.getName().equals("trim")) == false) {
                    builtInTokenFilters.add(randomStats("trim"));
                }
                return new AnalysisStats(
                    instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(),
                    instance.getUsedTokenFilterTypes(),
                    instance.getUsedAnalyzerTypes(),
                    instance.getUsedBuiltInCharFilters(),
                    instance.getUsedBuiltInTokenizers(),
                    builtInTokenFilters,
                    instance.getUsedBuiltInAnalyzers()
                );
            }
            case 7 -> {
                Set<IndexFeatureStats> builtInAnalyzers = new HashSet<>(instance.getUsedBuiltInAnalyzers());
                if (builtInAnalyzers.removeIf(s -> s.getName().equals("french")) == false) {
                    builtInAnalyzers.add(randomStats("french"));
                }
                return new AnalysisStats(
                    instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(),
                    instance.getUsedTokenFilterTypes(),
                    instance.getUsedAnalyzerTypes(),
                    instance.getUsedBuiltInCharFilters(),
                    instance.getUsedBuiltInTokenizers(),
                    instance.getUsedBuiltInTokenFilters(),
                    builtInAnalyzers
                );
            }
            default -> throw new AssertionError();
        }

    }

    public void testAccountsRegularIndices() {
        String mapping = """
            {"properties":{"bar":{"type":"text","analyzer":"german"}}}""";
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        Metadata metadata = new Metadata.Builder().put(new IndexMetadata.Builder("foo").settings(settings).putMapping(mapping)).build();
        {
            AnalysisStats analysisStats = AnalysisStats.of(metadata, () -> {});
            IndexFeatureStats expectedStats = new IndexFeatureStats("german");
            expectedStats.count = 1;
            expectedStats.indexCount = 1;
            assertEquals(Collections.singleton(expectedStats), analysisStats.getUsedBuiltInAnalyzers());
        }

        Metadata metadata2 = Metadata.builder(metadata)
            .put(new IndexMetadata.Builder("bar").settings(settings).putMapping(mapping))
            .build();
        {
            AnalysisStats analysisStats = AnalysisStats.of(metadata2, () -> {});
            IndexFeatureStats expectedStats = new IndexFeatureStats("german");
            expectedStats.count = 2;
            expectedStats.indexCount = 2;
            assertEquals(Collections.singleton(expectedStats), analysisStats.getUsedBuiltInAnalyzers());
        }

        Metadata metadata3 = Metadata.builder(metadata2).put(new IndexMetadata.Builder("baz").settings(settings).putMapping("""
            {"properties":{"bar1":{"type":"text","analyzer":"french"},
            "bar2":{"type":"text","analyzer":"french"},"bar3":{"type":"text","analyzer":"french"}}}""")).build();
        {
            AnalysisStats analysisStats = AnalysisStats.of(metadata3, () -> {});
            IndexFeatureStats expectedStatsGerman = new IndexFeatureStats("german");
            expectedStatsGerman.count = 2;
            expectedStatsGerman.indexCount = 2;
            IndexFeatureStats expectedStatsFrench = new IndexFeatureStats("french");
            expectedStatsFrench.count = 3;
            expectedStatsFrench.indexCount = 1;
            assertEquals(Set.of(expectedStatsGerman, expectedStatsFrench), analysisStats.getUsedBuiltInAnalyzers());
        }
    }

    public void testIgnoreSystemIndices() {
        String mapping = """
            {"properties":{"bar":{"type":"text","analyzer":"german"}}}""";
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexMetadata.Builder indexMetadata = new IndexMetadata.Builder("foo").settings(settings).putMapping(mapping).system(true);
        Metadata metadata = new Metadata.Builder().put(indexMetadata).build();
        AnalysisStats analysisStats = AnalysisStats.of(metadata, () -> {});
        assertEquals(Collections.emptySet(), analysisStats.getUsedBuiltInAnalyzers());
    }

    public void testChecksForCancellation() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexMetadata.Builder indexMetadata = new IndexMetadata.Builder("foo").settings(settings);
        Metadata metadata = new Metadata.Builder().put(indexMetadata).build();
        expectThrows(
            TaskCancelledException.class,
            () -> AnalysisStats.of(metadata, () -> { throw new TaskCancelledException("task cancelled"); })
        );
    }
}
