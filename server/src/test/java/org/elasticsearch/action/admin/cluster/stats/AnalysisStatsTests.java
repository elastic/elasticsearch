/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class AnalysisStatsTests extends AbstractWireSerializingTestCase<AnalysisStats> {

    private static final Set<String> SYNONYM_RULES_TYPES = AnalysisStats.SYNONYM_STATS_KEYS_FOR_CONFIG.keySet();

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

    private static Map<String, SynonymsStats> randomSynonymsStats() {
        Map<String, SynonymsStats> result = new HashMap<>();
        for (String ruleType : SYNONYM_RULES_TYPES) {
            if (randomBoolean()) {
                SynonymsStats stats = new SynonymsStats();
                stats.indexCount = randomIntBetween(1, 10);
                stats.count = randomIntBetween(stats.indexCount, 100);

                result.put(ruleType, stats);
            }
        }
        return result;
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
        Map<String, SynonymsStats> synonymsStats = randomSynonymsStats();

        return new AnalysisStats(
            charFilters,
            tokenizers,
            tokenFilters,
            analyzers,
            builtInCharFilters,
            builtInTokenizers,
            builtInTokenFilters,
            builtInAnalyzers,
            synonymsStats
        );
    }

    @Override
    protected AnalysisStats mutateInstance(AnalysisStats instance) {
        switch (randomInt(8)) {
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
                    instance.getUsedBuiltInAnalyzers(),
                    instance.getUsedSynonyms()
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
                    instance.getUsedBuiltInAnalyzers(),
                    instance.getUsedSynonyms()
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
                    instance.getUsedBuiltInAnalyzers(),
                    instance.getUsedSynonyms()
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
                    instance.getUsedBuiltInAnalyzers(),
                    instance.getUsedSynonyms()
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
                    instance.getUsedBuiltInAnalyzers(),
                    instance.getUsedSynonyms()
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
                    instance.getUsedBuiltInAnalyzers(),
                    instance.getUsedSynonyms()
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
                    instance.getUsedBuiltInAnalyzers(),
                    instance.getUsedSynonyms()
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
                    builtInAnalyzers,
                    instance.getUsedSynonyms()
                );
            }
            case 8 -> {
                return new AnalysisStats(
                    instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(),
                    instance.getUsedTokenFilterTypes(),
                    instance.getUsedAnalyzerTypes(),
                    instance.getUsedBuiltInCharFilters(),
                    instance.getUsedBuiltInTokenizers(),
                    instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers(),
                    randomValueOtherThan(instance.getUsedSynonyms(), AnalysisStatsTests::randomSynonymsStats)
                );
            }
            default -> throw new AssertionError();
        }

    }

    public void testAccountsRegularIndices() {
        String mapping = """
            {"properties":{"bar":{"type":"text","analyzer":"german"}}}""";
        Settings settings = indexSettings(IndexVersion.current(), 4, 1).build();
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
        Settings settings = indexSettings(IndexVersion.current(), 4, 1).build();
        IndexMetadata.Builder indexMetadata = new IndexMetadata.Builder("foo").settings(settings).putMapping(mapping).system(true);
        Metadata metadata = new Metadata.Builder().put(indexMetadata).build();
        AnalysisStats analysisStats = AnalysisStats.of(metadata, () -> {});
        assertEquals(Collections.emptySet(), analysisStats.getUsedBuiltInAnalyzers());
    }

    public void testChecksForCancellation() {
        IndexMetadata.Builder indexMetadata = new IndexMetadata.Builder("foo").settings(indexSettings(IndexVersion.current(), 4, 1));
        Metadata metadata = new Metadata.Builder().put(indexMetadata).build();
        expectThrows(
            TaskCancelledException.class,
            () -> AnalysisStats.of(metadata, () -> { throw new TaskCancelledException("task cancelled"); })
        );
    }

    public void testSynonymsStats() {
        final String settingsSourceIndex1 = """
            {
              "index": {
                "analysis": {
                  "filter": {
                    "bigram_max_size": {
                      "type": "length",
                      "max": "16",
                      "min": "0"
                    },
                    "synonyms_inline_filter": {
                      "type": "synonym",
                      "synonyms": ["foo, bar", "bar => baz"]
                    },
                    "other_inline_filter": {
                      "type": "synonym",
                      "synonyms": ["foo, bar, baz"]
                    },
                    "synonyms_path_filter": {
                      "type": "synonym",
                      "synonyms_path": "/a/reused/path"
                    },
                    "other_synonyms_path_filter": {
                      "type": "synonym_graph",
                      "synonyms_path": "/a/different/path"
                    },
                    "another_synonyms_path_filter": {
                      "type": "synonym_graph",
                      "synonyms_path": "/another/different/path"
                    },
                    "synonyms_set_filter": {
                      "type": "synonym_graph",
                      "synonyms_set": "reused-synonym-set"
                    }
                  }
                }
              }
            }
            """;
        Settings settings = indexSettings(IndexVersion.current(), 4, 1).loadFromSource(settingsSourceIndex1, XContentType.JSON).build();
        IndexMetadata indexMetadata1 = new IndexMetadata.Builder("foo").settings(settings).build();

        final String settingsSourceIndex2 = """
            {
              "index": {
                "analysis": {
                  "filter": {
                    "en-stem-filter": {
                      "name": "light_english",
                      "type": "stemmer",
                      "language": "light_english"
                    },
                    "other_synonyms_filter": {
                      "type": "synonym",
                      "synonyms_set": "another-synonym-set"
                    },
                    "a_repeated_synonyms_set_filter": {
                      "type": "synonym",
                      "synonyms_set": "reused-synonym-set"
                    },
                    "repeated_inline_filter": {
                      "type": "synonym",
                      "synonyms": ["foo, bar", "bar => baz"]
                    }
                  }
                }
              }
            }
            """;
        Settings settings2 = indexSettings(IndexVersion.current(), 4, 1).loadFromSource(settingsSourceIndex2, XContentType.JSON).build();
        IndexMetadata indexMetadata2 = new IndexMetadata.Builder("bar").settings(settings2).build();

        final String settingsSourceIndex3 = """
            {
              "index": {
                "analysis": {
                  "filter": {
                    "other_synonyms_filter": {
                      "type": "synonym",
                      "synonyms_set": "a-different-synonym-set"
                    },
                    "a_repeated_synonyms_set_filter": {
                      "type": "synonym",
                      "synonyms_set": "reused-synonym-set"
                    },
                    "more_inline_filter": {
                      "type": "synonym",
                      "synonyms": ["foo, bar", "bar => baz"]
                    }
                  }
                }
              }
            }
            """;
        Settings settings3 = indexSettings(IndexVersion.current(), 4, 1).loadFromSource(settingsSourceIndex3, XContentType.JSON).build();
        IndexMetadata indexMetadata3 = new IndexMetadata.Builder("baz").settings(settings3).build();

        Metadata metadata = new Metadata.Builder().build()
            .withAddedIndex(indexMetadata1)
            .withAddedIndex(indexMetadata2)
            .withAddedIndex(indexMetadata3);
        AnalysisStats analysisStats = AnalysisStats.of(metadata, () -> {});
        SynonymsStats expectedSynonymSetStats = new SynonymsStats();
        expectedSynonymSetStats.count = 3;
        expectedSynonymSetStats.indexCount = 3;
        SynonymsStats expectedSynonymPathStats = new SynonymsStats();
        expectedSynonymPathStats.count = 3;
        expectedSynonymPathStats.indexCount = 1;
        SynonymsStats expectedSynonymInlineStats = new SynonymsStats();
        expectedSynonymInlineStats.count = 4;
        expectedSynonymInlineStats.indexCount = 3;

        Map<String, SynonymsStats> expectedSynonymStats = new TreeMap<>();
        expectedSynonymStats.put("sets", expectedSynonymSetStats);
        expectedSynonymStats.put("paths", expectedSynonymPathStats);
        expectedSynonymStats.put("inline", expectedSynonymInlineStats);

        assertEquals(expectedSynonymStats, analysisStats.getUsedSynonyms());
    }
}
