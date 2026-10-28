/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
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
        int analyzersWithMultipleSynonymGraphFilters = randomIntBetween(0, 10);
        int indicesWithMultipleSynonymGraphFilters = randomIntBetween(0, analyzersWithMultipleSynonymGraphFilters);

        return new AnalysisStats(
            charFilters,
            tokenizers,
            tokenFilters,
            analyzers,
            builtInCharFilters,
            builtInTokenizers,
            builtInTokenFilters,
            builtInAnalyzers,
            synonymsStats,
            analyzersWithMultipleSynonymGraphFilters,
            indicesWithMultipleSynonymGraphFilters
        );
    }

    @Override
    protected AnalysisStats mutateInstance(AnalysisStats instance) {
        Set<IndexFeatureStats> charFilters = instance.getUsedCharFilterTypes();
        Set<IndexFeatureStats> tokenizers = instance.getUsedTokenizerTypes();
        Set<IndexFeatureStats> tokenFilters = instance.getUsedTokenFilterTypes();
        Set<IndexFeatureStats> analyzers = instance.getUsedAnalyzerTypes();
        Set<IndexFeatureStats> builtInCharFilters = instance.getUsedBuiltInCharFilters();
        Set<IndexFeatureStats> builtInTokenizers = instance.getUsedBuiltInTokenizers();
        Set<IndexFeatureStats> builtInTokenFilters = instance.getUsedBuiltInTokenFilters();
        Set<IndexFeatureStats> builtInAnalyzers = instance.getUsedBuiltInAnalyzers();
        Map<String, SynonymsStats> synonyms = instance.getUsedSynonyms();
        int analyzerCount = instance.getAnalyzersWithMultipleSynonymGraphFilters();
        int indexCount = instance.getIndicesWithMultipleSynonymGraphFilters();

        switch (randomInt(9)) {
            case 0 -> {
                charFilters = new HashSet<>(charFilters);
                if (charFilters.removeIf(s -> s.getName().equals("pattern_replace")) == false) {
                    charFilters.add(randomStats("pattern_replace"));
                }
            }
            case 1 -> {
                tokenizers = new HashSet<>(tokenizers);
                if (tokenizers.removeIf(s -> s.getName().equals("whitespace")) == false) {
                    tokenizers.add(randomStats("whitespace"));
                }
            }
            case 2 -> {
                tokenFilters = new HashSet<>(tokenFilters);
                if (tokenFilters.removeIf(s -> s.getName().equals("stop")) == false) {
                    tokenFilters.add(randomStats("stop"));
                }
            }
            case 3 -> {
                analyzers = new HashSet<>(analyzers);
                if (analyzers.removeIf(s -> s.getName().equals("english")) == false) {
                    analyzers.add(randomStats("english"));
                }
            }
            case 4 -> {
                builtInCharFilters = new HashSet<>(builtInCharFilters);
                if (builtInCharFilters.removeIf(s -> s.getName().equals("html_strip")) == false) {
                    builtInCharFilters.add(randomStats("html_strip"));
                }
            }
            case 5 -> {
                builtInTokenizers = new HashSet<>(builtInTokenizers);
                if (builtInTokenizers.removeIf(s -> s.getName().equals("keyword")) == false) {
                    builtInTokenizers.add(randomStats("keyword"));
                }
            }
            case 6 -> {
                builtInTokenFilters = new HashSet<>(builtInTokenFilters);
                if (builtInTokenFilters.removeIf(s -> s.getName().equals("trim")) == false) {
                    builtInTokenFilters.add(randomStats("trim"));
                }
            }
            case 7 -> {
                builtInAnalyzers = new HashSet<>(builtInAnalyzers);
                if (builtInAnalyzers.removeIf(s -> s.getName().equals("french")) == false) {
                    builtInAnalyzers.add(randomStats("french"));
                }
            }
            case 8 -> synonyms = randomValueOtherThan(synonyms, AnalysisStatsTests::randomSynonymsStats);
            case 9 -> {
                analyzerCount = randomValueOtherThan(analyzerCount, () -> randomIntBetween(0, 10));
                indexCount = randomIntBetween(0, analyzerCount);
            }
            default -> throw new AssertionError();
        }

        return new AnalysisStats(
            charFilters,
            tokenizers,
            tokenFilters,
            analyzers,
            builtInCharFilters,
            builtInTokenizers,
            builtInTokenFilters,
            builtInAnalyzers,
            synonyms,
            analyzerCount,
            indexCount
        );
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

    /**
     * Builds a JSON fragment for a single synonym_graph filter entry, randomly choosing between
     * synonyms_set, inline synonyms, and synonyms_path as the synonym source.
     * This lets a single test cover all source types without three near-identical copies.
     */
    private static String randomSynonymGraphFilter(String filterName, int idx) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> Strings.format("""
                "%s": { "type": "synonym_graph", "synonyms_set": "set-%d", "updateable": true }""", filterName, idx);
            case 1 -> Strings.format("""
                "%s": { "type": "synonym_graph", "synonyms": ["foo%d, bar%d"] }""", filterName, idx, idx);
            case 2 -> Strings.format("""
                "%s": { "type": "synonym_graph", "synonyms_path": "synonyms%d.txt" }""", filterName, idx);
            default -> throw new AssertionError();
        };
    }

    public void testMultipleSynonymGraphFiltersStats() {
        // Index with an analyzer that has two synonym_graph filters (synonym source randomized)
        String settingsWithMultiple = Strings.format("""
            {
              "index": {
                "analysis": {
                  "filter": {
                    %s,
                    %s
                  },
                  "analyzer": {
                    "my_analyzer": {
                      "type": "custom",
                      "tokenizer": "standard",
                      "filter": ["syn_graph_1", "syn_graph_2"]
                    }
                  }
                }
              }
            }
            """, randomSynonymGraphFilter("syn_graph_1", 1), randomSynonymGraphFilter("syn_graph_2", 2));
        Settings settings1 = indexSettings(IndexVersion.current(), 4, 1).loadFromSource(settingsWithMultiple, XContentType.JSON).build();
        IndexMetadata index1 = new IndexMetadata.Builder("idx_multiple").settings(settings1).build();

        // Index with a single synonym_graph filter (should not count)
        String settingsWithSingle = Strings.format("""
            {
              "index": {
                "analysis": {
                  "filter": {
                    %s
                  },
                  "analyzer": {
                    "single_syn_analyzer": {
                      "type": "custom",
                      "tokenizer": "standard",
                      "filter": ["syn_graph_only"]
                    }
                  }
                }
              }
            }
            """, randomSynonymGraphFilter("syn_graph_only", 1));
        Settings settings2 = indexSettings(IndexVersion.current(), 4, 1).loadFromSource(settingsWithSingle, XContentType.JSON).build();
        IndexMetadata index2 = new IndexMetadata.Builder("idx_single").settings(settings2).build();

        // Index with two analyzers, each having multiple synonym_graph filters (synonym source randomized per filter)
        String settingsWithTwoAnalyzers = Strings.format("""
            {
              "index": {
                "analysis": {
                  "filter": {
                    %s,
                    %s,
                    %s
                  },
                  "analyzer": {
                    "analyzer_one": {
                      "type": "custom",
                      "tokenizer": "standard",
                      "filter": ["sg_a", "sg_b"]
                    },
                    "analyzer_two": {
                      "type": "custom",
                      "tokenizer": "standard",
                      "filter": ["sg_b", "sg_c"]
                    }
                  }
                }
              }
            }
            """, randomSynonymGraphFilter("sg_a", 1), randomSynonymGraphFilter("sg_b", 2), randomSynonymGraphFilter("sg_c", 3));
        Settings settings3 = indexSettings(IndexVersion.current(), 4, 1).loadFromSource(settingsWithTwoAnalyzers, XContentType.JSON)
            .build();
        IndexMetadata index3 = new IndexMetadata.Builder("idx_two_analyzers").settings(settings3).build();

        Metadata metadata = new Metadata.Builder().build().withAddedIndex(index1).withAddedIndex(index2).withAddedIndex(index3);
        AnalysisStats analysisStats = AnalysisStats.of(metadata, () -> {});

        // 3 analyzers total with multiple synonym_graph filters: my_analyzer, analyzer_one, analyzer_two
        assertEquals(3, analysisStats.getAnalyzersWithMultipleSynonymGraphFilters());
        // 2 indices have at least one such analyzer: idx_multiple and idx_two_analyzers
        assertEquals(2, analysisStats.getIndicesWithMultipleSynonymGraphFilters());
    }

    public void testNoMultipleSynonymGraphFilters() {
        // Index with no synonym filters at all
        final String settingsSource = """
            {
              "index": {
                "analysis": {
                  "analyzer": {
                    "simple_analyzer": {
                      "type": "custom",
                      "tokenizer": "standard",
                      "filter": ["lowercase"]
                    }
                  }
                }
              }
            }
            """;
        Settings settings = indexSettings(IndexVersion.current(), 4, 1).loadFromSource(settingsSource, XContentType.JSON).build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("idx_none").settings(settings).build();

        Metadata metadata = new Metadata.Builder().build().withAddedIndex(indexMetadata);
        AnalysisStats analysisStats = AnalysisStats.of(metadata, () -> {});

        assertEquals(0, analysisStats.getAnalyzersWithMultipleSynonymGraphFilters());
        assertEquals(0, analysisStats.getIndicesWithMultipleSynonymGraphFilters());
    }

    public void testSynonymTypeNotCountedAsMultipleSynonymGraph() {
        // An analyzer with two plain "synonym" (not synonym_graph) filters with synonyms_set should NOT be counted
        final String settingsSource = """
            {
              "index": {
                "analysis": {
                  "filter": {
                    "syn_1": {
                      "type": "synonym",
                      "synonyms_set": "set-1",
                      "updateable": true
                    },
                    "syn_2": {
                      "type": "synonym",
                      "synonyms_set": "set-2",
                      "updateable": true
                    }
                  },
                  "analyzer": {
                    "double_synonym_analyzer": {
                      "type": "custom",
                      "tokenizer": "standard",
                      "filter": ["syn_1", "syn_2"]
                    }
                  }
                }
              }
            }
            """;
        Settings settings = indexSettings(IndexVersion.current(), 4, 1).loadFromSource(settingsSource, XContentType.JSON).build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("idx_synonym_only").settings(settings).build();

        Metadata metadata = new Metadata.Builder().build().withAddedIndex(indexMetadata);
        AnalysisStats analysisStats = AnalysisStats.of(metadata, () -> {});

        assertEquals(0, analysisStats.getAnalyzersWithMultipleSynonymGraphFilters());
        assertEquals(0, analysisStats.getIndicesWithMultipleSynonymGraphFilters());
    }
}
