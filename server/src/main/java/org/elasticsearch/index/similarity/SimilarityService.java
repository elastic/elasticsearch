/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.similarity;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.lucene.similarity.LegacyBM25Similarity;
import org.elasticsearch.script.ScriptService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public final class SimilarityService {
    public static final String DEFAULT_SIMILARITY = "BM25";
    private static final Map<String, Function<IndexVersion, Supplier<Similarity>>> DEFAULTS;
    public static final Map<String, TriFunction<Settings, IndexVersion, ScriptService, Similarity>> BUILT_IN;
    static {
        Map<String, Function<IndexVersion, Supplier<Similarity>>> defaults = new HashMap<>();
        defaults.put("BM25", version -> {
            final LegacyBM25Similarity similarity = SimilarityProviders.createBM25Similarity(Settings.EMPTY, version);
            return () -> similarity;
        });
        defaults.put("boolean", version -> {
            final Similarity similarity = new BooleanSimilarity();
            return () -> similarity;
        });

        Map<String, TriFunction<Settings, IndexVersion, ScriptService, Similarity>> builtIn = new HashMap<>();
        builtIn.put("BM25", (settings, version, scriptService) -> SimilarityProviders.createBM25Similarity(settings, version));
        builtIn.put("boolean", (settings, version, scriptService) -> SimilarityProviders.createBooleanSimilarity(settings, version));
        builtIn.put("DFR", (settings, version, scriptService) -> SimilarityProviders.createDfrSimilarity(settings, version));
        builtIn.put("IB", (settings, version, scriptService) -> SimilarityProviders.createIBSimilarity(settings, version));
        builtIn.put(
            "LMDirichlet",
            (settings, version, scriptService) -> SimilarityProviders.createLMDirichletSimilarity(settings, version)
        );
        builtIn.put(
            "LMJelinekMercer",
            (settings, version, scriptService) -> SimilarityProviders.createLMJelinekMercerSimilarity(settings, version)
        );
        builtIn.put("DFI", (settings, version, scriptService) -> SimilarityProviders.createDfiSimilarity(settings, version));
        builtIn.put("scripted", new ScriptedSimilarityProvider());
        DEFAULTS = Collections.unmodifiableMap(defaults);
        BUILT_IN = Collections.unmodifiableMap(builtIn);
    }

    private final Similarity defaultSimilarity;
    private final Map<String, Supplier<Similarity>> similarities;

    public SimilarityService(
        IndexSettings indexSettings,
        ScriptService scriptService,
        Map<String, TriFunction<Settings, IndexVersion, ScriptService, Similarity>> similarities
    ) {
        Map<String, Supplier<Similarity>> providers = Maps.newMapWithExpectedSize(similarities.size());
        Map<String, Settings> similaritySettings = indexSettings.getSettings().getGroups(IndexModule.SIMILARITY_SETTINGS_PREFIX);

        for (Map.Entry<String, Settings> entry : similaritySettings.entrySet()) {
            String name = entry.getKey();
            if (BUILT_IN.containsKey(name)) {
                throw new IllegalArgumentException("Cannot redefine built-in Similarity [" + name + "]");
            }
            Settings providerSettings = entry.getValue();
            String typeName = providerSettings.get("type");
            if (typeName == null) {
                throw new IllegalArgumentException("Similarity [" + name + "] must have an associated type");
            } else if ((similarities.containsKey(typeName) || BUILT_IN.containsKey(typeName)) == false) {
                throw new IllegalArgumentException("Unknown Similarity type [" + typeName + "] for [" + name + "]");
            }
            TriFunction<Settings, IndexVersion, ScriptService, Similarity> defaultFactory = BUILT_IN.get(typeName);
            TriFunction<Settings, IndexVersion, ScriptService, Similarity> factory = similarities.getOrDefault(typeName, defaultFactory);
            Similarity similarity = factory.apply(providerSettings, indexSettings.getIndexVersionCreated(), scriptService);
            validateSimilarity(indexSettings.getIndexVersionCreated(), similarity);
            if (BUILT_IN.containsKey(typeName) == false || "scripted".equals(typeName)) {
                // We don't trust custom similarities
                similarity = new NonNegativeScoresSimilarity(similarity);
            }
            final Similarity similarityF = similarity; // like similarity but final
            providers.put(name, () -> similarityF);
        }
        for (Map.Entry<String, Function<IndexVersion, Supplier<Similarity>>> entry : DEFAULTS.entrySet()) {
            providers.put(entry.getKey(), entry.getValue().apply(indexSettings.getIndexVersionCreated()));
        }
        this.similarities = providers;
        defaultSimilarity = (providers.get("default") != null)
            ? providers.get("default").get()
            : providers.get(SimilarityService.DEFAULT_SIMILARITY).get();
    }

    /**
     * The similarity to use in searches, which takes into account per-field configuration.
     */
    public Similarity similarity(@Nullable Function<String, MappedFieldType> fieldTypeLookup) {
        return (fieldTypeLookup != null) ? new PerFieldSimilarity(defaultSimilarity, fieldTypeLookup) : defaultSimilarity;
    }

    public SimilarityProvider getSimilarity(String name) {
        Supplier<Similarity> sim = similarities.get(name);
        if (sim == null) {
            return null;
        }
        return new SimilarityProvider(name, sim.get());
    }

    /**
     * The default similarity configured in the index settings.
     */
    public Similarity getDefaultSimilarity() {
        return defaultSimilarity;
    }

    static class PerFieldSimilarity extends PerFieldSimilarityWrapper {

        private final Similarity defaultSimilarity;
        private final Function<String, MappedFieldType> fieldTypeLookup;

        PerFieldSimilarity(Similarity defaultSimilarity, Function<String, MappedFieldType> fieldTypeLookup) {
            super();
            this.defaultSimilarity = defaultSimilarity;
            this.fieldTypeLookup = Objects.requireNonNull(fieldTypeLookup, "fieldTypeLookup cannot be null");
        }

        @Override
        public Similarity get(String name) {
            MappedFieldType fieldType = fieldTypeLookup.apply(name);
            return (fieldType != null && fieldType.getTextSearchInfo().similarity() != null)
                ? fieldType.getTextSearchInfo().similarity().get()
                : defaultSimilarity;
        }
    }

    static void validateSimilarity(IndexVersion indexCreatedVersion, Similarity similarity) {
        validateScoresArePositive(indexCreatedVersion, similarity);
        validateScoresDoNotDecreaseWithFreq(indexCreatedVersion, similarity);
        validateScoresDoNotIncreaseWithNorm(indexCreatedVersion, similarity);
    }

    private static void validateScoresArePositive(IndexVersion indexCreatedVersion, Similarity similarity) {
        CollectionStatistics collectionStats = new CollectionStatistics("some_field", 1200, 1100, 3000, 2000);
        TermStatistics termStats = new TermStatistics(new BytesRef("some_value"), 100, 130);
        SimScorer scorer = similarity.scorer(2f, collectionStats, termStats);
        FieldInvertState state = new FieldInvertState(
            indexCreatedVersion.luceneVersion().major,
            "some_field",
            IndexOptions.DOCS_AND_FREQS,
            20,
            20,
            0,
            50,
            10,
            3
        ); // length = 20, no overlap
        final long norm = similarity.computeNorm(state);
        for (int freq = 1; freq <= 10; ++freq) {
            float score = scorer.score(freq, norm);
            if (score < 0) {
                throw new IllegalArgumentException(
                    "Similarities should not return negative scores:\n" + scorer.explain(Explanation.match(freq, "term freq"), norm)
                );
            }
        }
    }

    private static void validateScoresDoNotDecreaseWithFreq(IndexVersion indexCreatedVersion, Similarity similarity) {
        CollectionStatistics collectionStats = new CollectionStatistics("some_field", 1200, 1100, 3000, 2000);
        TermStatistics termStats = new TermStatistics(new BytesRef("some_value"), 100, 130);
        SimScorer scorer = similarity.scorer(2f, collectionStats, termStats);
        FieldInvertState state = new FieldInvertState(
            indexCreatedVersion.luceneVersion().major,
            "some_field",
            IndexOptions.DOCS_AND_FREQS,
            20,
            20,
            0,
            50,
            10,
            3
        ); // length = 20, no overlap
        final long norm = similarity.computeNorm(state);
        float previousScore = 0;
        for (int freq = 1; freq <= 10; ++freq) {
            float score = scorer.score(freq, norm);
            if (score < previousScore) {
                throw new IllegalArgumentException(
                    "Similarity scores should not decrease when term frequency increases:\n"
                        + scorer.explain(Explanation.match(freq - 1, "term freq"), norm)
                        + "\n"
                        + scorer.explain(Explanation.match(freq, "term freq"), norm)
                );
            }
            previousScore = score;
        }
    }

    private static void validateScoresDoNotIncreaseWithNorm(IndexVersion indexCreatedVersion, Similarity similarity) {
        CollectionStatistics collectionStats = new CollectionStatistics("some_field", 1200, 1100, 3000, 2000);
        TermStatistics termStats = new TermStatistics(new BytesRef("some_value"), 100, 130);
        SimScorer scorer = similarity.scorer(2f, collectionStats, termStats);

        long previousNorm = 0;
        float previousScore = Float.MAX_VALUE;
        for (int length = 1; length <= 10; ++length) {
            FieldInvertState state = new FieldInvertState(
                indexCreatedVersion.luceneVersion().major,
                "some_field",
                IndexOptions.DOCS_AND_FREQS,
                length,
                length,
                0,
                50,
                10,
                3
            ); // length = 20, no overlap
            final long norm = similarity.computeNorm(state);
            if (Long.compareUnsigned(previousNorm, norm) > 0) {
                // esoteric similarity, skip this check
                break;
            }
            float score = scorer.score(1, norm);
            if (score > previousScore) {
                throw new IllegalArgumentException(
                    "Similarity scores should not increase when norm increases:\n"
                        + scorer.explain(Explanation.match(1, "term freq"), norm - 1)
                        + "\n"
                        + scorer.explain(Explanation.match(1, "term freq"), norm)
                );
            }
            previousScore = score;
            previousNorm = norm;
        }
    }
}
