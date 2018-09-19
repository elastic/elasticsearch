/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.similarity;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.search.similarities.Similarity.SimWeight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public final class SimilarityService extends AbstractIndexComponent {

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(LogManager.getLogger(SimilarityService.class));
    public static final String DEFAULT_SIMILARITY = "BM25";
    private static final String CLASSIC_SIMILARITY = "classic";
    private static final Map<String, Function<Version, Supplier<Similarity>>> DEFAULTS;
    public static final Map<String, TriFunction<Settings, Version, ScriptService, Similarity>> BUILT_IN;
    static {
        Map<String, Function<Version, Supplier<Similarity>>> defaults = new HashMap<>();
        defaults.put(CLASSIC_SIMILARITY, version -> {
            final ClassicSimilarity similarity = SimilarityProviders.createClassicSimilarity(Settings.EMPTY, version);
            return () -> {
                DEPRECATION_LOGGER.deprecated("The [classic] similarity is now deprecated in favour of BM25, which is generally "
                        + "accepted as a better alternative. Use the [BM25] similarity or build a custom [scripted] similarity "
                        + "instead.");
                return similarity;
            };
        });
        defaults.put("BM25", version -> {
            final BM25Similarity similarity = SimilarityProviders.createBM25Similarity(Settings.EMPTY, version);
            return () -> similarity;
        });
        defaults.put("boolean", version -> {
            final Similarity similarity = new BooleanSimilarity();
            return () -> similarity;
        });

        Map<String, TriFunction<Settings, Version, ScriptService, Similarity>> builtIn = new HashMap<>();
        builtIn.put(CLASSIC_SIMILARITY,
                (settings, version, script) -> {
                    DEPRECATION_LOGGER.deprecated("The [classic] similarity is now deprecated in favour of BM25, which is generally "
                            + "accepted as a better alternative. Use the [BM25] similarity or build a custom [scripted] similarity "
                            + "instead.");
                    return SimilarityProviders.createClassicSimilarity(settings, version);
                });
        builtIn.put("BM25",
                (settings, version, scriptService) -> SimilarityProviders.createBM25Similarity(settings, version));
        builtIn.put("boolean",
                (settings, version, scriptService) -> SimilarityProviders.createBooleanSimilarity(settings, version));
        builtIn.put("DFR",
                (settings, version, scriptService) -> SimilarityProviders.createDfrSimilarity(settings, version));
        builtIn.put("IB",
                (settings, version, scriptService) -> SimilarityProviders.createIBSimilarity(settings, version));
        builtIn.put("LMDirichlet",
                (settings, version, scriptService) -> SimilarityProviders.createLMDirichletSimilarity(settings, version));
        builtIn.put("LMJelinekMercer",
                (settings, version, scriptService) -> SimilarityProviders.createLMJelinekMercerSimilarity(settings, version));
        builtIn.put("DFI",
                (settings, version, scriptService) -> SimilarityProviders.createDfiSimilarity(settings, version));
        builtIn.put("scripted", new ScriptedSimilarityProvider());
        DEFAULTS = Collections.unmodifiableMap(defaults);
        BUILT_IN = Collections.unmodifiableMap(builtIn);
    }

    private final Similarity defaultSimilarity;
    private final Map<String, Supplier<Similarity>> similarities;

    public SimilarityService(IndexSettings indexSettings, ScriptService scriptService,
                             Map<String, TriFunction<Settings, Version, ScriptService, Similarity>> similarities) {
        super(indexSettings);
        Map<String, Supplier<Similarity>> providers = new HashMap<>(similarities.size());
        Map<String, Settings> similaritySettings = this.indexSettings.getSettings().getGroups(IndexModule.SIMILARITY_SETTINGS_PREFIX);

        for (Map.Entry<String, Settings> entry : similaritySettings.entrySet()) {
            String name = entry.getKey();
            // Starting with v5.0 indices, it should no longer be possible to redefine built-in similarities
            if(BUILT_IN.containsKey(name) && indexSettings.getIndexVersionCreated().onOrAfter(Version.V_5_0_0_alpha1)) {
                throw new IllegalArgumentException("Cannot redefine built-in Similarity [" + name + "]");
            }
            Settings providerSettings = entry.getValue();
            String typeName = providerSettings.get("type");
            if (typeName == null) {
                throw new IllegalArgumentException("Similarity [" + name + "] must have an associated type");
            } else if ((similarities.containsKey(typeName) || BUILT_IN.containsKey(typeName)) == false) {
                throw new IllegalArgumentException("Unknown Similarity type [" + typeName + "] for [" + name + "]");
            }
            TriFunction<Settings, Version, ScriptService, Similarity> defaultFactory = BUILT_IN.get(typeName);
            TriFunction<Settings, Version, ScriptService, Similarity> factory = similarities.getOrDefault(typeName, defaultFactory);
            Similarity similarity = factory.apply(providerSettings, indexSettings.getIndexVersionCreated(), scriptService);
            validateSimilarity(indexSettings.getIndexVersionCreated(), similarity);
            providers.put(name, () -> similarity);
        }
        for (Map.Entry<String, Function<Version, Supplier<Similarity>>> entry : DEFAULTS.entrySet()) {
            providers.put(entry.getKey(), entry.getValue().apply(indexSettings.getIndexVersionCreated()));
        }
        this.similarities = providers;
        defaultSimilarity = (providers.get("default") != null) ? providers.get("default").get()
                                                              : providers.get(SimilarityService.DEFAULT_SIMILARITY).get();
        if (providers.get("base") != null) {
            DEPRECATION_LOGGER.deprecated("The [base] similarity is ignored since query normalization and coords have been removed");
        }
    }

    public Similarity similarity(MapperService mapperService) {
        // TODO we can maybe factor out MapperService here entirely by introducing an interface for the lookup?
        return (mapperService != null) ? new PerFieldSimilarity(defaultSimilarity, mapperService) :
                defaultSimilarity;
    }


    public SimilarityProvider getSimilarity(String name) {
        Supplier<Similarity> sim = similarities.get(name);
        if (sim == null) {
            return null;
        }
        return new SimilarityProvider(name, sim.get());
    }

    // for testing
    Similarity getDefaultSimilarity() {
        return defaultSimilarity;
    }

    static class PerFieldSimilarity extends PerFieldSimilarityWrapper {

        private final Similarity defaultSimilarity;
        private final MapperService mapperService;

        PerFieldSimilarity(Similarity defaultSimilarity, MapperService mapperService) {
            super();
            this.defaultSimilarity = defaultSimilarity;
            this.mapperService = mapperService;
        }

        @Override
        public Similarity get(String name) {
            MappedFieldType fieldType = mapperService.fullName(name);
            return (fieldType != null && fieldType.similarity() != null) ? fieldType.similarity().get() : defaultSimilarity;
        }
    }

    static void validateSimilarity(Version indexCreatedVersion, Similarity similarity) {
        try {
            validateScoresArePositive(indexCreatedVersion, similarity);
            validateScoresDoNotDecreaseWithFreq(indexCreatedVersion, similarity);
            validateScoresDoNotIncreaseWithNorm(indexCreatedVersion, similarity);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class SingleNormLeafReader extends LeafReader {

        private final long norm;

        SingleNormLeafReader(long norm) {
            this.norm = norm;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return null;
        }

        @Override
        public Terms terms(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public NumericDocValues getNormValues(String field) throws IOException {
            return new NumericDocValues() {

                int doc = -1;

                @Override
                public long longValue() throws IOException {
                    return norm;
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    doc = target;
                    return true;
                }

                @Override
                public int docID() {
                    return doc;
                }

                @Override
                public int nextDoc() throws IOException {
                    return advance(doc + 1);
                }

                @Override
                public int advance(int target) throws IOException {
                    if (target == 0) {
                        return doc = 0;
                    } else {
                        return doc = NO_MORE_DOCS;
                    }
                }

                @Override
                public long cost() {
                    return 1;
                }

            };
        }

        @Override
        public FieldInfos getFieldInfos() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Bits getLiveDocs() {
            return null;
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkIntegrity() throws IOException {}

        @Override
        public LeafMetaData getMetaData() {
            return new LeafMetaData(
                    org.apache.lucene.util.Version.LATEST.major,
                    org.apache.lucene.util.Version.LATEST,
                    null);
        }

        @Override
        public Fields getTermVectors(int docID) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int numDocs() {
            return 1;
        }

        @Override
        public int maxDoc() {
            return 1;
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void doClose() throws IOException {
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            throw new UnsupportedOperationException();
        }

    }

    private static void validateScoresArePositive(Version indexCreatedVersion, Similarity similarity) throws IOException {
        CollectionStatistics collectionStats = new CollectionStatistics("some_field", 1200, 1100, 3000, 2000);
        TermStatistics termStats = new TermStatistics(new BytesRef("some_value"), 100, 130);
        SimWeight simWeight = similarity.computeWeight(2f, collectionStats, termStats);
        FieldInvertState state = new FieldInvertState(indexCreatedVersion.luceneVersion.major,
                "some_field", 20, 20, 0, 50); // length = 20, no overlap
        final long norm = similarity.computeNorm(state);
        LeafReader reader = new SingleNormLeafReader(norm);
        SimScorer scorer = similarity.simScorer(simWeight, reader.getContext());
        for (int freq = 1; freq <= 10; ++freq) {
            float score = scorer.score(0, freq);
            if (score < 0) {
                DEPRECATION_LOGGER.deprecated("Similarities should not return negative scores:\n" +
                        scorer.explain(0, Explanation.match(freq, "term freq")));
                break;
            }
        }
    }

    private static void validateScoresDoNotDecreaseWithFreq(Version indexCreatedVersion, Similarity similarity) throws IOException {
        CollectionStatistics collectionStats = new CollectionStatistics("some_field", 1200, 1100, 3000, 2000);
        TermStatistics termStats = new TermStatistics(new BytesRef("some_value"), 100, 130);
        SimWeight simWeight = similarity.computeWeight(2f, collectionStats, termStats);
        FieldInvertState state = new FieldInvertState(indexCreatedVersion.luceneVersion.major,
                "some_field", 20, 20, 0, 50); // length = 20, no overlap
        final long norm = similarity.computeNorm(state);
        LeafReader reader = new SingleNormLeafReader(norm);
        SimScorer scorer = similarity.simScorer(simWeight, reader.getContext());
        float previousScore = Float.NEGATIVE_INFINITY;
        for (int freq = 1; freq <= 10; ++freq) {
            float score = scorer.score(0, freq);
            if (score < previousScore) {
                DEPRECATION_LOGGER.deprecated("Similarity scores should not decrease when term frequency increases:\n" +
                        scorer.explain(0, Explanation.match(freq - 1, "term freq")) + "\n" +
                        scorer.explain(0, Explanation.match(freq, "term freq")));
                break;
            }
            previousScore = score;
        }
    }

    private static void validateScoresDoNotIncreaseWithNorm(Version indexCreatedVersion, Similarity similarity) throws IOException {
        CollectionStatistics collectionStats = new CollectionStatistics("some_field", 1200, 1100, 3000, 2000);
        TermStatistics termStats = new TermStatistics(new BytesRef("some_value"), 100, 130);
        SimWeight simWeight = similarity.computeWeight(2f, collectionStats, termStats);

        SimScorer previousScorer = null;
        long previousNorm = 0;
        float previousScore = Float.POSITIVE_INFINITY;
        for (int length = 1; length <= 10; ++length) {
            FieldInvertState state = new FieldInvertState(indexCreatedVersion.luceneVersion.major,
                    "some_field", length, length, 0, 50); // length = 20, no overlap
            final long norm = similarity.computeNorm(state);
            if (Long.compareUnsigned(previousNorm, norm) > 0) {
                // esoteric similarity, skip this check
                break;
            }
            LeafReader reader = new SingleNormLeafReader(norm);
            SimScorer scorer = similarity.simScorer(simWeight, reader.getContext());
            float score = scorer.score(0, 1);
            if (score > previousScore) {
                DEPRECATION_LOGGER.deprecated("Similarity scores should not increase when norm increases:\n" +
                        previousScorer.explain(0, Explanation.match(1, "term freq")) + "\n" +
                        scorer.explain(0, Explanation.match(1, "term freq")));
                break;
            }
            previousScorer = scorer;
            previousScore = score;
            previousNorm = norm;
        }
    }

}
