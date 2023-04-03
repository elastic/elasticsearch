/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.Version;
import org.elasticsearch.index.analysis.AnalysisMode;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldMapper.Parameter;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Utility functions for text mapper parameters
 */
public final class TextParams {

    private TextParams() {}

    /*
    *       i configured   s configured   sq configured  return
    *   i       y                                           i
    *   i       n                                           default index analyzer
    *   s       *               y                           s
    *   s       y               n                           i
    *   s       n               n                           default search analyzer
    *   sq      *               *               y           sq
    *   sq      *               y               n           s
    *   sq      y               n               n           i
    *   sq      n               n               n           default search quote analyzer
    *
    *
    *
    * */

    public record AnalyzerConfiguration(String indexAnalyzer, String searchAnalyzer, String searchQuoteAnalyzer, int posIncrementGap) {

        public Analyzers buildAnalyzers(IndexAnalyzers indexAnalyzers, boolean isLegacyVersion) {
            return new Analyzers(
                wrap(buildIndexAnalyzer(indexAnalyzers, isLegacyVersion), AnalysisMode.INDEX_TIME),
                wrap(buildSearchAnalyzer(indexAnalyzers, isLegacyVersion), AnalysisMode.SEARCH_TIME),
                wrap(buildSearchQuoteAnalyzer(indexAnalyzers, isLegacyVersion), AnalysisMode.SEARCH_TIME),
                this
            );
        }

        private NamedAnalyzer buildIndexAnalyzer(IndexAnalyzers indexAnalyzers, boolean isLegacyVersion) {
            if (this.indexAnalyzer == null) {
                return indexAnalyzers.getDefaultIndexAnalyzer();
            }
            NamedAnalyzer a = indexAnalyzers.get(this.indexAnalyzer);
            if (a == null) {
                if (isLegacyVersion) {
                    return indexAnalyzers.getDefaultIndexAnalyzer();
                }
                throw new IllegalArgumentException("Unknown analyzer [" + this.indexAnalyzer + "]");
            }
            return a;
        }

        private NamedAnalyzer buildSearchAnalyzer(IndexAnalyzers indexAnalyzers, boolean isLegacyVersion) {
            if (this.searchAnalyzer == null) {
                if (this.indexAnalyzer == null) {
                    return indexAnalyzers.getDefaultSearchAnalyzer();
                }
                return buildIndexAnalyzer(indexAnalyzers, isLegacyVersion);
            }
            NamedAnalyzer a = indexAnalyzers.get(this.searchAnalyzer);
            if (a == null) {
                if (isLegacyVersion) {
                    return indexAnalyzers.getDefaultSearchAnalyzer();
                }
                throw new IllegalArgumentException("Unknown analyzer [" + this.searchAnalyzer + "]");
            }
            return a;
        }

        private NamedAnalyzer buildSearchQuoteAnalyzer(IndexAnalyzers indexAnalyzers, boolean isLegacyVersion) {
            if (this.searchQuoteAnalyzer == null) {
                if (this.searchAnalyzer == null) {
                    if (this.indexAnalyzer == null) {
                        return indexAnalyzers.getDefaultSearchQuoteAnalyzer();
                    }
                    return buildIndexAnalyzer(indexAnalyzers, isLegacyVersion);
                }
                return buildSearchAnalyzer(indexAnalyzers, isLegacyVersion);
            }
            NamedAnalyzer a = indexAnalyzers.get(this.searchQuoteAnalyzer);
            if (a == null) {
                if (isLegacyVersion) {
                    return indexAnalyzers.getDefaultSearchQuoteAnalyzer();
                }
                throw new IllegalArgumentException("Unknown analyzer [" + this.searchQuoteAnalyzer + "]");
            }
            return a;
        }

        private NamedAnalyzer wrap(NamedAnalyzer in, AnalysisMode analysisMode) {
            in.checkAllowedInMode(analysisMode);
            if (in.getPositionIncrementGap("") != posIncrementGap) {
                return new NamedAnalyzer(in, posIncrementGap);
            }
            return in;
        }

    }

    public record Analyzers(
        NamedAnalyzer indexAnalyzer,
        NamedAnalyzer searchAnalyzer,
        NamedAnalyzer searchQuoteAnalyzer,
        AnalyzerConfiguration configuration
    ) {}

    public static final class AnalyzerParameters {
        public final Parameter<String> indexAnalyzer;
        public final Parameter<String> searchAnalyzer;
        public final Parameter<String> searchQuoteAnalyzer;
        public final Parameter<Integer> positionIncrementGap;

        private final Version indexCreatedVersion;

        public AnalyzerParameters(Function<FieldMapper, AnalyzerConfiguration> analyzerInitFunction, Version indexCreatedVersion) {
            this.indexCreatedVersion = indexCreatedVersion;
            this.indexAnalyzer = Parameter.stringParam(
                "analyzer",
                false,  // sort of - see merge validator!
                mapper -> analyzerInitFunction.apply(mapper).indexAnalyzer,
                null,
                // serializer check means that if value is null then we're being asked for defaults
                (builder, name, value) -> builder.field(name, value == null ? "default" : value)
            )
                .setSerializerCheck((includeDefaults, isConfigured, value) -> includeDefaults || value != null)
                .setMergeValidator(
                    // special cases
                    // - we allow 'default' to be merged in to an unconfigured analyzer
                    // - is the index was created a long time ago we allow updates to remove warnings about vanished analyzers
                    (previous, toMerge, conflicts) -> indexCreatedVersion.isLegacyIndexVersion()
                        || Objects.equals(previous, toMerge)
                        || (previous == null && "default".equals(toMerge))
                );
            this.searchAnalyzer = Parameter.stringParam(
                "search_analyzer",
                true,
                mapper -> analyzerInitFunction.apply(mapper).searchAnalyzer,
                null,
                // serializer check means that if value is null then we're being asked for defaults
                (builder, name, value) -> builder.field(name, value == null ? "default" : value)
            ).setSerializerCheck((includeDefaults, isConfigured, value) -> includeDefaults || value != null);
            this.searchQuoteAnalyzer = Parameter.stringParam(
                "search_quote_analyzer",
                true,
                mapper -> analyzerInitFunction.apply(mapper).searchQuoteAnalyzer,
                null,
                // serializer check means that if value is null then we're being asked for defaults
                (builder, name, value) -> builder.field(name, value == null ? "default" : value)
            ).setSerializerCheck((includeDefaults, isConfigured, value) -> includeDefaults || value != null);
            this.positionIncrementGap = Parameter.intParam(
                "position_increment_gap",
                false,
                mapper -> analyzerInitFunction.apply(mapper).posIncrementGap,
                TextFieldMapper.Defaults.POSITION_INCREMENT_GAP
            ).addValidator(v -> {
                if (v < 0) {
                    throw new MapperParsingException("[position_increment_gap] must be positive, got [" + v + "]");
                }
            });
        }

        public Analyzers buildAnalyzers(IndexAnalyzers indexAnalyzers) {
            AnalyzerConfiguration config = new AnalyzerConfiguration(
                indexAnalyzer.get(),
                searchAnalyzer.get(),
                searchQuoteAnalyzer.get(),
                positionIncrementGap.get()
            );
            return config.buildAnalyzers(indexAnalyzers, indexCreatedVersion.isLegacyIndexVersion());
        }
    }

    public static Parameter<Boolean> norms(boolean defaultValue, Function<FieldMapper, Boolean> initializer) {
        // norms can be updated from 'true' to 'false' but not vv
        return Parameter.boolParam("norms", true, initializer, defaultValue).setMergeValidator((o, n, c) -> o == n || (o && n == false));
    }

    public static Parameter<SimilarityProvider> similarity(Function<FieldMapper, SimilarityProvider> init) {
        return new Parameter<>(
            "similarity",
            false,
            () -> null,
            (n, c, o) -> TypeParsers.resolveSimilarity(c, n, o),
            init,
            (b, f, v) -> b.field(f, v == null ? null : v.name()),
            v -> v == null ? null : v.name()
        ).acceptsNull();
    }

    public static Parameter<String> keywordIndexOptions(Function<FieldMapper, String> initializer) {
        return Parameter.stringParam("index_options", false, initializer, "docs").addValidator(v -> {
            switch (v) {
                case "docs":
                case "freqs":
                    return;
                default:
                    throw new MapperParsingException(
                        "Unknown value [" + v + "] for field [index_options] - accepted values are [docs, freqs]"
                    );
            }
        });
    }

    public static Parameter<String> textIndexOptions(Function<FieldMapper, String> initializer) {
        return Parameter.stringParam("index_options", false, initializer, "positions").addValidator(v -> {
            switch (v) {
                case "positions":
                case "docs":
                case "freqs":
                case "offsets":
                    return;
                default:
                    throw new MapperParsingException(
                        "Unknown value [" + v + "] for field [index_options] - accepted values are [positions, docs, freqs, offsets]"
                    );
            }
        });
    }

    public static FieldType buildFieldType(
        Supplier<Boolean> indexed,
        Supplier<Boolean> stored,
        Supplier<String> indexOptions,
        Supplier<Boolean> norms,
        Supplier<String> termVectors
    ) {
        FieldType ft = new FieldType();
        ft.setStored(stored.get());
        ft.setTokenized(true);
        ft.setIndexOptions(toIndexOptions(indexed.get(), indexOptions.get()));
        ft.setOmitNorms(norms.get() == false);
        setTermVectorParams(termVectors.get(), ft);
        return ft;
    }

    public static IndexOptions toIndexOptions(boolean indexed, String indexOptions) {
        if (indexed == false) {
            return IndexOptions.NONE;
        }
        switch (indexOptions) {
            case "docs":
                return IndexOptions.DOCS;
            case "freqs":
                return IndexOptions.DOCS_AND_FREQS;
            case "positions":
                return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
            case "offsets":
                return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        }
        throw new IllegalArgumentException("Unknown [index_options] value: [" + indexOptions + "]");
    }

    public static Parameter<String> termVectors(Function<FieldMapper, String> initializer) {
        return Parameter.stringParam("term_vector", false, initializer, "no").addValidator(v -> {
            switch (v) {
                case "no":
                case "yes":
                case "with_positions":
                case "with_offsets":
                case "with_positions_offsets":
                case "with_positions_payloads":
                case "with_positions_offsets_payloads":
                    return;
                default:
                    throw new MapperParsingException(
                        "Unknown value ["
                            + v
                            + "] for field [term_vector] - accepted values are [no, yes, with_positions, with_offsets, "
                            + "with_positions_offsets, with_positions_payloads, with_positions_offsets_payloads]"
                    );
            }
        });
    }

    public static void setTermVectorParams(String configuration, FieldType fieldType) {
        switch (configuration) {
            case "no" -> {
                fieldType.setStoreTermVectors(false);
                return;
            }
            case "yes" -> {
                fieldType.setStoreTermVectors(true);
                return;
            }
            case "with_positions" -> {
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorPositions(true);
                return;
            }
            case "with_offsets" -> {
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorOffsets(true);
                return;
            }
            case "with_positions_offsets" -> {
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorPositions(true);
                fieldType.setStoreTermVectorOffsets(true);
                return;
            }
            case "with_positions_payloads" -> {
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorPositions(true);
                fieldType.setStoreTermVectorPayloads(true);
                return;
            }
            case "with_positions_offsets_payloads" -> {
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorPositions(true);
                fieldType.setStoreTermVectorOffsets(true);
                fieldType.setStoreTermVectorPayloads(true);
                return;
            }
        }
        throw new IllegalArgumentException("Unknown [term_vector] setting: [" + configuration + "]");
    }

}
