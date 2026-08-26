/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Statistics about analysis usage.
 */
public final class AnalysisStats implements ToXContentFragment, Writeable {

    static final NodeFeature MULTIPLE_SYNONYM_GRAPH_FILTERS_FEATURE = new NodeFeature("cluster.stats.multiple_synonym_graph_filters");

    static final TransportVersion MULTIPLE_SYNONYM_GRAPH_FILTERS_VERSION = TransportVersion.fromName(
        "cluster_stats_multiple_synonym_graph_filters"
    );

    private static final String SYNONYM_FILTER_TYPE = "synonym";
    private static final String SYNONYM_GRAPH_FILTER_TYPE = "synonym_graph";
    private static final Set<String> SYNONYM_FILTER_TYPES = Set.of(SYNONYM_FILTER_TYPE, SYNONYM_GRAPH_FILTER_TYPE);

    private static final String SYNONYMS_INLINE_CONFIG = "synonyms";
    private static final String SYNONYMS_SET_CONFIG = "synonyms_set";
    private static final String SYNONYMS_PATH_CONFIG = "synonyms_path";

    static final Map<String, String> SYNONYM_STATS_KEYS_FOR_CONFIG = Map.of(
        SYNONYMS_INLINE_CONFIG,
        "inline",
        SYNONYMS_SET_CONFIG,
        "sets",
        SYNONYMS_PATH_CONFIG,
        "paths"
    );

    /**
     * Create {@link AnalysisStats} from the given cluster state.
     */
    public static AnalysisStats of(Metadata metadata, Runnable ensureNotCancelled) {
        final Map<String, IndexFeatureStats> usedCharFilterTypes = new HashMap<>();
        final Map<String, IndexFeatureStats> usedTokenizerTypes = new HashMap<>();
        final Map<String, IndexFeatureStats> usedTokenFilterTypes = new HashMap<>();
        final Map<String, IndexFeatureStats> usedAnalyzerTypes = new HashMap<>();
        final Map<String, IndexFeatureStats> usedBuiltInCharFilters = new HashMap<>();
        final Map<String, IndexFeatureStats> usedBuiltInTokenizers = new HashMap<>();
        final Map<String, IndexFeatureStats> usedBuiltInTokenFilters = new HashMap<>();
        final Map<String, IndexFeatureStats> usedBuiltInAnalyzers = new HashMap<>();
        final Map<String, SynonymsStats> usedSynonyms = new HashMap<>();
        final Set<String> synonymsIdsUsedInIndices = new HashSet<>();
        final Set<String> synonymsIdsUsed = new HashSet<>();
        int analyzersWithMultipleSynonymGraphFilters = 0;
        int indicesWithMultipleSynonymGraphFilters = 0;

        final int mappingCount = metadata.projects().values().stream().mapToInt(p -> p.getMappingsByHash().size()).sum();
        final Map<MappingMetadata, Integer> mappingCounts = new IdentityHashMap<>(mappingCount);

        for (ProjectMetadata project : metadata.projects().values()) {
            for (IndexMetadata indexMetadata : project) {
                ensureNotCancelled.run();
                if (indexMetadata.isSystem()) {
                    // Don't include system indices in statistics about analysis,
                    // we care about the user's indices.
                    continue;
                }

                Set<String> indexCharFilters = new HashSet<>();
                Set<String> indexTokenizers = new HashSet<>();
                Set<String> indexTokenFilters = new HashSet<>();

                Set<String> indexAnalyzerTypes = new HashSet<>();
                Set<String> indexCharFilterTypes = new HashSet<>();
                Set<String> indexTokenizerTypes = new HashSet<>();
                Set<String> indexTokenFilterTypes = new HashSet<>();

                Settings indexSettings = indexMetadata.getSettings();
                Map<String, Settings> analyzerSettings = indexSettings.getGroups("index.analysis.analyzer");
                Map<String, Settings> tokenFilterSettings = indexSettings.getGroups("index.analysis.filter");

                Set<String> synonymGraphFilterNames = new HashSet<>();
                for (Map.Entry<String, Settings> entry : tokenFilterSettings.entrySet()) {
                    Settings filterSettings = entry.getValue();
                    if (SYNONYM_GRAPH_FILTER_TYPE.equals(filterSettings.get("type"))) {
                        synonymGraphFilterNames.add(entry.getKey());
                    }
                }

                usedBuiltInAnalyzers.keySet().removeAll(analyzerSettings.keySet());
                boolean indexHasMultipleSynonymGraphFilters = false;
                for (Settings analyzerSetting : analyzerSettings.values()) {
                    final String analyzerType = analyzerSetting.get("type", "custom");
                    IndexFeatureStats stats = usedAnalyzerTypes.computeIfAbsent(analyzerType, IndexFeatureStats::new);
                    stats.count++;
                    if (indexAnalyzerTypes.add(analyzerType)) {
                        stats.indexCount++;
                    }

                    int synonymGraphFilterCount = 0;
                    for (String charFilter : analyzerSetting.getAsList("char_filter")) {
                        stats = usedBuiltInCharFilters.computeIfAbsent(charFilter, IndexFeatureStats::new);
                        stats.count++;
                        if (indexCharFilters.add(charFilter)) {
                            stats.indexCount++;
                        }
                    }

                    String tokenizer = analyzerSetting.get("tokenizer");
                    if (tokenizer != null) {
                        stats = usedBuiltInTokenizers.computeIfAbsent(tokenizer, IndexFeatureStats::new);
                        stats.count++;
                        if (indexTokenizers.add(tokenizer)) {
                            stats.indexCount++;
                        }
                    }

                    for (String filter : analyzerSetting.getAsList("filter")) {
                        stats = usedBuiltInTokenFilters.computeIfAbsent(filter, IndexFeatureStats::new);
                        stats.count++;
                        if (indexTokenFilters.add(filter)) {
                            stats.indexCount++;
                        }
                        if (synonymGraphFilterNames.contains(filter)) {
                            synonymGraphFilterCount++;
                        }
                    }

                    if (synonymGraphFilterCount > 1) {
                        analyzersWithMultipleSynonymGraphFilters++;
                        indexHasMultipleSynonymGraphFilters = true;
                    }
                }
                if (indexHasMultipleSynonymGraphFilters) {
                    indicesWithMultipleSynonymGraphFilters++;
                }

                Map<String, Settings> charFilterSettings = indexSettings.getGroups("index.analysis.char_filter");
                usedBuiltInCharFilters.keySet().removeAll(charFilterSettings.keySet());
                aggregateAnalysisTypes(charFilterSettings.values(), usedCharFilterTypes, indexCharFilterTypes);

                Map<String, Settings> tokenizerSettings = indexSettings.getGroups("index.analysis.tokenizer");
                usedBuiltInTokenizers.keySet().removeAll(tokenizerSettings.keySet());
                aggregateAnalysisTypes(tokenizerSettings.values(), usedTokenizerTypes, indexTokenizerTypes);

                usedBuiltInTokenFilters.keySet().removeAll(tokenFilterSettings.keySet());
                aggregateAnalysisTypes(tokenFilterSettings.values(), usedTokenFilterTypes, indexTokenFilterTypes);
                aggregateSynonymsStats(
                    tokenFilterSettings.values(),
                    usedSynonyms,
                    indexMetadata.getIndex().getName(),
                    synonymsIdsUsed,
                    synonymsIdsUsedInIndices
                );
                countMapping(mappingCounts, indexMetadata);
            }
        }

        for (Map.Entry<MappingMetadata, Integer> mappingAndCount : mappingCounts.entrySet()) {
            ensureNotCancelled.run();
            Set<String> indexAnalyzers = new HashSet<>();
            final int count = mappingAndCount.getValue();
            MappingVisitor.visitMapping(mappingAndCount.getKey().getSourceAsMap(), (field, fieldMapping) -> {
                for (String key : new String[] { "analyzer", "search_analyzer", "search_quote_analyzer" }) {
                    Object analyzerO = fieldMapping.get(key);
                    if (analyzerO != null) {
                        final String analyzer = analyzerO.toString();
                        IndexFeatureStats stats = usedBuiltInAnalyzers.computeIfAbsent(analyzer, IndexFeatureStats::new);
                        stats.count += count;
                        if (indexAnalyzers.add(analyzer)) {
                            stats.indexCount += count;
                        }
                    }
                }
            });
        }

        return new AnalysisStats(
            usedCharFilterTypes.values(),
            usedTokenizerTypes.values(),
            usedTokenFilterTypes.values(),
            usedAnalyzerTypes.values(),
            usedBuiltInCharFilters.values(),
            usedBuiltInTokenizers.values(),
            usedBuiltInTokenFilters.values(),
            usedBuiltInAnalyzers.values(),
            usedSynonyms,
            analyzersWithMultipleSynonymGraphFilters,
            indicesWithMultipleSynonymGraphFilters
        );
    }

    public static void countMapping(Map<MappingMetadata, Integer> mappingCounts, IndexMetadata indexMetadata) {
        final MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata == null) {
            return;
        }
        mappingCounts.compute(mappingMetadata, (k, count) -> count == null ? 1 : count + 1);
    }

    private static void aggregateAnalysisTypes(
        Collection<Settings> settings,
        Map<String, IndexFeatureStats> stats,
        Set<String> indexTypes
    ) {
        for (Settings analysisComponentSettings : settings) {
            final String type = analysisComponentSettings.get("type");
            if (type != null) {
                IndexFeatureStats s = stats.computeIfAbsent(type, IndexFeatureStats::new);
                s.count++;
                if (indexTypes.add(type)) {
                    s.indexCount++;
                }
            }
        }
    }

    private static void aggregateSynonymsStats(
        Collection<Settings> filterSettings,
        Map<String, SynonymsStats> synonymsStats,
        String indexName,
        Set<String> synonymsIdsUsed,
        Set<String> synonymIdsUsedInIndices
    ) {
        for (Settings filterComponentSettings : filterSettings) {
            final String type = filterComponentSettings.get("type");
            if (SYNONYM_FILTER_TYPES.contains(type)) {
                boolean isInline = false;
                String synonymRuleType = SYNONYMS_SET_CONFIG;
                // Avoid requesting settings for synonyms rule type, as it transforms to string a potentially large number of synonym rules
                String synonymId = filterComponentSettings.get(synonymRuleType);
                if (synonymId == null) {
                    synonymRuleType = SYNONYMS_PATH_CONFIG;
                    synonymId = filterComponentSettings.get(synonymRuleType);
                }
                if (synonymId == null) {
                    synonymRuleType = SYNONYMS_INLINE_CONFIG;
                    isInline = true;
                }
                SynonymsStats stat = synonymsStats.computeIfAbsent(
                    SYNONYM_STATS_KEYS_FOR_CONFIG.get(synonymRuleType),
                    id -> new SynonymsStats()
                );
                if (synonymIdsUsedInIndices.add(synonymRuleType + indexName)) {
                    stat.indexCount++;
                }
                if (isInline || synonymsIdsUsed.add(synonymRuleType + synonymId)) {
                    stat.count++;
                }
            }
        }
    }

    private static Set<IndexFeatureStats> sort(Collection<IndexFeatureStats> set) {
        List<IndexFeatureStats> list = new ArrayList<>(set);
        list.sort(Comparator.comparing(IndexFeatureStats::getName));
        return Collections.unmodifiableSet(new LinkedHashSet<>(list));
    }

    private final Set<IndexFeatureStats> usedCharFilters, usedTokenizers, usedTokenFilters, usedAnalyzers;
    private final Set<IndexFeatureStats> usedBuiltInCharFilters, usedBuiltInTokenizers, usedBuiltInTokenFilters, usedBuiltInAnalyzers;

    private final Map<String, SynonymsStats> usedSynonyms;

    /**
     * Number of analyzers across the cluster whose filter chain contains more than one synonym_graph filter.
     */
    private final int analyzersWithMultipleSynonymGraphFilters;

    /**
     * Number of indices that contain at least one analyzer with more than one synonym_graph filter.
     */
    private final int indicesWithMultipleSynonymGraphFilters;

    AnalysisStats(
        Collection<IndexFeatureStats> usedCharFilters,
        Collection<IndexFeatureStats> usedTokenizers,
        Collection<IndexFeatureStats> usedTokenFilters,
        Collection<IndexFeatureStats> usedAnalyzers,
        Collection<IndexFeatureStats> usedBuiltInCharFilters,
        Collection<IndexFeatureStats> usedBuiltInTokenizers,
        Collection<IndexFeatureStats> usedBuiltInTokenFilters,
        Collection<IndexFeatureStats> usedBuiltInAnalyzers,
        Map<String, SynonymsStats> usedSynonyms,
        int analyzersWithMultipleSynonymGraphFilters,
        int indicesWithMultipleSynonymGraphFilters
    ) {
        this.usedCharFilters = sort(usedCharFilters);
        this.usedTokenizers = sort(usedTokenizers);
        this.usedTokenFilters = sort(usedTokenFilters);
        this.usedAnalyzers = sort(usedAnalyzers);
        this.usedBuiltInCharFilters = sort(usedBuiltInCharFilters);
        this.usedBuiltInTokenizers = sort(usedBuiltInTokenizers);
        this.usedBuiltInTokenFilters = sort(usedBuiltInTokenFilters);
        this.usedBuiltInAnalyzers = sort(usedBuiltInAnalyzers);
        this.usedSynonyms = new TreeMap<>(usedSynonyms);
        this.analyzersWithMultipleSynonymGraphFilters = analyzersWithMultipleSynonymGraphFilters;
        this.indicesWithMultipleSynonymGraphFilters = indicesWithMultipleSynonymGraphFilters;
    }

    public AnalysisStats(StreamInput input) throws IOException {
        usedCharFilters = Collections.unmodifiableSet(new LinkedHashSet<>(input.readCollectionAsList(IndexFeatureStats::new)));
        usedTokenizers = Collections.unmodifiableSet(new LinkedHashSet<>(input.readCollectionAsList(IndexFeatureStats::new)));
        usedTokenFilters = Collections.unmodifiableSet(new LinkedHashSet<>(input.readCollectionAsList(IndexFeatureStats::new)));
        usedAnalyzers = Collections.unmodifiableSet(new LinkedHashSet<>(input.readCollectionAsList(IndexFeatureStats::new)));
        usedBuiltInCharFilters = Collections.unmodifiableSet(new LinkedHashSet<>(input.readCollectionAsList(IndexFeatureStats::new)));
        usedBuiltInTokenizers = Collections.unmodifiableSet(new LinkedHashSet<>(input.readCollectionAsList(IndexFeatureStats::new)));
        usedBuiltInTokenFilters = Collections.unmodifiableSet(new LinkedHashSet<>(input.readCollectionAsList(IndexFeatureStats::new)));
        usedBuiltInAnalyzers = Collections.unmodifiableSet(new LinkedHashSet<>(input.readCollectionAsList(IndexFeatureStats::new)));
        usedSynonyms = input.readImmutableMap(SynonymsStats::new);
        if (input.getTransportVersion().supports(MULTIPLE_SYNONYM_GRAPH_FILTERS_VERSION)) {
            analyzersWithMultipleSynonymGraphFilters = input.readVInt();
            indicesWithMultipleSynonymGraphFilters = input.readVInt();
        } else {
            analyzersWithMultipleSynonymGraphFilters = 0;
            indicesWithMultipleSynonymGraphFilters = 0;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(usedCharFilters);
        out.writeCollection(usedTokenizers);
        out.writeCollection(usedTokenFilters);
        out.writeCollection(usedAnalyzers);
        out.writeCollection(usedBuiltInCharFilters);
        out.writeCollection(usedBuiltInTokenizers);
        out.writeCollection(usedBuiltInTokenFilters);
        out.writeCollection(usedBuiltInAnalyzers);
        out.writeMap(usedSynonyms, StreamOutput::writeWriteable);
        if (out.getTransportVersion().supports(MULTIPLE_SYNONYM_GRAPH_FILTERS_VERSION)) {
            out.writeVInt(analyzersWithMultipleSynonymGraphFilters);
            out.writeVInt(indicesWithMultipleSynonymGraphFilters);
        }
    }

    /**
     * Return the set of used char filters in the cluster.
     */
    public Set<IndexFeatureStats> getUsedCharFilterTypes() {
        return usedCharFilters;
    }

    /**
     * Return the set of used tokenizers in the cluster.
     */
    public Set<IndexFeatureStats> getUsedTokenizerTypes() {
        return usedTokenizers;
    }

    /**
     * Return the set of used token filters in the cluster.
     */
    public Set<IndexFeatureStats> getUsedTokenFilterTypes() {
        return usedTokenFilters;
    }

    /**
     * Return the set of used analyzers in the cluster.
     */
    public Set<IndexFeatureStats> getUsedAnalyzerTypes() {
        return usedAnalyzers;
    }

    /**
     * Return the set of used built-in char filters in the cluster.
     */
    public Set<IndexFeatureStats> getUsedBuiltInCharFilters() {
        return usedBuiltInCharFilters;
    }

    /**
     * Return the set of used built-in tokenizers in the cluster.
     */
    public Set<IndexFeatureStats> getUsedBuiltInTokenizers() {
        return usedBuiltInTokenizers;
    }

    /**
     * Return the set of used built-in token filters in the cluster.
     */
    public Set<IndexFeatureStats> getUsedBuiltInTokenFilters() {
        return usedBuiltInTokenFilters;
    }

    /**
     * Return the set of used built-in analyzers in the cluster.
     */
    public Set<IndexFeatureStats> getUsedBuiltInAnalyzers() {
        return usedBuiltInAnalyzers;
    }

    public Map<String, SynonymsStats> getUsedSynonyms() {
        return usedSynonyms;
    }

    /**
     * Return the number of analyzers across the cluster whose filter chain contains more than one synonym_graph filter.
     */
    public int getAnalyzersWithMultipleSynonymGraphFilters() {
        return analyzersWithMultipleSynonymGraphFilters;
    }

    /**
     * Return the number of indices that contain at least one analyzer with more than one synonym_graph filter.
     */
    public int getIndicesWithMultipleSynonymGraphFilters() {
        return indicesWithMultipleSynonymGraphFilters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalysisStats that = (AnalysisStats) o;
        return analyzersWithMultipleSynonymGraphFilters == that.analyzersWithMultipleSynonymGraphFilters
            && indicesWithMultipleSynonymGraphFilters == that.indicesWithMultipleSynonymGraphFilters
            && Objects.equals(usedCharFilters, that.usedCharFilters)
            && Objects.equals(usedTokenizers, that.usedTokenizers)
            && Objects.equals(usedTokenFilters, that.usedTokenFilters)
            && Objects.equals(usedAnalyzers, that.usedAnalyzers)
            && Objects.equals(usedBuiltInCharFilters, that.usedBuiltInCharFilters)
            && Objects.equals(usedBuiltInTokenizers, that.usedBuiltInTokenizers)
            && Objects.equals(usedBuiltInTokenFilters, that.usedBuiltInTokenFilters)
            && Objects.equals(usedBuiltInAnalyzers, that.usedBuiltInAnalyzers)
            && Objects.equals(usedSynonyms, that.usedSynonyms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            usedCharFilters,
            usedTokenizers,
            usedTokenFilters,
            usedAnalyzers,
            usedBuiltInCharFilters,
            usedBuiltInTokenizers,
            usedBuiltInTokenFilters,
            usedBuiltInAnalyzers,
            usedSynonyms,
            analyzersWithMultipleSynonymGraphFilters,
            indicesWithMultipleSynonymGraphFilters
        );
    }

    private static void toXContentCollection(XContentBuilder builder, Params params, String name, Collection<? extends ToXContent> coll)
        throws IOException {
        builder.startArray(name);
        for (ToXContent toXContent : coll) {
            toXContent.toXContent(builder, params);
        }
        builder.endArray();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("analysis");
        toXContentCollection(builder, params, "char_filter_types", usedCharFilters);
        toXContentCollection(builder, params, "tokenizer_types", usedTokenizers);
        toXContentCollection(builder, params, "filter_types", usedTokenFilters);
        toXContentCollection(builder, params, "analyzer_types", usedAnalyzers);
        toXContentCollection(builder, params, "built_in_char_filters", usedBuiltInCharFilters);
        toXContentCollection(builder, params, "built_in_tokenizers", usedBuiltInTokenizers);
        toXContentCollection(builder, params, "built_in_filters", usedBuiltInTokenFilters);
        toXContentCollection(builder, params, "built_in_analyzers", usedBuiltInAnalyzers);
        builder.field("synonyms");
        builder.map(usedSynonyms);
        builder.startObject("multiple_synonym_graph_filters");
        builder.field("analyzer_count", analyzersWithMultipleSynonymGraphFilters);
        builder.field("index_count", indicesWithMultipleSynonymGraphFilters);
        builder.endObject();
        builder.endObject();

        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
