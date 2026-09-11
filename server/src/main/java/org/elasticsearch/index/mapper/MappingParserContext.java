/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.mapper.vectors.VectorsFormatProvider;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.script.ScriptCompiler;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Holds everything that is needed to parse mappings. This is carried around while parsing mappings whether that
 * be from a dynamic template or from index mappings themselves.
 */
public class MappingParserContext {

    /**
     * Tracks field counts and enforces mapping limits during parse. Shared across
     * {@link MultiFieldParserContext} instances so that multi-fields count against the same budget
     * as the parent field. Dynamic-template contexts use {@link #UNLIMITED} to avoid counting
     * template definitions as real fields.
     */
    private static final class ParseFieldLimits {

        static final ParseFieldLimits UNLIMITED = new ParseFieldLimits(Long.MAX_VALUE, Long.MAX_VALUE, NewFieldsBudget.unlimited());

        private final long fieldNameLengthLimit;
        private final long nestedFieldsLimit;
        private long nestedFieldsCount = 0;
        private final NewFieldsBudget totalFieldsBudget;

        private ParseFieldLimits(long fieldNameLengthLimit, long nestedFieldsLimit, NewFieldsBudget totalFieldsBudget) {
            this.fieldNameLengthLimit = fieldNameLengthLimit;
            this.nestedFieldsLimit = nestedFieldsLimit;
            this.totalFieldsBudget = totalFieldsBudget;
        }

        void checkFieldNameLength(String leafName) {
            if (leafName.length() > fieldNameLengthLimit) {
                throw new MapperParsingException(
                    "Field name [" + leafName + "] is longer than the limit of [" + fieldNameLengthLimit + "] characters"
                );
            }
        }

        void checkNestedFieldCount() {
            nestedFieldsCount++;
            if (nestedFieldsCount > nestedFieldsLimit) {
                throw new MapperParsingException("Limit of nested fields [" + nestedFieldsLimit + "] has been exceeded");
            }
        }
    }

    /**
     * Builds the {@link ParseFieldLimits} appropriate for the given merge reason and index settings.
     * Recovery re-uses a mapping that was already validated, so no limits are enforced. Auto-updates
     * in drop-mode use per-field name/nested limits but leave total-fields counting to the merge-time
     * budget. All other updates additionally enforce a parse-time total-fields throwing budget.
     */
    static ParseFieldLimits parseFieldLimits(MapperService.MergeReason reason, IndexSettings indexSettings) {
        if (reason == MapperService.MergeReason.MAPPING_RECOVERY) {
            return ParseFieldLimits.UNLIMITED;
        }
        long nameLimit = indexSettings.getMappingFieldNameLengthLimit();
        long nestedLimit = indexSettings.getMappingNestedFieldsLimit();
        if (reason.isAutoUpdate() && indexSettings.isIgnoreDynamicFieldsBeyondLimit()) {
            return new ParseFieldLimits(nameLimit, nestedLimit, NewFieldsBudget.unlimited());
        }
        long totalLimit = indexSettings.getMappingTotalFieldsLimit();
        return new ParseFieldLimits(nameLimit, nestedLimit, NewFieldsBudget.throwing(totalLimit, totalLimit));
    }

    private final Function<String, SimilarityProvider> similarityLookupService;
    private final Function<String, Mapper.TypeParser> typeParsers;
    private final Function<String, RuntimeField.Parser> runtimeFieldParsers;
    private final IndexVersion indexVersionCreated;
    private final Supplier<TransportVersion> clusterTransportVersion;
    private final Supplier<SearchExecutionContext> searchExecutionContextSupplier;
    private final ScriptCompiler scriptCompiler;
    private final IndexAnalyzers indexAnalyzers;
    private final IndexSettings indexSettings;
    private final Function<Query, BitSetProducer> bitSetProducer;
    private final long mappingObjectDepthLimit;
    // Start at 1 to account for the root object, so the limit semantics match the post-build
    // checkObjectDepthLimit formula (path depth = dots + 2, where root = 1).
    private long mappingObjectDepth = 1;
    private final List<VectorsFormatProvider> vectorsFormatProviders;
    private final RootObjectMapperNamespaceValidator namespaceValidator;
    private final Supplier<ProjectMetadata> projectMetadataSupplier;
    private final ParseFieldLimits parseFieldLimits;

    // Package-private: used by MapperService (to pass ParseFieldLimits directly) and by inner subcontexts.
    MappingParserContext(
        Function<String, SimilarityProvider> similarityLookupService,
        Function<String, Mapper.TypeParser> typeParsers,
        Function<String, RuntimeField.Parser> runtimeFieldParsers,
        IndexVersion indexVersionCreated,
        Supplier<TransportVersion> clusterTransportVersion,
        Supplier<SearchExecutionContext> searchExecutionContextSupplier,
        ScriptCompiler scriptCompiler,
        IndexAnalyzers indexAnalyzers,
        IndexSettings indexSettings,
        Function<Query, BitSetProducer> bitSetProducer,
        List<VectorsFormatProvider> vectorsFormatProviders,
        RootObjectMapperNamespaceValidator namespaceValidator,
        Supplier<ProjectMetadata> projectMetadataSupplier,
        ParseFieldLimits parseFieldLimits
    ) {
        this.similarityLookupService = similarityLookupService;
        this.typeParsers = typeParsers;
        this.runtimeFieldParsers = runtimeFieldParsers;
        this.indexVersionCreated = indexVersionCreated;
        this.clusterTransportVersion = clusterTransportVersion;
        this.searchExecutionContextSupplier = searchExecutionContextSupplier;
        this.scriptCompiler = scriptCompiler;
        this.indexAnalyzers = indexAnalyzers;
        this.indexSettings = indexSettings;
        this.mappingObjectDepthLimit = indexSettings.getMappingDepthLimit();
        this.bitSetProducer = bitSetProducer;
        this.vectorsFormatProviders = vectorsFormatProviders;
        this.namespaceValidator = namespaceValidator;
        this.projectMetadataSupplier = projectMetadataSupplier;
        this.parseFieldLimits = parseFieldLimits;
    }

    public MappingParserContext(
        Function<String, SimilarityProvider> similarityLookupService,
        Function<String, Mapper.TypeParser> typeParsers,
        Function<String, RuntimeField.Parser> runtimeFieldParsers,
        IndexVersion indexVersionCreated,
        Supplier<TransportVersion> clusterTransportVersion,
        Supplier<SearchExecutionContext> searchExecutionContextSupplier,
        ScriptCompiler scriptCompiler,
        IndexAnalyzers indexAnalyzers,
        IndexSettings indexSettings,
        Function<Query, BitSetProducer> bitSetProducer,
        List<VectorsFormatProvider> vectorsFormatProviders,
        RootObjectMapperNamespaceValidator namespaceValidator,
        Supplier<ProjectMetadata> projectMetadataSupplier
    ) {
        this(
            similarityLookupService,
            typeParsers,
            runtimeFieldParsers,
            indexVersionCreated,
            clusterTransportVersion,
            searchExecutionContextSupplier,
            scriptCompiler,
            indexAnalyzers,
            indexSettings,
            bitSetProducer,
            vectorsFormatProviders,
            namespaceValidator,
            projectMetadataSupplier,
            new ParseFieldLimits(
                indexSettings.getMappingFieldNameLengthLimit(),
                indexSettings.getMappingNestedFieldsLimit(),
                NewFieldsBudget.unlimited()
            )
        );
    }

    public MappingParserContext(
        Function<String, SimilarityProvider> similarityLookupService,
        Function<String, Mapper.TypeParser> typeParsers,
        Function<String, RuntimeField.Parser> runtimeFieldParsers,
        IndexVersion indexVersionCreated,
        Supplier<TransportVersion> clusterTransportVersion,
        Supplier<SearchExecutionContext> searchExecutionContextSupplier,
        ScriptCompiler scriptCompiler,
        IndexAnalyzers indexAnalyzers,
        IndexSettings indexSettings,
        Function<Query, BitSetProducer> bitSetProducer,
        List<VectorsFormatProvider> vectorsFormatProviders
    ) {
        this(
            similarityLookupService,
            typeParsers,
            runtimeFieldParsers,
            indexVersionCreated,
            clusterTransportVersion,
            searchExecutionContextSupplier,
            scriptCompiler,
            indexAnalyzers,
            indexSettings,
            bitSetProducer,
            vectorsFormatProviders,
            null,
            null
        );
    }

    public RootObjectMapperNamespaceValidator getNamespaceValidator() {
        return namespaceValidator;
    }

    public IndexAnalyzers getIndexAnalyzers() {
        return indexAnalyzers;
    }

    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    public Settings getSettings() {
        return indexSettings.getSettings();
    }

    public SimilarityProvider getSimilarity(String name) {
        return similarityLookupService.apply(name);
    }

    public Mapper.TypeParser typeParser(String type) {
        return typeParsers.apply(type);
    }

    public RuntimeField.Parser runtimeFieldParser(String type) {
        return runtimeFieldParsers.apply(type);
    }

    public IndexVersion indexVersionCreated() {
        return indexVersionCreated;
    }

    public Supplier<TransportVersion> clusterTransportVersion() {
        return clusterTransportVersion;
    }

    public Supplier<SearchExecutionContext> searchExecutionContext() {
        return searchExecutionContextSupplier;
    }

    /**
     * Gets an optional default date format for date fields that do not have an explicit format set
     * <p>
     * If {@code null}, then date fields will default to {@link DateFieldMapper#DEFAULT_DATE_TIME_FORMATTER}.
     */
    public DateFormatter getDateFormatter() {
        return null;
    }

    public boolean isWithinMultiField() {
        return false;
    }

    /**
     * true if this pars context is coming from parsing dynamic template mappings
     */
    public boolean isFromDynamicTemplate() {
        return false;
    }

    /**
     * The {@linkplain ScriptCompiler} to compile scripts needed by the {@linkplain Mapper}.
     */
    public ScriptCompiler scriptCompiler() {
        return scriptCompiler;
    }

    public BitSetProducer bitSetProducer(Query query) {
        return bitSetProducer.apply(query);
    }

    public List<VectorsFormatProvider> getVectorsFormatProviders() {
        return vectorsFormatProviders;
    }

    void incrementMappingObjectDepth() throws MapperParsingException {
        mappingObjectDepth++;
        if (mappingObjectDepth > mappingObjectDepthLimit) {
            throw new MapperParsingException("Limit of mapping depth [" + mappingObjectDepthLimit + "] has been exceeded");
        }
    }

    void decrementMappingObjectDepth() throws MapperParsingException {
        mappingObjectDepth--;
    }

    /**
     * Checks that adding a dynamic object at {@code fullPath} would not exceed the object depth limit.
     * Depth is computed by counting dots in the path (each dot represents one nesting level).
     * Called eagerly during document parsing to avoid stack-overflow errors from deeply nested objects.
     */
    public void checkObjectDepthLimit(String fullPath) {
        int numDots = 0;
        for (int i = 0; i < fullPath.length(); i++) {
            if (fullPath.charAt(i) == '.') {
                numDots++;
            }
        }
        int depth = numDots + 2;
        if (depth > mappingObjectDepthLimit) {
            throw new MapperParsingException(
                "Limit of mapping depth [" + mappingObjectDepthLimit + "] has been exceeded due to object field [" + fullPath + "]"
            );
        }
    }

    /**
     * Checks that the given leaf name does not exceed the field name length limit.
     * Called at each point where a new field name component is validated during parsing.
     */
    public void checkFieldNameLength(String leafName) {
        parseFieldLimits.checkFieldNameLength(leafName);
    }

    /**
     * Records that a nested object field has been added during parsing,
     * and throws if the nested fields limit is exceeded.
     */
    public void checkNestedFieldCount() {
        parseFieldLimits.checkNestedFieldCount();
    }

    /**
     * Tries to claim {@code count} fields from the parse-time total-fields budget.
     * Returns {@code false} if the budget is exhausted (drop mode); throws
     * {@link IllegalArgumentException} if the budget is exhausted (throwing mode).
     * Always returns {@code true} when the budget is unlimited.
     */
    public boolean tryAddFields(int count) {
        return parseFieldLimits.totalFieldsBudget.decrementIfPossible(count);
    }

    public MappingParserContext createMultiFieldContext() {
        return new MultiFieldParserContext(this);
    }

    public Supplier<ProjectMetadata> getProjectMetadata() {
        return projectMetadataSupplier;
    }

    private static class MultiFieldParserContext extends MappingParserContext {
        MultiFieldParserContext(MappingParserContext in) {
            // Share parseFieldLimits so multi-fields count against the same budget as the parent field.
            super(
                in.similarityLookupService,
                in.typeParsers,
                in.runtimeFieldParsers,
                in.indexVersionCreated,
                in.clusterTransportVersion,
                in.searchExecutionContextSupplier,
                in.scriptCompiler,
                in.indexAnalyzers,
                in.indexSettings,
                in.bitSetProducer,
                in.vectorsFormatProviders,
                in.namespaceValidator,
                null,
                in.parseFieldLimits
            );
        }

        @Override
        public boolean isWithinMultiField() {
            return true;
        }
    }

    public MappingParserContext createDynamicTemplateContext(DateFormatter dateFormatter) {
        return new DynamicTemplateParserContext(this, dateFormatter);
    }

    private static class DynamicTemplateParserContext extends MappingParserContext {

        private final DateFormatter dateFormatter;

        DynamicTemplateParserContext(MappingParserContext in, DateFormatter dateFormatter) {
            super(
                in.similarityLookupService,
                in.typeParsers,
                in.runtimeFieldParsers,
                in.indexVersionCreated,
                in.clusterTransportVersion,
                in.searchExecutionContextSupplier,
                in.scriptCompiler,
                in.indexAnalyzers,
                in.indexSettings,
                in.bitSetProducer,
                in.vectorsFormatProviders,
                in.namespaceValidator,
                null,
                // Use UNLIMITED so that parsing a dynamic template definition does not count against field limits.
                ParseFieldLimits.UNLIMITED
            );
            this.dateFormatter = dateFormatter;
        }

        @Override
        public DateFormatter getDateFormatter() {
            return dateFormatter;
        }

        @Override
        public boolean isFromDynamicTemplate() {
            return true;
        }
    }
}
