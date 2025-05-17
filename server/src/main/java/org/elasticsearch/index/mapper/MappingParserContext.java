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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.script.ScriptCompiler;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Holds everything that is needed to parse mappings. This is carried around while parsing mappings whether that
 * be from a dynamic template or from index mappings themselves.
 */
public class MappingParserContext {

    private final Function<String, SimilarityProvider> similarityLookupService;
    private final Function<String, Mapper.TypeParser> typeParsers;
    private final Function<String, RuntimeField.Parser> runtimeFieldParsers;
    private final IndexVersion indexVersionCreated;
    private final Supplier<TransportVersion> clusterTransportVersion;
    private final Supplier<SearchExecutionContext> searchExecutionContextSupplier;
    private final ScriptCompiler scriptCompiler;
    private final IndexAnalyzers indexAnalyzers;
    private final IndexSettings indexSettings;
    private final IdFieldMapper idFieldMapper;
    private final Function<Query, BitSetProducer> bitSetProducer;
    private final long mappingObjectDepthLimit;
    private final RootObjectMapperNamespaceValidator namespaceValidator;
    private long mappingObjectDepth = 0;

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
        IdFieldMapper idFieldMapper,
        Function<Query, BitSetProducer> bitSetProducer,
        RootObjectMapperNamespaceValidator namespaceValidator
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
        this.idFieldMapper = idFieldMapper;
        this.mappingObjectDepthLimit = indexSettings.getMappingDepthLimit();
        this.bitSetProducer = bitSetProducer;
        this.namespaceValidator = namespaceValidator;
    }

    // MP TODO: only used by tests, so remove this after tests are updated?
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
        IdFieldMapper idFieldMapper,
        Function<Query, BitSetProducer> bitSetProducer
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
            idFieldMapper,
            bitSetProducer,
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

    public IdFieldMapper idFieldMapper() {
        return idFieldMapper;
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

    void incrementMappingObjectDepth() throws MapperParsingException {
        mappingObjectDepth++;
        if (mappingObjectDepth > mappingObjectDepthLimit) {
            throw new MapperParsingException("Limit of mapping depth [" + mappingObjectDepthLimit + "] has been exceeded");
        }
    }

    void decrementMappingObjectDepth() throws MapperParsingException {
        mappingObjectDepth--;
    }

    public MappingParserContext createMultiFieldContext() {
        return new MultiFieldParserContext(this);
    }

    private static class MultiFieldParserContext extends MappingParserContext {
        MultiFieldParserContext(MappingParserContext in) {
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
                in.idFieldMapper,
                in.bitSetProducer
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
                in.idFieldMapper,
                in.bitSetProducer
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
