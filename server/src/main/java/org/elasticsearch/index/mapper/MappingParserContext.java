/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.IndexSettings;
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
    private final Version indexVersionCreated;
    private final Supplier<SearchExecutionContext> searchExecutionContextSupplier;
    private final DateFormatter dateFormatter;
    private final ScriptCompiler scriptCompiler;
    private final IndexAnalyzers indexAnalyzers;
    private final IndexSettings indexSettings;
    private final IdFieldMapper idFieldMapper;

    public MappingParserContext(
        Function<String, SimilarityProvider> similarityLookupService,
        Function<String, Mapper.TypeParser> typeParsers,
        Function<String, RuntimeField.Parser> runtimeFieldParsers,
        Version indexVersionCreated,
        Supplier<SearchExecutionContext> searchExecutionContextSupplier,
        DateFormatter dateFormatter,
        ScriptCompiler scriptCompiler,
        IndexAnalyzers indexAnalyzers,
        IndexSettings indexSettings,
        IdFieldMapper idFieldMapper
    ) {
        this.similarityLookupService = similarityLookupService;
        this.typeParsers = typeParsers;
        this.runtimeFieldParsers = runtimeFieldParsers;
        this.indexVersionCreated = indexVersionCreated;
        this.searchExecutionContextSupplier = searchExecutionContextSupplier;
        this.dateFormatter = dateFormatter;
        this.scriptCompiler = scriptCompiler;
        this.indexAnalyzers = indexAnalyzers;
        this.indexSettings = indexSettings;
        this.idFieldMapper = idFieldMapper;
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

    public Version indexVersionCreated() {
        return indexVersionCreated;
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
        return dateFormatter;
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

    static MappingParserContext createMultiFieldContext(MappingParserContext in) {
        return new MultiFieldParserContext(in);
    }

    private static class MultiFieldParserContext extends MappingParserContext {
        MultiFieldParserContext(MappingParserContext in) {
            super(
                in.similarityLookupService,
                in.typeParsers,
                in.runtimeFieldParsers,
                in.indexVersionCreated,
                in.searchExecutionContextSupplier,
                in.dateFormatter,
                in.scriptCompiler,
                in.indexAnalyzers,
                in.indexSettings,
                in.idFieldMapper
            );
        }

        @Override
        public boolean isWithinMultiField() {
            return true;
        }
    }

    static class DynamicTemplateParserContext extends MappingParserContext {
        DynamicTemplateParserContext(MappingParserContext in) {
            super(
                in.similarityLookupService,
                in.typeParsers,
                in.runtimeFieldParsers,
                in.indexVersionCreated,
                in.searchExecutionContextSupplier,
                in.dateFormatter,
                in.scriptCompiler,
                in.indexAnalyzers,
                in.indexSettings,
                in.idFieldMapper
            );
        }

        @Override
        public boolean isFromDynamicTemplate() {
            return true;
        }
    }
}
