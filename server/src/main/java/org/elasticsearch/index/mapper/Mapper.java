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
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.script.ScriptService;

import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class Mapper implements ToXContentFragment, Iterable<Mapper> {

    public abstract static class Builder {

        public String name;

        protected Builder(String name) {
            this.name = name;
        }

        public String name() {
            return this.name;
        }

        /** Returns a newly built mapper. */
        public abstract Mapper build(ContentPath contentPath);
    }

    public interface TypeParser {

        class ParserContext {

            private final Function<String, SimilarityProvider> similarityLookupService;
            private final Function<String, TypeParser> typeParsers;
            private final Function<String, RuntimeFieldType.Parser> runtimeTypeParsers;
            private final boolean supportsDynamicRuntimeMappings;
            private final Version indexVersionCreated;
            private final Supplier<SearchExecutionContext> searchExecutionContextSupplier;
            private final DateFormatter dateFormatter;
            private final ScriptService scriptService;
            private final IndexAnalyzers indexAnalyzers;
            private final IndexSettings indexSettings;
            private final BooleanSupplier idFieldDataEnabled;

            public ParserContext(Function<String, SimilarityProvider> similarityLookupService,
                                 Function<String, TypeParser> typeParsers,
                                 Function<String, RuntimeFieldType.Parser> runtimeTypeParsers,
                                 Version indexVersionCreated,
                                 Supplier<SearchExecutionContext> searchExecutionContextSupplier,
                                 DateFormatter dateFormatter,
                                 ScriptService scriptService,
                                 IndexAnalyzers indexAnalyzers,
                                 IndexSettings indexSettings,
                                 BooleanSupplier idFieldDataEnabled,
                                 boolean supportsDynamicRuntimeMappings) {
                this.similarityLookupService = similarityLookupService;
                this.typeParsers = typeParsers;
                this.runtimeTypeParsers = runtimeTypeParsers;
                this.indexVersionCreated = indexVersionCreated;
                this.searchExecutionContextSupplier = searchExecutionContextSupplier;
                this.dateFormatter = dateFormatter;
                this.scriptService = scriptService;
                this.indexAnalyzers = indexAnalyzers;
                this.indexSettings = indexSettings;
                this.idFieldDataEnabled = idFieldDataEnabled;
                this.supportsDynamicRuntimeMappings = supportsDynamicRuntimeMappings;
            }

            public IndexAnalyzers getIndexAnalyzers() {
                return indexAnalyzers;
            }

            public IndexSettings getIndexSettings() {
                return indexSettings;
            }

            public BooleanSupplier isIdFieldDataEnabled() {
                return idFieldDataEnabled;
            }

            public Settings getSettings() {
                return indexSettings.getSettings();
            }

            public SimilarityProvider getSimilarity(String name) {
                return similarityLookupService.apply(name);
            }

            public TypeParser typeParser(String type) {
                return typeParsers.apply(type);
            }

            public RuntimeFieldType.Parser runtimeFieldTypeParser(String type) {
                return runtimeTypeParsers.apply(type);
            }

            public boolean supportsDynamicRuntimeMappings() {
                return supportsDynamicRuntimeMappings;
            }

            public Version indexVersionCreated() {
                return indexVersionCreated;
            }

            public Supplier<SearchExecutionContext> searchExecutionContext() {
                return searchExecutionContextSupplier;
            }

            /**
             * Gets an optional default date format for date fields that do not have an explicit format set
             *
             * If {@code null}, then date fields will default to {@link DateFieldMapper#DEFAULT_DATE_TIME_FORMATTER}.
             */
            public DateFormatter getDateFormatter() {
                return dateFormatter;
            }

            public boolean isWithinMultiField() { return false; }

            /**
             * true if this pars context is coming from parsing dynamic template mappings
             */
            public boolean isFromDynamicTemplate() { return false; }

            protected Function<String, SimilarityProvider> similarityLookupService() { return similarityLookupService; }

            /**
             * The {@linkplain ScriptService} to compile scripts needed by the {@linkplain Mapper}.
             */
            public ScriptService scriptService() {
                return scriptService;
            }

            ParserContext createMultiFieldContext(ParserContext in) {
                return new MultiFieldParserContext(in);
            }

            ParserContext createDynamicTemplateFieldContext(ParserContext in) {
                return new DynamicTemplateParserContext(in);
            }

            private static class MultiFieldParserContext extends ParserContext {
                MultiFieldParserContext(ParserContext in) {
                    super(in.similarityLookupService, in.typeParsers, in.runtimeTypeParsers, in.indexVersionCreated,
                        in.searchExecutionContextSupplier, in.dateFormatter, in.scriptService, in.indexAnalyzers, in.indexSettings,
                        in.idFieldDataEnabled, in.supportsDynamicRuntimeMappings);
                }

                @Override
                public boolean isWithinMultiField() { return true; }
            }

            private static class DynamicTemplateParserContext extends ParserContext {
                DynamicTemplateParserContext(ParserContext in) {
                    super(in.similarityLookupService, in.typeParsers, in.runtimeTypeParsers, in.indexVersionCreated,
                        in.searchExecutionContextSupplier, in.dateFormatter, in.scriptService, in.indexAnalyzers, in.indexSettings,
                        in.idFieldDataEnabled, in.supportsDynamicRuntimeMappings);
                }

                @Override
                public boolean isFromDynamicTemplate() { return true; }
            }
        }

        Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException;
    }

    private final String simpleName;

    public Mapper(String simpleName) {
        Objects.requireNonNull(simpleName);
        this.simpleName = simpleName;
    }

    /** Returns the simple name, which identifies this mapper against other mappers at the same level in the mappers hierarchy
     * TODO: make this protected once Mapper and FieldMapper are merged together */
    public final String simpleName() {
        return simpleName;
    }

    /** Returns the canonical name which uniquely identifies the mapper against other mappers in a type. */
    public abstract String name();

    /**
     * Returns a name representing the type of this mapper.
     */
    public abstract String typeName();

    /** Return the merge of {@code mergeWith} into this.
     *  Both {@code this} and {@code mergeWith} will be left unmodified. */
    public abstract Mapper merge(Mapper mergeWith);

    /**
     * Validate any cross-field references made by this mapper
     * @param mappers a {@link MappingLookup} that can produce references to other mappers
     */
    public abstract void validate(MappingLookup mappers);

}
