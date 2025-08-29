/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.CompositeSyntheticFieldLoader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.TextParams;
import org.elasticsearch.index.mapper.TextSearchInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A {@link FieldMapper} for full-text log fields that internally splits text into a low cardinality template component
 * and high cardinality argument component. Separating these pieces allows the template component to be highly compressed.
 */
public class PatternedTextFieldMapper extends FieldMapper {

    public static final FeatureFlag PATTERNED_TEXT_MAPPER = new FeatureFlag("patterned_text");

    public static class Defaults {
        public static final FieldType FIELD_TYPE_DOCS;
        public static final FieldType FIELD_TYPE_POSITIONS;

        static {
            final FieldType ft = new FieldType();
            ft.setTokenized(true);
            ft.setStored(false);
            ft.setStoreTermVectors(false);
            ft.setOmitNorms(true);
            ft.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE_DOCS = freezeAndDeduplicateFieldType(ft);
        }

        static {
            final FieldType ft = new FieldType();
            ft.setTokenized(true);
            ft.setStored(false);
            ft.setStoreTermVectors(false);
            ft.setOmitNorms(true);
            ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE_POSITIONS = freezeAndDeduplicateFieldType(ft);
        }
    }

    public static class Builder extends FieldMapper.Builder {

        private final IndexVersion indexCreatedVersion;
        private final IndexSettings indexSettings;
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final TextParams.Analyzers analyzers;
        private final Parameter<String> indexOptions = patternedTextIndexOptions(m -> ((PatternedTextFieldMapper) m).indexOptions);

        public Builder(String name, MappingParserContext context) {
            this(name, context.indexVersionCreated(), context.getIndexSettings(), context.getIndexAnalyzers());
        }

        public Builder(String name, IndexVersion indexCreatedVersion, IndexSettings indexSettings, IndexAnalyzers indexAnalyzers) {
            super(name);
            this.indexCreatedVersion = indexCreatedVersion;
            this.indexSettings = indexSettings;
            this.analyzers = new TextParams.Analyzers(
                indexAnalyzers,
                m -> ((PatternedTextFieldMapper) m).indexAnalyzer,
                m -> ((PatternedTextFieldMapper) m).positionIncrementGap,
                indexCreatedVersion
            );
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta, indexOptions };
        }

        private PatternedTextFieldType buildFieldType(FieldType fieldType, MapperBuilderContext context) {
            NamedAnalyzer searchAnalyzer = analyzers.getSearchAnalyzer();
            NamedAnalyzer searchQuoteAnalyzer = analyzers.getSearchQuoteAnalyzer();
            NamedAnalyzer indexAnalyzer = analyzers.getIndexAnalyzer();
            TextSearchInfo tsi = new TextSearchInfo(fieldType, null, searchAnalyzer, searchQuoteAnalyzer);
            return new PatternedTextFieldType(
                context.buildFullName(leafName()),
                tsi,
                indexAnalyzer,
                context.isSourceSynthetic(),
                meta.getValue()
            );
        }

        private static FieldType buildLuceneFieldType(Supplier<String> indexOptionSupplier) {
            var indexOptions = TextParams.toIndexOptions(true, indexOptionSupplier.get());
            return indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS ? Defaults.FIELD_TYPE_POSITIONS : Defaults.FIELD_TYPE_DOCS;
        }

        private static Parameter<String> patternedTextIndexOptions(Function<FieldMapper, String> initializer) {
            return Parameter.stringParam("index_options", false, initializer, "docs").addValidator(v -> {
                switch (v) {
                    case "positions":
                    case "docs":
                        return;
                    default:
                        throw new MapperParsingException(
                            "Unknown value [" + v + "] for field [index_options] - accepted values are [positions, docs]"
                        );
                }
            });
        }

        @Override
        public PatternedTextFieldMapper build(MapperBuilderContext context) {
            FieldType fieldType = buildLuceneFieldType(indexOptions);
            PatternedTextFieldType patternedTextFieldType = buildFieldType(fieldType, context);
            BuilderParams builderParams = builderParams(this, context);
            var templateIdMapper = KeywordFieldMapper.Builder.buildWithDocValuesSkipper(
                patternedTextFieldType.templateIdFieldName(),
                indexSettings.getMode(),
                indexCreatedVersion,
                true
            ).indexed(false).build(context);
            return new PatternedTextFieldMapper(leafName(), fieldType, patternedTextFieldType, builderParams, this, templateIdMapper);
        }
    }

    public static final TypeParser PARSER = new TypeParser(Builder::new);

    private final IndexVersion indexCreatedVersion;
    private final IndexAnalyzers indexAnalyzers;
    private final NamedAnalyzer indexAnalyzer;
    private final IndexSettings indexSettings;
    private final String indexOptions;
    private final int positionIncrementGap;
    private final FieldType fieldType;
    private final KeywordFieldMapper templateIdMapper;

    private PatternedTextFieldMapper(
        String simpleName,
        FieldType fieldType,
        PatternedTextFieldType mappedFieldType,
        BuilderParams builderParams,
        Builder builder,
        KeywordFieldMapper templateIdMapper
    ) {
        super(simpleName, mappedFieldType, builderParams);
        assert mappedFieldType.getTextSearchInfo().isTokenized();
        assert mappedFieldType.hasDocValues() == false;
        this.fieldType = fieldType;
        this.indexCreatedVersion = builder.indexCreatedVersion;
        this.indexAnalyzers = builder.analyzers.indexAnalyzers;
        this.indexAnalyzer = builder.analyzers.getIndexAnalyzer();
        this.indexSettings = builder.indexSettings;
        this.indexOptions = builder.indexOptions.getValue();
        this.positionIncrementGap = builder.analyzers.positionIncrementGap.getValue();
        this.templateIdMapper = templateIdMapper;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), indexAnalyzer);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), indexCreatedVersion, indexSettings, indexAnalyzers).init(this);
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> mappers = new ArrayList<>();
        Iterator<Mapper> m = super.iterator();
        while (m.hasNext()) {
            mappers.add(m.next());
        }
        mappers.add(templateIdMapper);
        return mappers.iterator();
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        final String value = context.parser().textOrNull();
        if (value == null) {
            return;
        }

        var existingValue = context.doc().getField(fieldType().name());
        if (existingValue != null) {
            throw new IllegalArgumentException("Multiple values are not allowed for field [" + fieldType().name() + "].");
        }

        // Parse template and args
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(value);

        // Add index on original value
        context.doc().add(new Field(fieldType().name(), value, fieldType));

        // Add template doc_values
        context.doc().add(new SortedSetDocValuesField(fieldType().templateFieldName(), new BytesRef(parts.template())));

        // Add template_id doc_values
        context.doc().add(templateIdMapper.buildKeywordField(new BytesRef(parts.templateId())));

        // Add args Info
        String argsInfoEncoded = Arg.encodeInfo(parts.argsInfo());
        context.doc().add(new SortedSetDocValuesField(fieldType().argsInfoFieldName(), new BytesRef(argsInfoEncoded)));

        // Add args doc_values
        if (parts.args().isEmpty() == false) {
            String remainingArgs = Arg.encodeRemainingArgs(parts);
            context.doc().add(new SortedSetDocValuesField(fieldType().argsFieldName(), new BytesRef(remainingArgs)));
        }
    }

    @Override
    protected String contentType() {
        return PatternedTextFieldType.CONTENT_TYPE;
    }

    @Override
    public PatternedTextFieldType fieldType() {
        return (PatternedTextFieldType) super.fieldType();
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport.Native(
            () -> new CompositeSyntheticFieldLoader(
                leafName(),
                fullPath(),
                new PatternedTextSyntheticFieldLoaderLayer(
                    fieldType().name(),
                    fieldType().templateFieldName(),
                    fieldType().argsFieldName(),
                    fieldType().argsInfoFieldName()
                )
            )
        );
    }
}
