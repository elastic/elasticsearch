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
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.TextParams;
import org.elasticsearch.index.mapper.TextSearchInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.IndexSettings.USE_DOC_VALUES_SKIPPER;

/**
 * A {@link FieldMapper} that assigns every document the same value.
 */
public class PatternedTextFieldMapper extends FieldMapper {

    public static final FeatureFlag PATTERNED_TEXT_MAPPER = new FeatureFlag("patterned_text");

    public static class Defaults {
        public static final FieldType FIELD_TYPE;

        static {
            final FieldType ft = new FieldType();
            ft.setTokenized(true);
            ft.setStored(false);
            ft.setStoreTermVectors(false);
            ft.setOmitNorms(true);
            ft.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE = freezeAndDeduplicateFieldType(ft);
        }
    }

    public static class Builder extends FieldMapper.Builder {

        private final IndexVersion indexCreatedVersion;
        private final IndexSettings indexSettings;
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final TextParams.Analyzers analyzers;
        private final boolean enableDocValuesSkipper;

        public Builder(String name, MappingParserContext context) {
            this(
                name,
                context.indexVersionCreated(),
                context.getIndexSettings(),
                context.getIndexAnalyzers(),
                USE_DOC_VALUES_SKIPPER.get(context.getSettings())
            );
        }

        public Builder(
            String name,
            IndexVersion indexCreatedVersion,
            IndexSettings indexSettings,
            IndexAnalyzers indexAnalyzers,
            boolean enableDocValuesSkipper
        ) {
            super(name);
            this.indexCreatedVersion = indexCreatedVersion;
            this.indexSettings = indexSettings;
            this.analyzers = new TextParams.Analyzers(
                indexAnalyzers,
                m -> ((PatternedTextFieldMapper) m).indexAnalyzer,
                m -> ((PatternedTextFieldMapper) m).positionIncrementGap,
                indexCreatedVersion
            );
            this.enableDocValuesSkipper = enableDocValuesSkipper;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta };
        }

        private PatternedTextFieldType buildFieldType(MapperBuilderContext context) {
            NamedAnalyzer searchAnalyzer = analyzers.getSearchAnalyzer();
            NamedAnalyzer searchQuoteAnalyzer = analyzers.getSearchQuoteAnalyzer();
            NamedAnalyzer indexAnalyzer = analyzers.getIndexAnalyzer();
            TextSearchInfo tsi = new TextSearchInfo(Defaults.FIELD_TYPE, null, searchAnalyzer, searchQuoteAnalyzer);
            return new PatternedTextFieldType(
                context.buildFullName(leafName()),
                tsi,
                indexAnalyzer,
                context.isSourceSynthetic(),
                meta.getValue()
            );
        }

        @Override
        public PatternedTextFieldMapper build(MapperBuilderContext context) {
            PatternedTextFieldType patternedTextFieldType = buildFieldType(context);
            BuilderParams builderParams = builderParams(this, context);
            var templateIdMapper = KeywordFieldMapper.Builder.buildWithDocValuesSkipper(
                patternedTextFieldType.templateIdFieldName(),
                indexSettings.getMode(),
                indexCreatedVersion,
                enableDocValuesSkipper
            ).build(context);
            return new PatternedTextFieldMapper(leafName(), patternedTextFieldType, builderParams, this, templateIdMapper);
        }
    }

    public static final TypeParser PARSER = new TypeParser(Builder::new);

    private final IndexVersion indexCreatedVersion;
    private final IndexAnalyzers indexAnalyzers;
    private final IndexSettings indexSettings;
    private final NamedAnalyzer indexAnalyzer;
    private final boolean enableDocValuesSkipper;
    private final int positionIncrementGap;
    private final FieldType fieldType;
    private final KeywordFieldMapper templateIdMapper;

    private PatternedTextFieldMapper(
        String simpleName,
        PatternedTextFieldType mappedFieldPatternedTextFieldType,
        BuilderParams builderParams,
        Builder builder,
        KeywordFieldMapper templateIdMapper
    ) {
        super(simpleName, mappedFieldPatternedTextFieldType, builderParams);
        assert mappedFieldPatternedTextFieldType.getTextSearchInfo().isTokenized();
        assert mappedFieldPatternedTextFieldType.hasDocValues() == false;
        this.fieldType = Defaults.FIELD_TYPE;
        this.indexCreatedVersion = builder.indexCreatedVersion;
        this.indexAnalyzers = builder.analyzers.indexAnalyzers;
        this.indexAnalyzer = builder.analyzers.getIndexAnalyzer();
        this.indexSettings = builder.indexSettings;
        this.enableDocValuesSkipper = builder.enableDocValuesSkipper;
        this.positionIncrementGap = builder.analyzers.positionIncrementGap.getValue();
        this.templateIdMapper = templateIdMapper;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), indexAnalyzer);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), indexCreatedVersion, indexSettings, indexAnalyzers, enableDocValuesSkipper).init(this);
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

        // Parse template and args.
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(value);

        // Add index on original value
        context.doc().add(new Field(fieldType().name(), value, fieldType));

        // Add template doc_values
        context.doc().add(new SortedSetDocValuesField(fieldType().templateFieldName(), new BytesRef(parts.template())));

        // Add template_id doc_values
        context.doc().add(templateIdMapper.buildKeywordField(new BytesRef(parts.templateId())));

        // Add args doc_values
        if (parts.args().isEmpty() == false) {
            String remainingArgs = PatternedTextValueProcessor.encodeRemainingArgs(parts);
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
                new PatternedTextSyntheticFieldLoaderLayer(fieldType().name(), fieldType().templateFieldName(), fieldType().argsFieldName())
            )
        );
    }
}
