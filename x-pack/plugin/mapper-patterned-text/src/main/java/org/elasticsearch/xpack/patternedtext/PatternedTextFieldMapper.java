/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.CompositeSyntheticFieldLoader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.TextParams;
import org.elasticsearch.index.mapper.TextSearchInfo;

import java.io.IOException;
import java.util.Map;

/**
 * A {@link FieldMapper} that assigns every document the same value.
 */
public class PatternedTextFieldMapper extends FieldMapper {

    static final int OPTIMIZED_ARG_COUNT = 0;

    public static class Defaults {
        public static final FieldType FIELD_TYPE;
        public static final FieldType TEMPLATE_TYPE;

        static {
            final FieldType ft = new FieldType();
            ft.setTokenized(true);
            ft.setStored(false);
            ft.setStoreTermVectors(false);
            ft.setOmitNorms(true);
            ft.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE = freezeAndDeduplicateFieldType(ft);
        }

        static {
            final FieldType ft = new FieldType();
            ft.setTokenized(true);
            ft.setStored(false);
            ft.setStoreTermVectors(false);
            ft.setOmitNorms(true);
            ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            TEMPLATE_TYPE = freezeAndDeduplicateFieldType(ft);
        }

    }

    public static class Builder extends FieldMapper.Builder {

        private final IndexVersion indexCreatedVersion;

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final TextParams.Analyzers analyzers;

        public Builder(String name, IndexVersion indexCreatedVersion, IndexAnalyzers indexAnalyzers) {
            super(name);
            this.indexCreatedVersion = indexCreatedVersion;
            this.analyzers = new TextParams.Analyzers(
                indexAnalyzers,
                m -> ((PatternedTextFieldMapper) m).indexAnalyzer,
                m -> ((PatternedTextFieldMapper) m).positionIncrementGap,
                indexCreatedVersion
            );
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
            return new PatternedTextFieldMapper(leafName(), buildFieldType(context), builderParams(this, context), this);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, c.indexVersionCreated(), c.getIndexAnalyzers()));

    private final IndexVersion indexCreatedVersion;
    private final IndexAnalyzers indexAnalyzers;
    private final NamedAnalyzer indexAnalyzer;
    private final int positionIncrementGap;
    private final FieldType fieldType;
    private final FieldType templateFieldType;

    private PatternedTextFieldMapper(
        String simpleName,
        PatternedTextFieldType mappedFieldPatternedTextFieldType,
        BuilderParams builderParams,
        Builder builder
    ) {
        super(simpleName, mappedFieldPatternedTextFieldType, builderParams);
        assert mappedFieldPatternedTextFieldType.getTextSearchInfo().isTokenized();
        assert mappedFieldPatternedTextFieldType.hasDocValues();
        this.fieldType = Defaults.FIELD_TYPE;
        this.templateFieldType = Defaults.TEMPLATE_TYPE;
        this.indexCreatedVersion = builder.indexCreatedVersion;
        this.indexAnalyzers = builder.analyzers.indexAnalyzers;
        this.indexAnalyzer = builder.analyzers.getIndexAnalyzer();
        this.positionIncrementGap = builder.analyzers.positionIncrementGap.getValue();
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), indexAnalyzer, fieldType().templateFieldName(), indexAnalyzer);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), indexCreatedVersion, indexAnalyzers).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        final String value = context.parser().textOrNull();
        if (value == null) {
            return;
        }

        // Parse template and args.
        PatternedTextValueProcessor.Parts parts = PatternedTextValueProcessor.split(value);

        // Add template and args index.
        context.doc().add(new Field(fieldType().name(), parts.indexed(), fieldType));

        // Add template docvalues and index.
        context.doc().add(new SortedSetDocValuesField(fieldType().templateFieldName(), new BytesRef(parts.template())));
        // todo: calling templateStripped() right after split() seems like a waste, would be better to do it in the split() method
        context.doc().add(new Field(fieldType().templateFieldName(), parts.templateStripped(), templateFieldType));

        // Add timestamp docvalues.
        if (parts.timestamp() != null) {
            context.doc().add(new SortedNumericDocValuesField(fieldType().timestampFieldName(), parts.timestamp()));
        }

        // Add args docvalues.
        for (int i = 0; i < Integer.min(parts.args().size(), OPTIMIZED_ARG_COUNT); i++) {
            String argFieldname = fieldType().argsFieldName() + "." + i;
            context.doc().add(new SortedSetDocValuesField(argFieldname, new BytesRef(parts.args().get(i))));
        }
        if (parts.args().size() > OPTIMIZED_ARG_COUNT) {
            String remainingArgs = PatternedTextValueProcessor.mergeRemainingArgs(parts, OPTIMIZED_ARG_COUNT);
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
                    fieldType().templateFieldName(),
                    fieldType().timestampFieldName(),
                    fieldType().argsFieldName()
                )
            )
        );
    }
}
