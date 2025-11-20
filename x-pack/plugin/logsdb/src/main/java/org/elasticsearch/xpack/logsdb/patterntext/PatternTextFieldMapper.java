/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.CompositeSyntheticFieldLoader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.StringStoredFieldFieldLoader;
import org.elasticsearch.index.mapper.TextParams;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.xcontent.XContentBuilder;

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
public class PatternTextFieldMapper extends FieldMapper {

    private static final NamedAnalyzer STANDARD_ANALYZER = new NamedAnalyzer("standard", AnalyzerScope.GLOBAL, new StandardAnalyzer());

    /**
     * A setting that indicates that pattern text fields should disable templating, usually because there is
     * no valid enterprise license.
     */
    public static final Setting<Boolean> DISABLE_TEMPLATING_SETTING = Setting.boolSetting(
        "index.mapping.pattern_text.disable_templating",
        false,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex
    );

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

    public static class Builder extends TextFamilyBuilder {

        private final IndexSettings indexSettings;
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<String> indexOptions = patternTextIndexOptions(m -> ((PatternTextFieldMapper) m).indexOptions);
        private final Parameter<NamedAnalyzer> analyzer;
        private final Parameter<Boolean> disableTemplating;

        public Builder(String name, MappingParserContext context) {
            this(name, context.indexVersionCreated(), context.getIndexSettings(), context.isWithinMultiField());
        }

        public Builder(String name, IndexVersion indexCreatedVersion, IndexSettings indexSettings, boolean isWithinMultiField) {
            super(name, indexCreatedVersion, isWithinMultiField);
            this.indexSettings = indexSettings;
            this.analyzer = analyzerParam(name, m -> ((PatternTextFieldMapper) m).analyzer);
            this.disableTemplating = disableTemplatingParameter(indexSettings);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta, indexOptions, analyzer, disableTemplating };
        }

        private PatternTextFieldType buildFieldType(FieldType fieldType, MapperBuilderContext context) {
            NamedAnalyzer analyzer = this.analyzer.get();
            TextSearchInfo tsi = new TextSearchInfo(fieldType, null, analyzer, analyzer);
            return new PatternTextFieldType(
                context.buildFullName(leafName()),
                tsi,
                analyzer,
                disableTemplating.getValue(),
                meta.getValue(),
                context.isSourceSynthetic(),
                isWithinMultiField()
            );
        }

        private static FieldType buildLuceneFieldType(Supplier<String> indexOptionSupplier) {
            var indexOptions = TextParams.toIndexOptions(true, indexOptionSupplier.get());
            return indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS ? Defaults.FIELD_TYPE_POSITIONS : Defaults.FIELD_TYPE_DOCS;
        }

        private static Parameter<String> patternTextIndexOptions(Function<FieldMapper, String> initializer) {
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

        private static Parameter<NamedAnalyzer> analyzerParam(String name, Function<FieldMapper, NamedAnalyzer> initializer) {
            return new Parameter<>("analyzer", false, () -> DelimiterAnalyzer.INSTANCE, (n, c, o) -> {
                String analyzerName = o.toString();
                switch (analyzerName) {
                    case "standard":
                        return STANDARD_ANALYZER;
                    case "delimiter":
                        return DelimiterAnalyzer.INSTANCE;
                    default:
                        throw new IllegalArgumentException(
                            "unsupported analyzer [" + analyzerName + "] for field [" + name + "], supported analyzers are [standard, log]"
                        );
                }
            }, initializer, (b, n, v) -> b.field(n, v.name()), NamedAnalyzer::name);
        }

        /**
         * A parameter that indicates the pattern_text mapper should disable templating, usually
         * because there is no valid enterprise license.
         * <p>
         * The parameter should only be explicitly enabled or left unset. When left unset, it defaults to the value determined from the
         * associated index setting, which is set from the current license status.
         */
        private static Parameter<Boolean> disableTemplatingParameter(IndexSettings indexSettings) {
            boolean forceDisable = DISABLE_TEMPLATING_SETTING.get(indexSettings.getSettings());
            return Parameter.boolParam(
                "disable_templating",
                false,
                m -> ((PatternTextFieldMapper) m).fieldType().disableTemplating(),
                forceDisable
            ).addValidator(value -> {
                if (value == false && forceDisable) {
                    throw new MapperParsingException(
                        "value [false] for mapping parameter [disable_templating] contradicts value [true] for index setting ["
                            + DISABLE_TEMPLATING_SETTING.getKey()
                            + "]"
                    );
                }
            }).setSerializerCheck((includeDefaults, isConfigured, value) -> includeDefaults || isConfigured || value);
        }

        @Override
        public PatternTextFieldMapper build(MapperBuilderContext context) {
            FieldType fieldType = buildLuceneFieldType(indexOptions);
            PatternTextFieldType patternTextFieldType = buildFieldType(fieldType, context);
            BuilderParams builderParams = builderParams(this, context);
            var templateIdMapper = KeywordFieldMapper.Builder.buildWithDocValuesSkipper(
                patternTextFieldType.templateIdFieldName(leafName()),
                indexSettings,
                isWithinMultiField()
            ).indexed(false).build(context);
            return new PatternTextFieldMapper(leafName(), fieldType, patternTextFieldType, builderParams, this, templateIdMapper);
        }
    }

    public static final TypeParser PARSER = new TypeParser(Builder::new);

    private final IndexVersion indexCreatedVersion;
    private final NamedAnalyzer analyzer;
    private final IndexSettings indexSettings;
    private final String indexOptions;
    private final FieldType fieldType;
    private final KeywordFieldMapper templateIdMapper;

    private PatternTextFieldMapper(
        String simpleName,
        FieldType fieldType,
        PatternTextFieldType mappedFieldType,
        BuilderParams builderParams,
        Builder builder,
        KeywordFieldMapper templateIdMapper
    ) {
        super(simpleName, mappedFieldType, builderParams);
        assert mappedFieldType.getTextSearchInfo().isTokenized();
        assert mappedFieldType.hasDocValues() == false;
        this.fieldType = fieldType;
        this.indexCreatedVersion = builder.indexCreatedVersion();
        this.analyzer = builder.analyzer.get();
        this.indexSettings = builder.indexSettings;
        this.indexOptions = builder.indexOptions.getValue();
        this.templateIdMapper = templateIdMapper;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), analyzer);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), indexCreatedVersion, indexSettings, fieldType().isWithinMultiField()).init(this);
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

        // Add index on original value
        context.doc().add(new Field(fieldType().name(), value, fieldType));

        if (fieldType().disableTemplating()) {
            context.doc().add(new StoredField(fieldType().storedNamed(), new BytesRef(value)));
            return;
        }

        // Parse template and args
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(value);

        // Add template_id doc_values
        context.doc().add(templateIdMapper.buildKeywordField(new BytesRef(parts.templateId())));

        if (parts.useStoredField()) {
            context.doc().add(new StoredField(fieldType().storedNamed(), new BytesRef(value)));
        } else {
            // Add template doc_values
            context.doc().add(new SortedSetDocValuesField(fieldType().templateFieldName(), new BytesRef(parts.template())));

            // Add args Info
            String argsInfoEncoded = Arg.encodeInfo(parts.argsInfo());
            context.doc().add(new SortedSetDocValuesField(fieldType().argsInfoFieldName(), new BytesRef(argsInfoEncoded)));

            // Add args doc_values
            if (parts.args().isEmpty() == false) {
                String remainingArgs = Arg.encodeRemainingArgs(parts);
                context.doc().add(new SortedSetDocValuesField(fieldType().argsFieldName(), new BytesRef(remainingArgs)));
            }
        }
    }

    @Override
    protected String contentType() {
        return PatternTextFieldType.CONTENT_TYPE;
    }

    @Override
    public PatternTextFieldType fieldType() {
        return (PatternTextFieldType) super.fieldType();
    }

    @FunctionalInterface
    interface DocValuesSupplier {
        BinaryDocValues get(LeafReader leafReader) throws IOException;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport.Native(this::getSyntheticFieldLoader);
    }

    private SourceLoader.SyntheticFieldLoader getSyntheticFieldLoader() {
        if (fieldType().disableTemplating()) {
            return new StringStoredFieldFieldLoader(fieldType().storedNamed(), fieldType().name(), leafName()) {
                @Override
                protected void write(XContentBuilder b, Object value) throws IOException {
                    b.value(((BytesRef) value).utf8ToString());
                }
            };
        }

        return new CompositeSyntheticFieldLoader(
            leafName(),
            fullPath(),
            new PatternTextSyntheticFieldLoaderLayer(
                fieldType().name(),
                leafReader -> PatternTextCompositeValues.from(leafReader, fieldType())
            )
        );
    }

    NamedAnalyzer getAnalyzer() {
        return analyzer;
    }
}
