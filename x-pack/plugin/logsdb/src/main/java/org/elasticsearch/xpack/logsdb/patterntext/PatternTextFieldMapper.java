/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfColumnBuilder;
import org.elasticsearch.escf.EscfColumnBuilder.CollisionPolicy;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.escf.EscfColumnTransforms;
import org.elasticsearch.escf.LuceneBinaryColumn;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.BatchMappingContext;
import org.elasticsearch.index.mapper.BinaryDocValuesSyntheticFieldLoader;
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
import org.elasticsearch.transport.BytesRefRecycler;
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
        private final IndexVersion indexCreatedVersion;
        private final boolean useBinaryDocValuesForRawText;

        public Builder(String name, MappingParserContext context) {
            this(
                name,
                context.indexVersionCreated(),
                context.getIndexSettings(),
                context.isWithinMultiField(),
                useBinaryDocValuesForRawText(context.getIndexSettings())
            );
        }

        public Builder(
            String name,
            IndexVersion indexCreatedVersion,
            IndexSettings indexSettings,
            boolean isWithinMultiField,
            boolean useBinaryDocValuesForRawText
        ) {
            super(name, indexCreatedVersion, isWithinMultiField);
            this.indexSettings = indexSettings;
            this.analyzer = analyzerParam(name, m -> ((PatternTextFieldMapper) m).analyzer);
            this.disableTemplating = disableTemplatingParameter(indexSettings);
            this.indexCreatedVersion = indexCreatedVersion;
            this.useBinaryDocValuesForRawText = useBinaryDocValuesForRawText;
        }

        private boolean useBinaryDocValuesForArgsColumn() {
            return indexCreatedVersion.onOrAfter(IndexVersions.PATTERN_TEXT_ARGS_IN_BINARY_DOC_VALUES);
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
                isWithinMultiField(),
                useBinaryDocValuesForArgsColumn(),
                useBinaryDocValuesForRawText
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
        public String contentType() {
            return PatternTextFieldType.CONTENT_TYPE;
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
                // Enforce LOW cardinality even if cardinality defaults to HIGH:
            ).indexed(false).docValues(DocValuesParameter.Values.Cardinality.LOW).build(context);
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
    private final FieldType templateIdFieldType;
    private final boolean useBinaryDocValueArgs;
    private final boolean useBinaryDocValuesForRawText;

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
        this.templateIdFieldType = templateIdMapper.luceneFieldType();
        this.useBinaryDocValueArgs = builder.useBinaryDocValuesForArgsColumn();
        this.useBinaryDocValuesForRawText = builder.useBinaryDocValuesForRawText;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), analyzer);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), indexCreatedVersion, indexSettings, fieldType().isWithinMultiField(), useBinaryDocValuesForRawText)
            .init(this);
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
            storePatternAsRawText(context, value);
            return;
        }

        // Parse template and args
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(value);

        // Add template_id doc_values
        context.doc().add(templateIdMapper.buildKeywordField(new BytesRef(parts.templateId())));

        if (parts.useBinaryDocValuesForRawText()) {
            storePatternAsRawText(context, value);
        } else {
            // Add template doc_values
            context.doc().add(new SortedSetDocValuesField(fieldType().templateFieldName(), new BytesRef(parts.template())));

            // Add args Info
            String argsInfoEncoded = Arg.encodeInfo(parts.argsInfo());
            context.doc().add(new SortedSetDocValuesField(fieldType().argsInfoFieldName(), new BytesRef(argsInfoEncoded)));

            // Add args doc_values
            if (parts.args().isEmpty() == false) {
                String remainingArgs = Arg.encodeRemainingArgs(parts);
                if (useBinaryDocValueArgs) {
                    context.doc().add(new BinaryDocValuesField(fieldType().argsFieldName(), new BytesRef(remainingArgs)));
                } else {
                    context.doc().add(new SortedSetDocValuesField(fieldType().argsFieldName(), new BytesRef(remainingArgs)));
                }
            }
        }
    }

    /**
     * Store the value as a raw text field, without analyzing it. This can happen when templating is disabled or when the value is too long
     * to be analyzed.
     *
     * Values may be stored in binary doc values or in stored fields, both of which don't have the same length limitations as regular doc
     * values do.
     */
    private void storePatternAsRawText(DocumentParserContext context, final String value) {
        if (useBinaryDocValuesForRawText) {
            context.doc().add(new BinaryDocValuesField(fieldType().storedNamed(), new BytesRef(value)));
        } else {
            // for bwc, store in stored fields
            context.doc().add(new StoredField(fieldType().storedNamed(), new BytesRef(value)));
        }
    }

    private static boolean useBinaryDocValuesForRawText(IndexSettings indexSettings) {
        return indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.STORE_PATTERN_TEXT_FIELDS_IN_BINARY_DOC_VALUES)
            && indexSettings.useTimeSeriesDocValuesFormat();
    }

    @Override
    public boolean supportsColumnarParse(IndexSettings settings) {
        // Only activate on strict-columnar index modes (COLUMNAR / LOGSDB_COLUMNAR), which
        // guarantee useBinaryDocValuesForRawText == true (via USE_TIME_SERIES_DOC_VALUES_FORMAT).
        // We require it explicitly here rather than implicitly to make the invariant visible.
        return settings.getMode().isStrictColumnar()
            && useBinaryDocValueArgs            // only the binary-doc-values args encoding is handled
            && useBinaryDocValuesForRawText     // always true in columnar mode; required for correctness
            && copyTo().copyToFields().isEmpty()
            && multiFields().iterator().hasNext() == false
            && fieldType().isWithinMultiField() == false;
    }

    /**
     * Maps a batch of documents for this {@code pattern_text} field from the supplied ESCF source
     * column.
     *
     * <p>Up to six columns may be emitted:
     * <ol>
     *   <li>Analyzed value (inverted index) — always, zero-copy when the source is a plain STRING
     *       column.</li>
     *   <li>{@code .template_id} — always (even for length-exceeded values).</li>
     *   <li>{@code .template} — for TEMPLATED values only.</li>
     *   <li>{@code .args_info} — for TEMPLATED values only.</li>
     *   <li>{@code .args} — for TEMPLATED values with at least one arg.</li>
     *   <li>{@code .stored} raw text — for length-exceeded values and when
     *       {@link PatternTextFieldType#disableTemplating()} is {@code true}.</li>
     * </ol>
     *
     * @throws UnsupportedOperationException when a document has more than one value (causes
     *         {@link org.elasticsearch.index.mapper.ShardBatchMapper} to fall back to the row path
     *         which raises the per-doc error with the correct {@code on_failure} behaviour)
     */
    @Override
    public void mapColumnBatch(BatchMappingContext ctx, EscfColumn source) {
        final int docCount = ctx.docCount();
        // retainValues=false: every value is consumed within one loop iteration, before the cursor advances.
        final ObjectTupleCursor<BytesRef> cursor = EscfColumnTransforms.utf8Cursor(source, false);

        // Zero-copy path: when the source is a plain STRING column (no UNION wrapper for nulls)
        // the analyzed column can share the column data directly. A builder is allocated lazily
        // only when the source is UNION/other kind.
        final EscfColumnBuilder analyzedBuilder = source.leafValueKind() != EscfColumnKind.STRING ? newStringBuilder() : null;

        // Never written when templating is disabled, so do not allocate it in that case.
        final EscfColumnBuilder templateIdBuilder = fieldType().disableTemplating() ? null : newStringBuilder();
        // These are allocated when first needed (TEMPLATED path) to avoid waste for
        // disable_templating=true or all-LENGTH_EXCEEDED batches.
        EscfColumnBuilder templateBuilder = null;
        EscfColumnBuilder argsInfoBuilder = null;
        EscfColumnBuilder argsBuilder = null;
        EscfColumnBuilder rawTextBuilder = null;

        final PatternTextUtf8Splitter splitter = new PatternTextUtf8Splitter();
        boolean valuesProduced = false;
        int currentDoc = -1;
        boolean valueSeenThisDoc = false;

        while (true) {
            final int nextDoc = cursor.nextDoc();
            if (nextDoc == DocIdSetIterator.NO_MORE_DOCS) {
                break;
            }
            if (nextDoc != currentDoc) {
                currentDoc = nextDoc;
                valueSeenThisDoc = false;
            }

            final BytesRef v = cursor.value();
            if (v == null) {
                // JSON null: no fields emitted, mirroring the row path's textOrNull() == null check.
                continue;
            }

            if (valueSeenThisDoc) {
                // pattern_text is single-valued; bail so ShardBatchMapper falls back to the
                // row path which raises the correct per-doc error (on_failure=FAIL).
                // TODO: Improve and handle this here.
                throw new UnsupportedOperationException(
                    "mapColumnBatch: pattern_text field [" + fullPath() + "] has more than one value for doc [" + currentDoc + "]"
                );
            }
            valueSeenThisDoc = true;
            valuesProduced = true;

            // Populate the analyzed column builder when we are not on the zero-copy path.
            if (analyzedBuilder != null) {
                analyzedBuilder.setString(currentDoc, v);
            }

            if (fieldType().disableTemplating()) {
                // Templating disabled: emit the analyzed value and the full raw text only.
                rawTextBuilder = lazyBuilder(rawTextBuilder);
                rawTextBuilder.setString(currentDoc, v);
                continue;
            }

            // Run the byte-level split.
            final PatternTextUtf8Splitter.Result result = splitter.split(v);

            templateIdBuilder.setString(currentDoc, splitter.templateId());

            if (result == PatternTextUtf8Splitter.Result.LENGTH_EXCEEDED) {
                // Value exceeds the length limit: store the full original value as raw text.
                rawTextBuilder = lazyBuilder(rawTextBuilder);
                rawTextBuilder.setString(currentDoc, v);
            } else {
                // TEMPLATED: emit template, args_info, and (if present) args.
                templateBuilder = lazyBuilder(templateBuilder);
                templateBuilder.setString(currentDoc, splitter.template());

                argsInfoBuilder = lazyBuilder(argsInfoBuilder);
                argsInfoBuilder.setString(currentDoc, splitter.argsInfo());

                if (splitter.argCount() > 0) {
                    argsBuilder = lazyBuilder(argsBuilder);
                    argsBuilder.setString(currentDoc, splitter.joinedArgs());
                }
            }
        }

        if (valuesProduced == false) {
            return;
        }

        // Emit the analyzed column (zero-copy when source is plain STRING).
        final EscfColumnData analyzedData = analyzedBuilder != null ? analyzedBuilder.finish(docCount) : source.columnData();
        ctx.addColumn(LuceneBinaryColumn.of(analyzedData, fieldType().name(), fieldType));

        if (fieldType().disableTemplating() == false) {
            ctx.addColumn(
                LuceneBinaryColumn.of(templateIdBuilder.finish(docCount), fieldType().templateIdFieldName(), templateIdFieldType)
            );
        }

        if (templateBuilder != null) {
            ctx.addColumn(
                LuceneBinaryColumn.of(templateBuilder.finish(docCount), fieldType().templateFieldName(), SortedSetDocValuesField.TYPE)
            );
        }
        if (argsInfoBuilder != null) {
            ctx.addColumn(
                LuceneBinaryColumn.of(argsInfoBuilder.finish(docCount), fieldType().argsInfoFieldName(), SortedSetDocValuesField.TYPE)
            );
        }
        if (argsBuilder != null) {
            ctx.addColumn(LuceneBinaryColumn.of(argsBuilder.finish(docCount), fieldType().argsFieldName(), BinaryDocValuesField.TYPE));
        }
        if (rawTextBuilder != null) {
            ctx.addColumn(LuceneBinaryColumn.of(rawTextBuilder.finish(docCount), fieldType().storedNamed(), BinaryDocValuesField.TYPE));
        }
    }

    private static EscfColumnBuilder newStringBuilder() {
        EscfColumnBuilder b = new EscfColumnBuilder(CollisionPolicy.MERGE, BytesRefRecycler.NON_RECYCLING_INSTANCE);
        b.lockScalar(EscfColumnKind.STRING);
        return b;
    }

    private static EscfColumnBuilder lazyBuilder(EscfColumnBuilder existing) {
        return existing != null ? existing : newStringBuilder();
    }

    @Override
    protected String contentType() {
        return PatternTextFieldType.CONTENT_TYPE;
    }

    @Override
    public PatternTextFieldType fieldType() {
        return (PatternTextFieldType) super.fieldType();
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport.Native(this::getSyntheticFieldLoader);
    }

    private SourceLoader.SyntheticFieldLoader getSyntheticFieldLoader() {
        if (fieldType().disableTemplating()) {
            if (useBinaryDocValuesForRawText) {
                return new BinaryDocValuesSyntheticFieldLoader(fieldType().storedNamed()) {
                    @Override
                    protected void writeValue(XContentBuilder b, BytesRef value) throws IOException {
                        // pattern text fields are not multi-valued, so there is no special encoding here unlike other fields that use
                        // binary doc values. As a result, we don't need to much and this function remains simple
                        b.field(leafName(), value.utf8ToString());
                    }
                };
            }

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
                leafReader -> PatternTextFallbackDocValues.fromEnabledPatternText(leafReader, fieldType())
            )
        );
    }

    NamedAnalyzer getAnalyzer() {
        return analyzer;
    }
}
