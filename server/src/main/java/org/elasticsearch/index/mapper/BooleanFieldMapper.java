/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.SourceValueFetcherSortedBooleanIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.blockloader.docvalues.BooleansBlockLoader;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.BooleanFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.field.BooleanDocValuesField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.FieldValues;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.index.mapper.FieldArrayContext.getOffsetsFieldName;
import static org.elasticsearch.index.mapper.FieldMapper.Parameter.useTimeSeriesDocValuesSkippers;

/**
 * A field mapper for boolean fields.
 */
public class BooleanFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "boolean";

    public static class Values {
        public static final BytesRef TRUE = new BytesRef("T");
        public static final BytesRef FALSE = new BytesRef("F");
    }

    private static BooleanFieldMapper toType(FieldMapper in) {
        return (BooleanFieldMapper) in;
    }

    public static final class Builder extends FieldMapper.DimensionBuilder {

        private final Parameter<Boolean> docValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final Parameter<Boolean> indexed;
        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).stored, false);
        private final Parameter<Explicit<Boolean>> ignoreMalformed;
        private final Parameter<Boolean> nullValue = new Parameter<>(
            "null_value",
            false,
            () -> null,
            (n, c, o) -> o == null ? null : XContentMapValues.nodeBooleanValue(o),
            m -> toType(m).nullValue,
            XContentBuilder::field,
            Objects::toString
        ).acceptsNull();

        private final Parameter<Script> script = Parameter.scriptParam(m -> toType(m).script);
        private final Parameter<OnScriptError> onScriptErrorParam = Parameter.onScriptErrorParam(
            m -> toType(m).builderParams.onScriptError(),
            script
        );

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final ScriptCompiler scriptCompiler;

        private final IndexSettings indexSettings;

        private final Parameter<Boolean> dimension;

        public Builder(String name, ScriptCompiler scriptCompiler, IndexSettings indexSettings) {
            super(name);
            this.scriptCompiler = Objects.requireNonNull(scriptCompiler);
            this.indexSettings = Objects.requireNonNull(indexSettings);
            this.ignoreMalformed = Parameter.explicitBoolParam(
                "ignore_malformed",
                true,
                m -> toType(m).ignoreMalformed,
                IGNORE_MALFORMED_SETTING.get(indexSettings.getSettings())
            );
            this.script.precludesParameters(ignoreMalformed, nullValue);
            this.dimension = TimeSeriesParams.dimensionParam(m -> toType(m).fieldType().isDimension(), docValues::get);
            this.indexed = Parameter.indexParam(m -> toType(m).indexed, indexSettings, dimension);
            addScriptValidation(script, indexed, docValues);
        }

        public Builder dimension(boolean dimension) {
            this.dimension.setValue(dimension);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] {
                meta,
                docValues,
                indexed,
                nullValue,
                stored,
                script,
                onScriptErrorParam,
                ignoreMalformed,
                dimension };
        }

        private IndexType indexType() {
            if (indexed.get() && indexSettings.getIndexVersionCreated().isLegacyIndexVersion() == false) {
                return IndexType.terms(true, docValues.getValue());
            }
            if (docValues.get() == false) {
                return IndexType.NONE;
            }
            return useTimeSeriesDocValuesSkippers(indexSettings, dimension.get()) ? IndexType.skippers() : IndexType.docValuesOnly();
        }

        @Override
        public BooleanFieldMapper build(MapperBuilderContext context) {
            if (inheritDimensionParameterFromParentObject(context)) {
                dimension(true);
            }
            MappedFieldType ft = new BooleanFieldType(
                context.buildFullName(leafName()),
                indexType(),
                stored.getValue(),
                nullValue.getValue(),
                scriptValues(),
                meta.getValue(),
                dimension.getValue(),
                context.isSourceSynthetic()
            );
            hasScript = script.get() != null;
            onScriptError = onScriptErrorParam.getValue();
            String offsetsFieldName = getOffsetsFieldName(
                context,
                indexSettings.sourceKeepMode(),
                docValues.getValue(),
                stored.getValue(),
                this,
                indexSettings.getIndexVersionCreated(),
                IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_BOOLEAN
            );
            return new BooleanFieldMapper(
                leafName(),
                ft,
                builderParams(this, context),
                context.isSourceSynthetic(),
                this,
                offsetsFieldName
            );
        }

        private FieldValues<Boolean> scriptValues() {
            if (script.get() == null) {
                return null;
            }
            BooleanFieldScript.Factory scriptFactory = scriptCompiler.compile(script.get(), BooleanFieldScript.CONTEXT);
            return scriptFactory == null
                ? null
                : (lookup, ctx, doc, consumer) -> scriptFactory.newFactory(leafName(), script.get().getParams(), lookup, OnScriptError.FAIL)
                    .newInstance(ctx)
                    .runForDoc(doc, consumer);
        }
    }

    public static final TypeParser PARSER = createTypeParserWithLegacySupport(
        (n, c) -> new Builder(n, c.scriptCompiler(), c.getIndexSettings())
    );

    public static final class BooleanFieldType extends TermBasedFieldType {

        private final Boolean nullValue;
        private final FieldValues<Boolean> scriptValues;
        private final boolean isDimension;
        private final boolean isSyntheticSource;

        public BooleanFieldType(
            String name,
            IndexType indexType,
            boolean isStored,
            Boolean nullValue,
            FieldValues<Boolean> scriptValues,
            Map<String, String> meta,
            boolean isDimension,
            boolean isSyntheticSource
        ) {
            super(name, indexType, isStored, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            this.nullValue = nullValue;
            this.scriptValues = scriptValues;
            this.isDimension = isDimension;
            this.isSyntheticSource = isSyntheticSource;
        }

        public BooleanFieldType(String name) {
            this(name, IndexType.terms(true, true));
        }

        public BooleanFieldType(String name, IndexType indexType) {
            this(name, indexType, true, false, null, Collections.emptyMap(), false, false);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isSearchable() {
            return indexType.hasTerms() || hasDocValues();
        }

        @Override
        public boolean isDimension() {
            return isDimension;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            if (this.scriptValues != null) {
                return FieldValues.valueFetcher(this.scriptValues, context);
            }
            return sourceValueFetcher(
                context.isSourceEnabled() ? context.sourcePath(name()) : Collections.emptySet(),
                context.getIndexSettings()
            );
        }

        private SourceValueFetcher sourceValueFetcher(Set<String> sourcePaths, IndexSettings indexSettings) {
            return new SourceValueFetcher(sourcePaths, nullValue, indexSettings.getIgnoredSourceFormat()) {
                @Override
                protected Boolean parseSourceValue(Object value) {
                    if (value instanceof Boolean) {
                        return (Boolean) value;
                    } else {
                        String textValue = value.toString();
                        return parseBoolean(textValue);
                    }
                }
            };
        }

        private boolean parseBoolean(String text) {
            return Booleans.parseBoolean(text.toCharArray(), 0, text.length(), false);
        }

        @Override
        public BytesRef indexedValueForSearch(Object value) {
            if (value == null) {
                return Values.FALSE;
            }
            if (value instanceof Boolean) {
                return ((Boolean) value) ? Values.TRUE : Values.FALSE;
            }
            String sValue;
            if (value instanceof BytesRef) {
                sValue = ((BytesRef) value).utf8ToString();
            } else {
                sValue = value.toString();
            }
            return switch (sValue) {
                case "true" -> Values.TRUE;
                case "false" -> Values.FALSE;
                default -> throw new IllegalArgumentException("Can't parse boolean value [" + sValue + "], expected [true] or [false]");
            };
        }

        private long docValueForSearch(Object value) {
            BytesRef ref = indexedValueForSearch(value);
            if (Values.TRUE.equals(ref)) {
                return 1;
            } else {
                return 0;
            }
        }

        @Override
        public Boolean valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            return switch (value.toString()) {
                case "F" -> false;
                case "T" -> true;
                default -> throw new IllegalArgumentException("Expected [T] or [F] but got [" + value + "]");
            };
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            if (hasDocValues()) {
                return new BooleansBlockLoader(name());
            }

            // Multi fields don't have fallback synthetic source.
            if (isSyntheticSource && blContext.parentField(name()) == null) {
                return new FallbackSyntheticSourceBlockLoader(
                    fallbackSyntheticSourceBlockLoaderReader(),
                    name(),
                    IgnoredSourceFieldMapper.ignoredSourceFormat(blContext.indexSettings().getIndexVersionCreated())
                ) {
                    @Override
                    public Builder builder(BlockFactory factory, int expectedCount) {
                        return factory.booleans(expectedCount);
                    }
                };
            }

            var fetcher = sourceValueFetcher(blContext.sourcePaths(name()), blContext.indexSettings());
            BlockSourceReader.LeafIteratorLookup lookup = indexType.hasTerms() || isStored()
                ? BlockSourceReader.lookupFromFieldNames(blContext.fieldNames(), name())
                : BlockSourceReader.lookupMatchingAll();
            return new BlockSourceReader.BooleansBlockLoader(fetcher, lookup);
        }

        private FallbackSyntheticSourceBlockLoader.Reader<?> fallbackSyntheticSourceBlockLoaderReader() {
            return new FallbackSyntheticSourceBlockLoader.SingleValueReader<Boolean>(nullValue) {
                @Override
                public void convertValue(Object value, List<Boolean> accumulator) {
                    try {
                        if (value instanceof Boolean b) {
                            accumulator.add(b);
                        } else {
                            String stringValue = value.toString();
                            // Matches logic in parser invoked by `parseCreateField`
                            accumulator.add(parseBoolean(stringValue));
                        }
                    } catch (Exception e) {
                        // Malformed value, skip it
                    }
                }

                @Override
                protected void parseNonNullValue(XContentParser parser, List<Boolean> accumulator) throws IOException {
                    // Aligned with implementation of `parseCreateField(XContentParser)`
                    try {
                        var value = parser.booleanValue();
                        accumulator.add(value);
                    } catch (Exception e) {
                        // Malformed value, skip it
                    }
                }

                @Override
                public void writeToBlock(List<Boolean> values, BlockLoader.Builder blockBuilder) {
                    var booleanBuilder = (BlockLoader.BooleanBuilder) blockBuilder;

                    for (var value : values) {
                        booleanBuilder.appendBoolean(value);
                    }
                }
            };
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            FielddataOperation operation = fieldDataContext.fielddataOperation();

            if (operation == FielddataOperation.SEARCH) {
                failIfNoDocValues();
            }

            if ((operation == FielddataOperation.SEARCH || operation == FielddataOperation.SCRIPT) && hasDocValues()) {
                return new SortedNumericIndexFieldData.Builder(name(), NumericType.BOOLEAN, BooleanDocValuesField::new, indexType);
            }

            if (operation == FielddataOperation.SCRIPT) {
                SearchLookup searchLookup = fieldDataContext.lookupSupplier().get();
                Set<String> sourcePaths = fieldDataContext.sourcePathsLookup().apply(name());

                return new SourceValueFetcherSortedBooleanIndexFieldData.Builder(
                    name(),
                    CoreValuesSourceType.BOOLEAN,
                    sourceValueFetcher(sourcePaths, fieldDataContext.indexSettings()),
                    searchLookup,
                    BooleanDocValuesField::new
                );
            }

            throw new IllegalStateException("unknown field data type [" + operation.name() + "]");
        }

        @Override
        public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
            checkNoFormat(format);
            checkNoTimeZone(timeZone);
            return DocValueFormat.BOOLEAN;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (indexType.hasTerms()) {
                return super.termQuery(value, context);
            } else {
                return SortedNumericDocValuesField.newSlowExactQuery(name(), docValueForSearch(value));
            }
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (indexType.hasTerms()) {
                return super.termsQuery(values, context);
            } else {
                Set<?> dedupe = new HashSet<>(values);
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                for (Object value : dedupe) {
                    builder.add(termQuery(value, context), BooleanClause.Occur.SHOULD);
                }
                return new ConstantScoreQuery(builder.build());
            }
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            SearchExecutionContext context
        ) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (indexType.hasTerms()) {
                return new TermRangeQuery(
                    name(),
                    lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                    upperTerm == null ? null : indexedValueForSearch(upperTerm),
                    includeLower,
                    includeUpper
                );
            } else {
                long l = 0;
                long u = 1;
                if (lowerTerm != null) {
                    l = docValueForSearch(lowerTerm);
                    if (includeLower == false) {
                        l = Math.max(1, l + 1);
                    }
                }
                if (upperTerm != null) {
                    u = docValueForSearch(upperTerm);
                    if (includeUpper == false) {
                        l = Math.min(0, l - 1);
                    }
                }
                if (l > u) {
                    return new MatchNoDocsQuery();
                }
                return SortedNumericDocValuesField.newSlowRangeQuery(name(), l, u);
            }
        }
    }

    private final Boolean nullValue;
    private final boolean indexed;
    private final boolean hasDocValues;
    private final boolean stored;
    private final Script script;
    private final FieldValues<Boolean> scriptValues;
    private final ScriptCompiler scriptCompiler;
    private final Explicit<Boolean> ignoreMalformed;
    private final IndexSettings indexSettings;
    private final boolean storeMalformedFields;

    private final String offsetsFieldName;

    protected BooleanFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        BuilderParams builderParams,
        boolean storeMalformedFields,
        Builder builder,
        String offsetsFieldName
    ) {
        super(simpleName, mappedFieldType, builderParams);
        this.nullValue = builder.nullValue.getValue();
        this.stored = builder.stored.getValue();
        this.indexed = builder.indexed.getValue();
        this.hasDocValues = builder.docValues.getValue();
        this.script = builder.script.get();
        this.scriptValues = builder.scriptValues();
        this.scriptCompiler = builder.scriptCompiler;
        this.indexSettings = builder.indexSettings;
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.storeMalformedFields = storeMalformedFields;
        this.offsetsFieldName = offsetsFieldName;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), Lucene.KEYWORD_ANALYZER);
    }

    @Override
    public BooleanFieldType fieldType() {
        return (BooleanFieldType) super.fieldType();
    }

    @Override
    public String getOffsetFieldName() {
        return offsetsFieldName;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        if (indexed == false && stored == false && hasDocValues == false) {
            return;
        }

        Boolean value = null;
        XContentParser.Token token = context.parser().currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            if (nullValue != null) {
                value = nullValue;
            }
        } else {
            try {
                value = context.parser().booleanValue();
            } catch (IllegalArgumentException e) {
                if (ignoreMalformed.value() && context.parser().currentToken().isValue()) {
                    context.addIgnoredField(mappedFieldType.name());
                    if (storeMalformedFields) {
                        // Save a copy of the field so synthetic source can load it
                        context.doc().add(IgnoreMalformedStoredValues.storedField(fullPath(), context.parser()));
                    }
                    return;
                } else {
                    throw e;
                }
            }
        }
        indexValue(context, value);
        if (offsetsFieldName != null && context.isImmediateParentAnArray() && context.canAddIgnoredField()) {
            if (value != null) {
                context.getOffSetContext().recordOffset(offsetsFieldName, value);
            } else {
                context.getOffSetContext().recordNull(offsetsFieldName);
            }
        }
    }

    private void indexValue(DocumentParserContext context, Boolean value) {
        if (value == null) {
            return;
        }

        if (fieldType().isDimension()) {
            context.getRoutingFields().addBoolean(fieldType().name(), value);
        }
        if (indexed) {
            context.doc().add(new StringField(fieldType().name(), value ? Values.TRUE : Values.FALSE, Field.Store.NO));
        }
        if (stored) {
            context.doc().add(new StoredField(fieldType().name(), value ? "T" : "F"));
        }
        if (hasDocValues) {
            if (fieldType().indexType.hasDocValuesSkipper()) {
                context.doc().add(SortedNumericDocValuesField.indexedField(fieldType().name(), value ? 1 : 0));
            } else {
                context.doc().add(new SortedNumericDocValuesField(fieldType().name(), value ? 1 : 0));
            }
        } else {
            context.addToFieldNames(fieldType().name());
        }
    }

    @Override
    protected void indexScriptValues(
        SearchLookup searchLookup,
        LeafReaderContext readerContext,
        int doc,
        DocumentParserContext documentParserContext
    ) {
        this.scriptValues.valuesForDoc(searchLookup, readerContext, doc, value -> indexValue(documentParserContext, value));
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), scriptCompiler, indexSettings).dimension(fieldType().isDimension()).init(this);
    }

    @Override
    public void doValidate(MappingLookup lookup) {
        if (fieldType().isDimension() && null != lookup.nestedLookup().getNestedParent(fullPath())) {
            throw new IllegalArgumentException(
                TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM + " can't be configured in nested field [" + fullPath() + "]"
            );
        }
    }

    @Override
    public boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    private SourceLoader.SyntheticFieldLoader docValuesSyntheticFieldLoader() {
        if (offsetsFieldName != null) {
            var layers = new ArrayList<CompositeSyntheticFieldLoader.Layer>(2);
            layers.add(
                new SortedNumericWithOffsetsDocValuesSyntheticFieldLoaderLayer(
                    fullPath(),
                    offsetsFieldName,
                    (b, value) -> b.value(value == 1)
                )
            );
            if (ignoreMalformed.value()) {
                layers.add(new CompositeSyntheticFieldLoader.MalformedValuesLayer(fullPath()));
            }
            return new CompositeSyntheticFieldLoader(leafName(), fullPath(), layers);
        } else {
            return new SortedNumericDocValuesSyntheticFieldLoader(fullPath(), leafName(), ignoreMalformed.value()) {
                @Override
                protected void writeValue(XContentBuilder b, long value) throws IOException {
                    b.value(value == 1);
                }
            };
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        if (hasDocValues) {
            return new SyntheticSourceSupport.Native(this::docValuesSyntheticFieldLoader);
        }

        return super.syntheticSourceSupport();
    }
}
