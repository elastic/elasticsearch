/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.CompositeSyntheticFieldLoader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IgnoreMalformedStoredValues;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.IgnoredSourceFormat;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SortedSetDocValuesSyntheticFieldLoaderLayer;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.index.mapper.FieldMapper.Parameter.useTimeSeriesDocValuesSkippers;

/**
 * A keyword-like field that only accepts "delta" and "cumulative" as values.
 * In addition, it doesn't allow multiple values for the same field in the same document.
 */
public class MetricTemporalityFieldMapper extends FieldMapper {

    public static final FeatureFlag FEATURE_FLAG = new FeatureFlag("metric_temporality_field_type");

    public static final String CONTENT_TYPE = "metric_temporality";
    private static final String DELTA = "delta";
    private static final String CUMULATIVE = "cumulative";
    private static final BytesRef DELTA_BYTES = new BytesRef(DELTA);
    private static final BytesRef CUMULATIVE_BYTES = new BytesRef(CUMULATIVE);

    private static MetricTemporalityFieldMapper toType(FieldMapper in) {
        return (MetricTemporalityFieldMapper) in;
    }

    static class Builder extends FieldMapper.DimensionBuilder {
        private final Parameter<Boolean> indexed;
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<Explicit<Boolean>> ignoreMalformed;
        private final IndexVersion indexCreatedVersion;
        private final Parameter<Boolean> dimension = TimeSeriesParams.dimensionParam(m -> toType(m).fieldType().isDimension(), () -> true);
        private final IndexSettings indexSettings;

        Builder(String name, boolean ignoreMalformedByDefault, IndexVersion indexCreatedVersion, IndexSettings indexSettings) {
            super(name);
            this.indexCreatedVersion = indexCreatedVersion;
            this.indexSettings = indexSettings;
            this.ignoreMalformed = Parameter.explicitBoolParam(
                "ignore_malformed",
                true,
                m -> toType(m).ignoreMalformed,
                ignoreMalformedByDefault
            );
            this.indexed = Parameter.indexParam(m -> toType(m).indexed, this.indexSettings, dimension);
        }

        Builder dimension(boolean value) {
            dimension.setValue(value);
            return this;
        }

        Builder indexed(boolean value) {
            indexed.setValue(value);
            return this;
        }

        private boolean useDocValuesSkippers() {
            if (indexed.get()) {
                return false;
            }
            if (indexSettings.useDocValuesSkipper() && indexCreatedVersion.onOrAfter(IndexVersions.STANDARD_INDEXES_USE_SKIPPERS)) {
                return true;
            }
            return useTimeSeriesDocValuesSkippers(indexSettings, dimension.get());
        }

        private FieldType buildFieldType() {
            FieldType ft = new FieldType();
            ft.setTokenized(false);
            ft.setOmitNorms(true);
            ft.setIndexOptions(indexed.get() ? IndexOptions.DOCS : IndexOptions.NONE);
            ft.setDocValuesType(DocValuesType.SORTED_SET);
            if (useDocValuesSkippers()) {
                ft.setDocValuesSkipIndexType(DocValuesSkipIndexType.RANGE);
            }
            return freezeAndDeduplicateFieldType(ft);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta, indexed, ignoreMalformed, dimension };
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }

        @Override
        public MetricTemporalityFieldMapper build(MapperBuilderContext context) {
            if (inheritDimensionParameterFromParentObject(context)) {
                dimension(true);
            }
            if (dimension.getValue() == false) {
                throw new IllegalArgumentException("Field type [" + CONTENT_TYPE + "] requires [" + dimension.name + "] to be [true]");
            }
            FieldType fieldType = buildFieldType();
            TextSearchInfo textSearchInfo = indexed.get()
                ? new TextSearchInfo(fieldType, null, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER)
                : TextSearchInfo.NONE;
            return new MetricTemporalityFieldMapper(
                leafName(),
                new MetricTemporalityFieldType(
                    context.buildFullName(leafName()),
                    IndexType.terms(fieldType),
                    textSearchInfo,
                    meta.getValue()
                ),
                fieldType,
                builderParams(this, context),
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(
            n,
            IGNORE_MALFORMED_SETTING.get(c.getSettings()),
            c.getIndexSettings().getIndexVersionCreated(),
            c.getIndexSettings()
        ),
        notInMultiFields(CONTENT_TYPE)
    );

    static final class MetricTemporalityFieldType extends StringFieldType {

        MetricTemporalityFieldType(String name, IndexType indexType, TextSearchInfo textSearchInfo, Map<String, String> meta) {
            super(name, indexType, false, textSearchInfo, meta);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isDimension() {
            return true;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            return sourceValueFetcher(
                context.isSourceEnabled() ? context.sourcePath(name()) : Collections.emptySet(),
                context.getIndexSettings().getIgnoredSourceFormat()
            );
        }

        private ValueFetcher sourceValueFetcher(Set<String> sourcePaths, IgnoredSourceFormat ignoredSourceFormat) {
            return new SourceValueFetcher(sourcePaths, null, ignoredSourceFormat) {
                @Override
                protected String parseSourceValue(Object value) {
                    return normalizeTemporality(value.toString());
                }
            };
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(
                name(),
                CoreValuesSourceType.KEYWORD,
                (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n)
            );
        }
    }

    private final Explicit<Boolean> ignoreMalformed;
    private final boolean ignoreMalformedByDefault;
    private final IndexVersion indexCreatedVersion;
    private final IndexSettings indexSettings;
    private final FieldType fieldType;
    private final boolean indexed;

    private MetricTemporalityFieldMapper(
        String simpleName,
        MetricTemporalityFieldType mappedFieldType,
        FieldType fieldType,
        BuilderParams builderParams,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, builderParams);
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.ignoreMalformedByDefault = builder.ignoreMalformed.getDefaultValue().value();
        this.indexCreatedVersion = builder.indexCreatedVersion;
        this.indexSettings = builder.indexSettings;
        this.fieldType = fieldType;
        this.indexed = builder.indexed.getValue();
    }

    @Override
    public MetricTemporalityFieldType fieldType() {
        return (MetricTemporalityFieldType) super.fieldType();
    }

    @Override
    public boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public Map<String, org.elasticsearch.index.analysis.NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), Lucene.KEYWORD_ANALYZER);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), ignoreMalformedByDefault, indexCreatedVersion, indexSettings).dimension(fieldType().isDimension())
            .indexed(indexed)
            .init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return;
        }
        if (parser.currentToken().isValue() == false) {
            throw new DocumentParsingException(parser.getTokenLocation(), "Expected a value, but got [" + parser.currentToken() + "]");
        }
        if (context.doc().getField(fieldType().name()) != null) {
            throw new IllegalArgumentException(
                "Field ["
                    + fullPath()
                    + "] of type ["
                    + typeName()
                    + "] doesn't support indexing multiple values for the same field in the same document"
            );
        }
        try {
            ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.currentToken(), context.parser());
            BytesRef normalizedValue = normalizeTemporalityAsBytes(parser.text());
            context.doc().add(new KeywordFieldMapper.KeywordField(fieldType().name(), normalizedValue, fieldType));

            assert fieldType().isDimension();
            context.getRoutingFields().addString(fieldType().name(), normalizedValue);
        } catch (Exception e) {
            if (ignoreMalformed()) {
                if (context.mappingLookup().isSourceSynthetic()) {
                    IgnoreMalformedStoredValues.storeMalformedValueForSyntheticSource(context, fullPath(), parser);
                }
                context.addIgnoredField(fieldType().name());
            } else {
                throw e;
            }
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport.Native(
            () -> new CompositeSyntheticFieldLoader(leafName(), fullPath(), new SortedSetDocValuesSyntheticFieldLoaderLayer(fullPath()) {
                @Override
                protected BytesRef convert(BytesRef value) {
                    return value;
                }

                @Override
                protected BytesRef preserve(BytesRef value) {
                    return BytesRef.deepCopyOf(value);
                }
            }, CompositeSyntheticFieldLoader.malformedValuesLayer(fullPath(), indexCreatedVersion))
        );
    }

    private static BytesRef normalizeTemporalityAsBytes(String value) {
        if (DELTA.equalsIgnoreCase(value)) {
            return DELTA_BYTES;
        }
        if (CUMULATIVE.equalsIgnoreCase(value)) {
            return CUMULATIVE_BYTES;
        }
        throw new IllegalArgumentException(
            "Unknown value [" + value + "] for field [" + CONTENT_TYPE + "] - accepted values are [" + DELTA + ", " + CUMULATIVE + "]"
        );
    }

    private static String normalizeTemporality(String value) {
        return normalizeTemporalityAsBytes(value) == DELTA_BYTES ? DELTA : CUMULATIVE;
    }
}
