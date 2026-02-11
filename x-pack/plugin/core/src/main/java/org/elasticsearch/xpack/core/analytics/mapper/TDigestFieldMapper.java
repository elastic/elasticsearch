/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.analytics.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexHistogramFieldData;
import org.elasticsearch.index.fielddata.LeafHistogramFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.CompositeSyntheticFieldLoader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IgnoreMalformedStoredValues;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.DoublesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.metrics.TDigestExecutionHint;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.CopyingXContentParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentSubParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Field Mapper for pre-aggregated histograms.
 */
public class TDigestFieldMapper extends FieldMapper {

    public static final String CENTROIDS_NAME = "centroids";
    public static final String COUNTS_NAME = "counts";

    public static final String CONTENT_TYPE = "tdigest";

    private static TDigestFieldMapper toType(FieldMapper in) {
        return (TDigestFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {
        private static final double DEFAULT_COMPRESSION = 100d;
        private static final double MAXIMUM_COMPRESSION = 10000d;

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<Explicit<Boolean>> ignoreMalformed;
        private final Parameter<TDigestExecutionHint> digestType;
        private final Parameter<Double> compression;
        /**
         * Parameter that marks this field as a time series metric defining its time series metric type.
         * Only the metric type histogram is supported.
         */
        private final Parameter<TimeSeriesParams.MetricType> metric;

        public Builder(String name, boolean ignoreMalformedByDefault) {
            super(name);
            this.ignoreMalformed = Parameter.explicitBoolParam(
                "ignore_malformed",
                true,
                m -> toType(m).ignoreMalformed,
                ignoreMalformedByDefault
            );
            this.digestType = Parameter.enumParam(
                "digest_type",
                false,
                m -> toType(m).digestType,
                TDigestExecutionHint.DEFAULT,
                TDigestExecutionHint.class
            );
            this.compression = new Parameter<>(
                "compression",
                false,
                () -> DEFAULT_COMPRESSION,
                (n, c1, o) -> XContentMapValues.nodeDoubleValue(o),
                m -> toType(m).compression,
                XContentBuilder::field,
                Objects::toString
            ).addValidator(c -> {
                if (c <= 0 || c > MAXIMUM_COMPRESSION) {
                    throw new IllegalArgumentException(
                        "compression must be a positive integer between 1 and " + MAXIMUM_COMPRESSION + " was [" + c + "]"
                    );
                }
            });
            this.metric = TimeSeriesParams.metricParam(m -> toType(m).metricType, TimeSeriesParams.MetricType.HISTOGRAM);
        }

        public Builder metric(TimeSeriesParams.MetricType metric) {
            this.metric.setValue(metric);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { digestType, compression, ignoreMalformed, meta, metric };
        }

        @Override
        public TDigestFieldMapper build(MapperBuilderContext context) {
            return new TDigestFieldMapper(
                leafName(),
                new TDigestFieldType(context.buildFullName(leafName()), meta.getValue(), this.metric.getValue()),
                builderParams(this, context),
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(n, IGNORE_MALFORMED_SETTING.get(c.getSettings())),
        notInMultiFields(CONTENT_TYPE)
    );

    private final Explicit<Boolean> ignoreMalformed;
    private final boolean ignoreMalformedByDefault;
    private final TDigestExecutionHint digestType;
    private final double compression;
    private final TimeSeriesParams.MetricType metricType;

    public TDigestFieldMapper(String simpleName, MappedFieldType mappedFieldType, BuilderParams builderParams, Builder builder) {
        super(simpleName, mappedFieldType, builderParams);
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.ignoreMalformedByDefault = builder.ignoreMalformed.getDefaultValue().value();
        this.digestType = builder.digestType.getValue();
        this.compression = builder.compression.getValue();
        this.metricType = builder.metric.get();
    }

    @Override
    public boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    public TDigestExecutionHint digestType() {
        return digestType;
    }

    public double compression() {
        return compression;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), ignoreMalformedByDefault).metric(metricType).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    public static class TDigestFieldType extends MappedFieldType {
        private final TimeSeriesParams.MetricType metricType;

        public TDigestFieldType(String name, Map<String, String> meta, TimeSeriesParams.MetricType metricType) {
            super(name, IndexType.docValuesOnly(), false, meta);
            this.metricType = metricType;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public TimeSeriesParams.MetricType getMetricType() {
            return metricType;
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            DoublesBlockLoader minimaLoader = new DoublesBlockLoader(valuesMinSubFieldName(name()), NumericUtils::sortableLongToDouble);
            DoublesBlockLoader maximaLoader = new DoublesBlockLoader(valuesMaxSubFieldName(name()), NumericUtils::sortableLongToDouble);
            DoublesBlockLoader sumsLoader = new DoublesBlockLoader(valuesSumSubFieldName(name()), NumericUtils::sortableLongToDouble);
            LongsBlockLoader valueCountsLoader = new LongsBlockLoader(valuesCountSubFieldName(name()));
            BytesRefsFromBinaryBlockLoader digestLoader = new BytesRefsFromBinaryBlockLoader(name());

            // TODO: We're constantly passing around this set of 5 things. It would be nice to make a container for that.
            return new TDigestBlockLoader(digestLoader, minimaLoader, maximaLoader, sumsLoader, valueCountsLoader);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public boolean isSearchable() {
            return false;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            // TODO - This needs to be changed to a custom values source type
            return (cache, breakerService) -> new IndexHistogramFieldData(name(), null) {

                @Override
                public LeafHistogramFieldData load(LeafReaderContext context) {
                    return new LeafHistogramFieldData() {
                        @Override
                        public HistogramValues getHistogramValues() throws IOException {
                            try {
                                final BinaryDocValues values = DocValues.getBinary(context.reader(), fieldName);
                                final InternalTDigestValue value = new InternalTDigestValue();
                                return new HistogramValues() {

                                    @Override
                                    public boolean advanceExact(int doc) throws IOException {
                                        return values.advanceExact(doc);
                                    }

                                    @Override
                                    public HistogramValue histogram() throws IOException {
                                        try {
                                            value.reset(values.binaryValue());
                                            return value;
                                        } catch (IOException e) {
                                            throw new IOException("Cannot load doc value", e);
                                        }
                                    }
                                };
                            } catch (IOException e) {
                                throw new IOException("Cannot load doc values", e);
                            }
                        }

                        @Override
                        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
                            throw new UnsupportedOperationException("The [" + CONTENT_TYPE + "] field does not " + "support scripts");
                        }

                        @Override
                        public FormattedDocValues getFormattedValues(DocValueFormat format) {
                            try {
                                final BinaryDocValues values = DocValues.getBinary(context.reader(), fieldName);
                                final InternalTDigestValue value = new InternalTDigestValue();
                                return new FormattedDocValues() {
                                    @Override
                                    public boolean advanceExact(int docId) throws IOException {
                                        return values.advanceExact(docId);
                                    }

                                    @Override
                                    public int docValueCount() {
                                        return 1;
                                    }

                                    @Override
                                    public Object nextValue() throws IOException {
                                        value.reset(values.binaryValue());
                                        return value;
                                    }
                                };
                            } catch (IOException e) {
                                throw new UncheckedIOException("Unable to loead histogram doc values", e);
                            }
                        }

                        @Override
                        public SortedBinaryDocValues getBytesValues() {
                            throw new UnsupportedOperationException(
                                "String representation of doc values " + "for [" + CONTENT_TYPE + "] fields is not supported"
                            );
                        }

                        @Override
                        public long ramBytesUsed() {
                            return 0; // Unknown
                        }

                    };
                }

                @Override
                public LeafHistogramFieldData loadDirect(LeafReaderContext context) {
                    return load(context);
                }

                @Override
                public SortField sortField(Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
                    throw new IllegalArgumentException("can't sort on the [" + CONTENT_TYPE + "] field");
                }

                @Override
                public BucketedSort newBucketedSort(
                    BigArrays bigArrays,
                    Object missingValue,
                    MultiValueMode sortMode,
                    Nested nested,
                    SortOrder sortOrder,
                    DocValueFormat format,
                    int bucketSize,
                    BucketedSort.ExtraData extra
                ) {
                    throw new IllegalArgumentException("can't sort on the [" + CONTENT_TYPE + "] field");
                }

                @Override
                public ValuesSourceType getValuesSourceType() {
                    throw new UnsupportedOperationException("The [" + CONTENT_TYPE + "] field does not support aggregations");
                }
            };
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException(
                "[" + CONTENT_TYPE + "] field do not support searching, " + "use dedicated aggregations instead: [" + name() + "]"
            );
        }
    }

    @Override
    protected boolean supportsParsingObject() {
        return true;
    }

    @Override
    public void parse(DocumentParserContext context) throws IOException {
        context.path().add(leafName());

        boolean shouldStoreMalformedDataForSyntheticSource = context.mappingLookup().isSourceSynthetic() && ignoreMalformed();
        XContentParser.Token token;
        XContentSubParser subParser = null;
        XContentBuilder malformedDataForSyntheticSource = null;

        try {
            token = context.parser().currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                context.path().remove();
                return;
            }
            // should be an object
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, context.parser());
            if (shouldStoreMalformedDataForSyntheticSource) {
                var copyingParser = new CopyingXContentParser(context.parser());
                malformedDataForSyntheticSource = copyingParser.getBuilder();
                subParser = new XContentSubParser(copyingParser);
            } else {
                subParser = new XContentSubParser(context.parser());
            }
            subParser.nextToken();
            // TODO: Here we should build a t-digest out of the input, based on the settings on the field
            TDigestParser.ParsedTDigest parsedTDigest = TDigestParser.parse(
                fullPath(),
                subParser,
                DocumentParsingException::new,
                XContentParserUtils::parsingException
            );

            BytesRef docValue = encodeCentroidsAndCounts(parsedTDigest.centroids(), parsedTDigest.counts());
            Field digestField = new BinaryDocValuesField(fullPath(), docValue);

            // Add numeric doc values fields for the summary data
            NumericDocValuesField maxField = null;
            if (Double.isNaN(parsedTDigest.max()) == false) {
                maxField = new NumericDocValuesField(
                    valuesMaxSubFieldName(fullPath()),
                    NumericUtils.doubleToSortableLong(parsedTDigest.max())
                );
            }

            NumericDocValuesField minField = null;
            if (Double.isNaN(parsedTDigest.min()) == false) {
                minField = new NumericDocValuesField(
                    valuesMinSubFieldName(fullPath()),
                    NumericUtils.doubleToSortableLong(parsedTDigest.min())
                );
            }
            NumericDocValuesField countField = new NumericDocValuesField(valuesCountSubFieldName(fullPath()), parsedTDigest.count());
            NumericDocValuesField sumField = null;
            if (Double.isNaN(parsedTDigest.sum()) == false) {
                sumField = new NumericDocValuesField(
                    valuesSumSubFieldName(fullPath()),
                    NumericUtils.doubleToSortableLong(parsedTDigest.sum())
                );
            }
            if (context.doc().getByKey(fieldType().name()) != null) {
                throw new IllegalArgumentException(
                    "Field ["
                        + fullPath()
                        + "] of type ["
                        + typeName()
                        + "] doesn't support indexing multiple values for the same field in the same document"
                );
            }
            context.doc().addWithKey(fieldType().name(), digestField);
            context.doc().add(countField);
            if (sumField != null) {
                context.doc().add(sumField);
            }
            if (maxField != null) {
                context.doc().add(maxField);
            }
            if (minField != null) {
                context.doc().add(minField);
            }

        } catch (Exception ex) {
            if (ignoreMalformed.value() == false) {
                throw new DocumentParsingException(
                    context.parser().getTokenLocation(),
                    "failed to parse field [" + fieldType().name() + "] of type [" + fieldType().typeName() + "]",
                    ex
                );
            }

            if (subParser != null) {
                // close the subParser so we advance to the end of the object
                subParser.close();
            } else if (shouldStoreMalformedDataForSyntheticSource) {
                // We have a malformed value, but it's not an object given that `subParser` is null.
                // So we just remember whatever it is.
                malformedDataForSyntheticSource = XContentBuilder.builder(context.parser().contentType().xContent())
                    .copyCurrentStructure(context.parser());
            }

            if (malformedDataForSyntheticSource != null) {
                context.doc().add(IgnoreMalformedStoredValues.storedField(fullPath(), malformedDataForSyntheticSource));
            }

            context.addIgnoredField(fieldType().name());
        }
        context.path().remove();
    }

    private static BytesRef encodeCentroidsAndCounts(List<Double> centroids, List<Long> counts) throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput();

        for (int i = 0; i < centroids.size(); i++) {
            long count = counts.get(i);
            assert count >= 0;
            // we do not add elements with count == 0
            if (count > 0) {
                streamOutput.writeVLong(count);
                streamOutput.writeDouble(centroids.get(i));
            }
        }

        BytesRef docValue = streamOutput.bytes().toBytesRef();
        return docValue;
    }

    private static String valuesCountSubFieldName(String fullPath) {
        return fullPath + "._values_count";
    }

    private static String valuesSumSubFieldName(String fullPath) {
        return fullPath + "._values_sum";
    }

    private static String valuesMinSubFieldName(String fullPath) {
        return fullPath + "._values_min";
    }

    private static String valuesMaxSubFieldName(String fullPath) {
        return fullPath + "._values_max";
    }

    /** re-usable {@link HistogramValue} implementation */
    static class InternalTDigestValue extends HistogramValue {
        double value;
        long count;
        boolean isExhausted;

        final ByteArrayStreamInput streamInput;

        InternalTDigestValue() {
            streamInput = new ByteArrayStreamInput();
        }

        /** reset the value for the histogram */
        void reset(BytesRef bytesRef) throws IOException {
            streamInput.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            isExhausted = false;
            value = 0;
            count = 0;
        }

        @Override
        public boolean next() throws IOException {
            if (streamInput.available() > 0) {
                count = streamInput.readVLong();
                value = streamInput.readDouble();
                return true;
            }
            isExhausted = true;
            return false;
        }

        @Override
        public double value() {
            if (isExhausted) {
                throw new IllegalArgumentException("histogram already exhausted");
            }
            return value;
        }

        @Override
        public long count() {
            if (isExhausted) {
                throw new IllegalArgumentException("histogram already exhausted");
            }
            return count;
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport.Native(
            () -> new CompositeSyntheticFieldLoader(
                leafName(),
                fullPath(),
                new TDigestSyntheticFieldLoader(),
                new CompositeSyntheticFieldLoader.MalformedValuesLayer(fullPath())
            )
        );
    }

    private class TDigestSyntheticFieldLoader implements CompositeSyntheticFieldLoader.DocValuesLayer {
        private final InternalTDigestValue value = new InternalTDigestValue();
        private BytesRef binaryValue;
        private double min;
        private double max;
        private double sum;

        @Override
        public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
            BinaryDocValues docValues = leafReader.getBinaryDocValues(fieldType().name());
            NumericDocValues minValues = leafReader.getNumericDocValues(valuesMinSubFieldName(fullPath()));
            NumericDocValues maxValues = leafReader.getNumericDocValues(valuesMaxSubFieldName(fullPath()));
            NumericDocValues sumValues = leafReader.getNumericDocValues(valuesSumSubFieldName(fullPath()));
            if (docValues == null) {
                // No values in this leaf
                binaryValue = null;
                return null;
            }
            return docId -> {
                if (docValues.advanceExact(docId)) {
                    // we assume the summary sub-
                    if (minValues != null && minValues.advanceExact(docId)) {
                        min = NumericUtils.sortableLongToDouble(minValues.longValue());
                    } else {
                        min = Double.NaN;
                    }

                    if (maxValues != null && maxValues.advanceExact(docId)) {
                        max = NumericUtils.sortableLongToDouble(maxValues.longValue());
                    } else {
                        max = Double.NaN;
                    }

                    if (sumValues != null && sumValues.advanceExact(docId)) {
                        sum = NumericUtils.sortableLongToDouble(sumValues.longValue());
                    } else {
                        sum = Double.NaN;
                    }

                    binaryValue = docValues.binaryValue();
                    return true;
                }
                binaryValue = null;
                return false;
            };
        }

        @Override
        public boolean hasValue() {
            return binaryValue != null;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            if (binaryValue == null) {
                return;
            }
            value.reset(binaryValue);
            b.startObject();

            // TODO: Load the summary values out of the sub-fields, if they exist
            if (Double.isNaN(min) == false) {
                b.field("min", min);
            }
            if (Double.isNaN(max) == false) {
                b.field("max", max);
            }
            if (Double.isNaN(sum) == false) {
                b.field("sum", sum);
            }

            b.startArray(CENTROIDS_NAME);
            while (value.next()) {
                b.value(value.value());
            }
            b.endArray();

            value.reset(binaryValue);
            b.startArray(COUNTS_NAME);
            while (value.next()) {
                b.value(value.count());
            }
            b.endArray();

            b.endObject();
        }

        @Override
        public String fieldName() {
            return fullPath();
        }

        @Override
        public long valueCount() {
            return binaryValue != null ? 1 : 0;
        }
    }
}
