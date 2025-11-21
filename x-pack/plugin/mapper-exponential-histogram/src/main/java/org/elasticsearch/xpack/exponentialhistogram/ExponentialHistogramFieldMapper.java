/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.CompressedExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.exponentialhistogram.ZeroBucket;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.CompositeSyntheticFieldLoader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IgnoreMalformedStoredValues;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.DoublesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.CopyingXContentParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentSubParser;
import org.elasticsearch.xpack.analytics.mapper.ExponentialHistogramParser;
import org.elasticsearch.xpack.analytics.mapper.HistogramParser;
import org.elasticsearch.xpack.analytics.mapper.IndexWithCount;
import org.elasticsearch.xpack.analytics.mapper.ParsedHistogramConverter;
import org.elasticsearch.xpack.exponentialhistogram.fielddata.ExponentialHistogramValuesReader;
import org.elasticsearch.xpack.exponentialhistogram.fielddata.IndexExponentialHistogramFieldData;
import org.elasticsearch.xpack.exponentialhistogram.fielddata.LeafExponentialHistogramFieldData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * {@link FieldMapper} for the exponential_histogram field type. The mapped data represents {@link ExponentialHistogram}s.
 *
 * <p>Example index mapping with an exponential_histogram field:</p>
 * <pre><code>{
 *   "mappings": {
 *     "properties": {
 *       "my_histo": {
 *         "type": "exponential_histogram"
 *       }
 *     }
 *   }
 * }
 * </code></pre>
 *
 * <p>For a example histogram data for a full histogram value,
 * see {@link ExponentialHistogramParser}.
 */
public class ExponentialHistogramFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "exponential_histogram";

    // use the same default as numbers
    private static final Setting<Boolean> COERCE_SETTING = NumberFieldMapper.COERCE_SETTING;

    private static ExponentialHistogramFieldMapper toType(FieldMapper in) {
        return (ExponentialHistogramFieldMapper) in;
    }

    /**
     * We store the zero-threshold as a separate doc value with this lucene field name.
     * We don't expect the zero threshold to be accessed independently of the other histogram data,
     * but this approach should save a lot of storage space: The zero threshold is a value configured by users.
     * This means it is expected to not or only rarely change over time and is very often the same across time series.
     * Storing it as a separate doc value allows us to compress the zero threshold across documents.
     *
     * @param fullPath the full path of the mapped field
     * @return the name for the lucene field
     */
    static String zeroThresholdSubFieldName(String fullPath) {
        return fullPath + "._zero_threshold";
    }

    /**
     * We store the sum of the counts over all buckets (including the zero bucket) in this value.
     * This is done to allow access to it at query time without having to load the entire histogram.
     *
     * As a side effect, this is used to avoid storing the count for the zero bucket with the histogram data,
     * as it can be computed from this value minus the sum of the counts of all other buckets.
     *
     * @param fullPath the full path of the mapped field
     * @return the name for the lucene field
     */
    static String valuesCountSubFieldName(String fullPath) {
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

    static class Builder extends FieldMapper.Builder {

        private final FieldMapper.Parameter<Map<String, String>> meta = FieldMapper.Parameter.metaParam();
        private final FieldMapper.Parameter<Explicit<Boolean>> ignoreMalformed;
        private final Parameter<Explicit<Boolean>> coerce;
        /**
         * Parameter that marks this field as a time series metric defining its time series metric type.
         * Only the metric type histogram is supported.
         */
        private final Parameter<TimeSeriesParams.MetricType> metric;

        Builder(String name, boolean ignoreMalformedByDefault, boolean coerceByDefault) {
            super(name);
            this.ignoreMalformed = FieldMapper.Parameter.explicitBoolParam(
                "ignore_malformed",
                true,
                m -> toType(m).ignoreMalformed,
                ignoreMalformedByDefault
            );
            this.coerce = Parameter.explicitBoolParam("coerce", true, m -> toType(m).coerce, coerceByDefault);
            this.metric = TimeSeriesParams.metricParam(m -> toType(m).metricType, TimeSeriesParams.MetricType.HISTOGRAM);
        }

        public Builder metric(TimeSeriesParams.MetricType metric) {
            this.metric.setValue(metric);
            return this;
        }

        @Override
        protected FieldMapper.Parameter<?>[] getParameters() {
            return new FieldMapper.Parameter<?>[] { ignoreMalformed, coerce, meta, metric };
        }

        @Override
        public ExponentialHistogramFieldMapper build(MapperBuilderContext context) {
            return new ExponentialHistogramFieldMapper(
                leafName(),
                new ExponentialHistogramFieldType(context.buildFullName(leafName()), meta.getValue(), metric.getValue()),
                builderParams(this, context),
                this
            );
        }
    }

    public static final FieldMapper.TypeParser PARSER = new FieldMapper.TypeParser(
        (n, c) -> new Builder(n, IGNORE_MALFORMED_SETTING.get(c.getSettings()), COERCE_SETTING.get(c.getSettings())),
        notInMultiFields(CONTENT_TYPE)
    );

    private final Explicit<Boolean> ignoreMalformed;
    private final boolean ignoreMalformedByDefault;

    private final Explicit<Boolean> coerce;
    private final boolean coerceByDefault;
    private final TimeSeriesParams.MetricType metricType;

    ExponentialHistogramFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        FieldMapper.BuilderParams builderParams,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, builderParams);
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.ignoreMalformedByDefault = builder.ignoreMalformed.getDefaultValue().value();
        this.coerce = builder.coerce.getValue();
        this.coerceByDefault = builder.coerce.getDefaultValue().value();
        this.metricType = builder.metric.getValue();
    }

    @Override
    public boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    boolean coerce() {
        return coerce.value();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), ignoreMalformedByDefault, coerceByDefault).metric(metricType).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    public static final class ExponentialHistogramFieldType extends MappedFieldType {

        private final TimeSeriesParams.MetricType metricType;

        // Visible for testing
        public ExponentialHistogramFieldType(String name, Map<String, String> meta, TimeSeriesParams.MetricType metricType) {
            super(name, IndexType.docValuesOnly(), false, meta);
            this.metricType = metricType;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
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
        public boolean isAggregatable() {
            return true;
        }

        @Override
        public TimeSeriesParams.MetricType getMetricType() {
            return metricType;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            return (cache, breakerService) -> new IndexExponentialHistogramFieldData(name()) {
                @Override
                public LeafExponentialHistogramFieldData load(LeafReaderContext context) {
                    return new LeafExponentialHistogramFieldData() {
                        @Override
                        public ExponentialHistogramValuesReader getHistogramValues() throws IOException {
                            return new DocValuesReader(context.reader(), fieldName);
                        }

                        @Override
                        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
                            throw new UnsupportedOperationException("The [" + CONTENT_TYPE + "] field does not " + "support scripts");
                        }

                        @Override
                        public SortedBinaryDocValues getBytesValues() {
                            throw new UnsupportedOperationException(
                                "String representation of doc values " + "for [" + CONTENT_TYPE + "] fields is not supported"
                            );
                        }

                        @Override
                        public FormattedDocValues getFormattedValues(DocValueFormat format) {
                            return createFormattedDocValues(context.reader(), fieldName);
                        }

                        @Override
                        public long ramBytesUsed() {
                            return 0; // No dynamic allocations
                        }
                    };
                }

                @Override
                public LeafExponentialHistogramFieldData loadDirect(LeafReaderContext context) throws Exception {
                    return load(context);
                }

                @Override
                public SortField sortField(
                    Object missingValue,
                    MultiValueMode sortMode,
                    XFieldComparatorSource.Nested nested,
                    boolean reverse
                ) {
                    throw new IllegalArgumentException("can't sort on the [" + CONTENT_TYPE + "] field");
                }

                @Override
                public BucketedSort newBucketedSort(
                    BigArrays bigArrays,
                    Object missingValue,
                    MultiValueMode sortMode,
                    XFieldComparatorSource.Nested nested,
                    SortOrder sortOrder,
                    DocValueFormat format,
                    int bucketSize,
                    BucketedSort.ExtraData extra
                ) {
                    throw new IllegalArgumentException("can't sort on the [" + CONTENT_TYPE + "] field");
                }

            };
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException(
                "[" + CONTENT_TYPE + "] field do not support searching, " + "use dedicated aggregations instead: [" + name() + "]"
            );
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            DoublesBlockLoader minimaLoader = new DoublesBlockLoader(valuesMinSubFieldName(name()), NumericUtils::sortableLongToDouble);
            DoublesBlockLoader maximaLoader = new DoublesBlockLoader(valuesMaxSubFieldName(name()), NumericUtils::sortableLongToDouble);
            DoublesBlockLoader sumsLoader = new DoublesBlockLoader(valuesSumSubFieldName(name()), NumericUtils::sortableLongToDouble);
            LongsBlockLoader valueCountsLoader = new LongsBlockLoader(valuesCountSubFieldName(name()));
            DoublesBlockLoader zeroThresholdsLoader = new DoublesBlockLoader(
                zeroThresholdSubFieldName(name()),
                NumericUtils::sortableLongToDouble
            );
            BytesRefsFromBinaryBlockLoader bytesLoader = new BytesRefsFromBinaryBlockLoader(name());

            return new BlockDocValuesReader.DocValuesBlockLoader() {
                @Override
                public Builder builder(BlockFactory factory, int expectedCount) {
                    return factory.exponentialHistogramBlockBuilder(expectedCount);
                }

                @Override
                public AllReader reader(LeafReaderContext context) throws IOException {
                    AllReader bytesReader = bytesLoader.reader(context);
                    BlockLoader.AllReader minimaReader = minimaLoader.reader(context);
                    BlockLoader.AllReader maximaReader = maximaLoader.reader(context);
                    AllReader sumsReader = sumsLoader.reader(context);
                    AllReader valueCountsReader = valueCountsLoader.reader(context);
                    AllReader zeroThresholdsReader = zeroThresholdsLoader.reader(context);

                    return new AllReader() {
                        @Override
                        public boolean canReuse(int startingDocID) {
                            return minimaReader.canReuse(startingDocID)
                                && maximaReader.canReuse(startingDocID)
                                && sumsReader.canReuse(startingDocID)
                                && valueCountsReader.canReuse(startingDocID)
                                && zeroThresholdsReader.canReuse(startingDocID)
                                && bytesReader.canReuse(startingDocID);
                        }

                        @Override
                        public String toString() {
                            return "BlockDocValuesReader.ExponentialHistogram";
                        }

                        @Override
                        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
                            Block minima = null;
                            Block maxima = null;
                            Block sums = null;
                            Block valueCounts = null;
                            Block zeroThresholds = null;
                            Block encodedBytes = null;
                            Block result;
                            boolean success = false;
                            try {
                                minima = minimaReader.read(factory, docs, offset, nullsFiltered);
                                maxima = maximaReader.read(factory, docs, offset, nullsFiltered);
                                sums = sumsReader.read(factory, docs, offset, nullsFiltered);
                                valueCounts = valueCountsReader.read(factory, docs, offset, nullsFiltered);
                                zeroThresholds = zeroThresholdsReader.read(factory, docs, offset, nullsFiltered);
                                encodedBytes = bytesReader.read(factory, docs, offset, nullsFiltered);
                                result = factory.buildExponentialHistogramBlockDirect(
                                    minima,
                                    maxima,
                                    sums,
                                    valueCounts,
                                    zeroThresholds,
                                    encodedBytes
                                );
                                success = true;
                            } finally {
                                if (success == false) {
                                    Releasables.close(minima, maxima, sums, valueCounts, zeroThresholds, encodedBytes);
                                }
                            }
                            return result;
                        }

                        @Override
                        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
                            ExponentialHistogramBuilder histogramBuilder = (ExponentialHistogramBuilder) builder;
                            minimaReader.read(docId, storedFields, histogramBuilder.minima());
                            maximaReader.read(docId, storedFields, histogramBuilder.maxima());
                            sumsReader.read(docId, storedFields, histogramBuilder.sums());
                            valueCountsReader.read(docId, storedFields, histogramBuilder.valueCounts());
                            zeroThresholdsReader.read(docId, storedFields, histogramBuilder.zeroThresholds());
                            bytesReader.read(docId, storedFields, histogramBuilder.encodedHistograms());
                        }
                    };
                }
            };
        }
    }

    // Visible for testing
    static FormattedDocValues createFormattedDocValues(LeafReader reader, String fieldName) {
        return new FormattedDocValues() {

            boolean hasNext = false;
            ExponentialHistogramValuesReader delegate;

            private ExponentialHistogramValuesReader lazyDelegate() throws IOException {
                if (delegate == null) {
                    delegate = new DocValuesReader(reader, fieldName);
                }
                return delegate;
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                hasNext = lazyDelegate().advanceExact(docId);
                return hasNext;
            }

            @Override
            public int docValueCount() throws IOException {
                return 1; // no multivalue support, so always 1
            }

            @Override
            public Object nextValue() throws IOException {
                if (hasNext == false) {
                    throw new IllegalStateException("No value available, make sure to call advanceExact() first");
                }
                hasNext = false;
                return lazyDelegate().histogramValue();
            }

        };
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

            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, context.parser());
            if (shouldStoreMalformedDataForSyntheticSource) {
                var copyingParser = new CopyingXContentParser(context.parser());
                malformedDataForSyntheticSource = copyingParser.getBuilder();
                subParser = new XContentSubParser(copyingParser);
            } else {
                subParser = new XContentSubParser(context.parser());
            }
            subParser.nextToken();
            ExponentialHistogramParser.ParsedExponentialHistogram parsedHistogram;
            if (coerce()
                && subParser.currentToken() == XContentParser.Token.FIELD_NAME
                && HistogramParser.isHistogramSubFieldName(subParser.currentName())) {
                HistogramParser.ParsedHistogram parsedTDigest = HistogramParser.parse(fullPath(), subParser);
                parsedHistogram = ParsedHistogramConverter.tDigestToExponential(parsedTDigest);
            } else {
                parsedHistogram = ExponentialHistogramParser.parse(fullPath(), subParser);
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

            long totalValueCount;
            try {
                totalValueCount = getTotalValueCount(parsedHistogram);
            } catch (ArithmeticException e) {
                throw new IllegalArgumentException(
                    "Field [" + fullPath() + "] has a total value count exceeding the allowed maximum value of " + Long.MAX_VALUE
                );
            }
            double sum = validateOrEstimateSum(parsedHistogram, subParser);
            double min = validateOrEstimateMin(parsedHistogram, subParser);
            double max = validateOrEstimateMax(parsedHistogram, subParser);

            HistogramDocValueFields docValues = buildDocValueFields(
                fullPath(),
                parsedHistogram.scale(),
                parsedHistogram.negativeBuckets(),
                parsedHistogram.positiveBuckets(),
                parsedHistogram.zeroThreshold(),
                totalValueCount,
                sum,
                min,
                max
            );
            docValues.addToDoc(context.doc());

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

    // Visible for testing, to construct realistic doc values in tests
    public static HistogramDocValueFields buildDocValueFields(
        String fieldName,
        int scale,
        List<IndexWithCount> negativeBuckets,
        List<IndexWithCount> positiveBuckets,
        double zeroThreshold,
        long totalValueCount,
        double sum,
        double min,
        double max
    ) throws IOException {
        BytesStreamOutput histogramBytesOutput = new BytesStreamOutput();
        CompressedExponentialHistogram.writeHistogramBytes(
            histogramBytesOutput,
            scale,
            IndexWithCount.asBuckets(scale, negativeBuckets).iterator(),
            IndexWithCount.asBuckets(scale, positiveBuckets).iterator()
        );
        BytesRef histoBytes = histogramBytesOutput.bytes().toBytesRef();

        BinaryDocValuesField histoField = new BinaryDocValuesField(fieldName, histoBytes);
        long thresholdAsLong = NumericUtils.doubleToSortableLong(zeroThreshold);
        NumericDocValuesField zeroThresholdField = new NumericDocValuesField(zeroThresholdSubFieldName(fieldName), thresholdAsLong);
        NumericDocValuesField valuesCountField = new NumericDocValuesField(valuesCountSubFieldName(fieldName), totalValueCount);
        // for empty histograms, we store null as sum so that SUM() / COUNT() in ESQL yields NULL without warnings
        NumericDocValuesField sumField = null;
        if (totalValueCount > 0) {
            sumField = new NumericDocValuesField(valuesSumSubFieldName(fieldName), NumericUtils.doubleToSortableLong(sum));
        } else {
            // empty histogram must have a sum of 0.0
            assert sum == 0.0;
        }
        NumericDocValuesField minField = null;
        if (Double.isNaN(min) == false) {
            minField = new NumericDocValuesField(valuesMinSubFieldName(fieldName), NumericUtils.doubleToSortableLong(min));
        }
        NumericDocValuesField maxField = null;
        if (Double.isNaN(max) == false) {
            maxField = new NumericDocValuesField(valuesMaxSubFieldName(fieldName), NumericUtils.doubleToSortableLong(max));
        }
        HistogramDocValueFields docValues = new HistogramDocValueFields(
            histoField,
            zeroThresholdField,
            valuesCountField,
            sumField,
            minField,
            maxField
        );
        return docValues;
    }

    // Visible for testing
    public record HistogramDocValueFields(
        BinaryDocValuesField histo,
        NumericDocValuesField zeroThreshold,
        NumericDocValuesField valuesCount,
        @Nullable NumericDocValuesField sumField,
        @Nullable NumericDocValuesField minField,
        @Nullable NumericDocValuesField maxField
    ) {

        public void addToDoc(LuceneDocument doc) {
            doc.addWithKey(histo.name(), histo);
            doc.add(zeroThreshold);
            doc.add(valuesCount);
            if (sumField != null) {
                doc.add(sumField);
            }
            if (minField != null) {
                doc.add(minField);
            }
            if (maxField != null) {
                doc.add(maxField);
            }
        }

        public List<IndexableField> fieldsAsList() {
            List<IndexableField> fields = new ArrayList<>();
            fields.add(histo);
            fields.add(zeroThreshold);
            fields.add(valuesCount);
            if (sumField != null) {
                fields.add(sumField);
            }
            if (minField != null) {
                fields.add(minField);
            }
            if (maxField != null) {
                fields.add(maxField);
            }
            return fields;
        }
    }

    private static boolean isEmpty(ExponentialHistogramParser.ParsedExponentialHistogram histogram) {
        return histogram.positiveBuckets().isEmpty() && histogram.negativeBuckets().isEmpty() && histogram.zeroCount() == 0;
    }

    private double validateOrEstimateSum(ExponentialHistogramParser.ParsedExponentialHistogram histogram, XContentSubParser subParser) {
        if (histogram.sum() == null) {
            return ExponentialHistogramUtils.estimateSum(
                IndexWithCount.asBuckets(histogram.scale(), histogram.negativeBuckets()).iterator(),
                IndexWithCount.asBuckets(histogram.scale(), histogram.positiveBuckets()).iterator()
            );
        }
        if (isEmpty(histogram) && histogram.sum() != 0.0) {
            throw new DocumentParsingException(
                subParser.getTokenLocation(),
                "error parsing field [" + fullPath() + "], sum field must be zero if the histogram is empty, but got " + histogram.sum()
            );
        }
        return histogram.sum();
    }

    private double validateOrEstimateMin(ExponentialHistogramParser.ParsedExponentialHistogram histogram, XContentSubParser subParser) {
        if (histogram.min() == null) {
            OptionalDouble estimatedMin = ExponentialHistogramUtils.estimateMin(
                ZeroBucket.create(histogram.zeroThreshold(), histogram.zeroCount()),
                IndexWithCount.asBuckets(histogram.scale(), histogram.negativeBuckets()),
                IndexWithCount.asBuckets(histogram.scale(), histogram.positiveBuckets())
            );
            return estimatedMin.isPresent() ? estimatedMin.getAsDouble() : Double.NaN;
        }
        if (isEmpty(histogram)) {
            throw new DocumentParsingException(
                subParser.getTokenLocation(),
                "error parsing field [" + fullPath() + "], min field must be null if the histogram is empty, but got " + histogram.min()
            );
        }
        return histogram.min();
    }

    private double validateOrEstimateMax(ExponentialHistogramParser.ParsedExponentialHistogram histogram, XContentSubParser subParser) {
        if (histogram.max() == null) {
            OptionalDouble estimatedMax = ExponentialHistogramUtils.estimateMax(
                ZeroBucket.create(histogram.zeroThreshold(), histogram.zeroCount()),
                IndexWithCount.asBuckets(histogram.scale(), histogram.negativeBuckets()),
                IndexWithCount.asBuckets(histogram.scale(), histogram.positiveBuckets())
            );
            return estimatedMax.isPresent() ? estimatedMax.getAsDouble() : Double.NaN;
        }
        if (isEmpty(histogram)) {
            throw new DocumentParsingException(
                subParser.getTokenLocation(),
                "error parsing field [" + fullPath() + "], max field must be null if the histogram is empty, but got " + histogram.max()
            );
        }
        return histogram.max();
    }

    private static long getTotalValueCount(ExponentialHistogramParser.ParsedExponentialHistogram histogram) {
        long totalValueCount = histogram.zeroCount();
        for (IndexWithCount bucket : histogram.positiveBuckets()) {
            totalValueCount = Math.addExact(totalValueCount, bucket.count());
        }
        for (IndexWithCount bucket : histogram.negativeBuckets()) {
            totalValueCount = Math.addExact(totalValueCount, bucket.count());
        }
        return totalValueCount;
    }

    @Override
    protected FieldMapper.SyntheticSourceSupport syntheticSourceSupport() {
        return new FieldMapper.SyntheticSourceSupport.Native(
            () -> new CompositeSyntheticFieldLoader(
                leafName(),
                fullPath(),
                new ExponentialHistogramSyntheticFieldLoader(),
                new CompositeSyntheticFieldLoader.MalformedValuesLayer(fullPath())
            )
        );
    }

    private static class DocValuesReader implements ExponentialHistogramValuesReader {

        private final BinaryDocValues histoDocValues;
        private final NumericDocValues zeroThresholds;
        private final NumericDocValues valueCounts;
        private final NumericDocValues valueSums;
        private final NumericDocValues valueMinima;
        private final NumericDocValues valueMaxima;

        private int currentDocId = -1;
        private final CompressedExponentialHistogram tempHistogram = new CompressedExponentialHistogram();

        DocValuesReader(LeafReader leafReader, String fullPath) throws IOException {
            histoDocValues = leafReader.getBinaryDocValues(fullPath);
            zeroThresholds = leafReader.getNumericDocValues(zeroThresholdSubFieldName(fullPath));
            valueCounts = leafReader.getNumericDocValues(valuesCountSubFieldName(fullPath));
            valueSums = leafReader.getNumericDocValues(valuesSumSubFieldName(fullPath));
            valueMinima = leafReader.getNumericDocValues(valuesMinSubFieldName(fullPath));
            valueMaxima = leafReader.getNumericDocValues(valuesMaxSubFieldName(fullPath));
        }

        boolean hasAnyValues() {
            return valueCounts != null;
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            boolean isPresent = valueCounts != null && valueCounts.advanceExact(docId);
            currentDocId = isPresent ? docId : -1;
            return isPresent;
        }

        @Override
        public ExponentialHistogram histogramValue() throws IOException {
            if (currentDocId == -1) {
                throw new IllegalStateException("No histogram present for current document");
            }
            boolean histoPresent = histoDocValues.advanceExact(currentDocId);
            boolean zeroThresholdPresent = zeroThresholds.advanceExact(currentDocId);
            assert zeroThresholdPresent && histoPresent;

            BytesRef encodedHistogram = histoDocValues.binaryValue();
            double zeroThreshold = NumericUtils.sortableLongToDouble(zeroThresholds.longValue());
            long valueCount = valueCounts.longValue();
            double valueSum;
            if (valueCount > 0) {
                boolean valueSumsPresent = valueSums.advanceExact(currentDocId);
                assert valueSumsPresent;
                valueSum = NumericUtils.sortableLongToDouble(valueSums.longValue());
            } else {
                valueSum = 0.0; // empty histogram has sum of 0.0, but we store null in the doc values
            }
            double valueMin;
            if (valueMinima != null && valueMinima.advanceExact(currentDocId)) {
                valueMin = NumericUtils.sortableLongToDouble(valueMinima.longValue());
            } else {
                valueMin = Double.NaN;
            }
            double valueMax;
            if (valueMaxima != null && valueMaxima.advanceExact(currentDocId)) {
                valueMax = NumericUtils.sortableLongToDouble(valueMaxima.longValue());
            } else {
                valueMax = Double.NaN;
            }
            tempHistogram.reset(zeroThreshold, valueCount, valueSum, valueMin, valueMax, encodedHistogram);
            return tempHistogram;
        }

        @Override
        public long valuesCountValue() throws IOException {
            return valueCounts.longValue();
        }

        @Override
        public double sumValue() throws IOException {
            if (currentDocId == -1) {
                throw new IllegalStateException("No histogram present for current document");
            }
            if (valueSums == null || valueSums.advanceExact(currentDocId) == false) {
                // empty histogram, must have sum of 0.0
                return 0.0;
            }
            return NumericUtils.sortableLongToDouble(valueSums.longValue());
        }
    }

    private class ExponentialHistogramSyntheticFieldLoader implements CompositeSyntheticFieldLoader.DocValuesLayer {

        @Nullable
        private ExponentialHistogram currentHistogram;

        @Override
        public SourceLoader.SyntheticFieldLoader.DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf)
            throws IOException {
            DocValuesReader histogramReader = new DocValuesReader(leafReader, fullPath());
            if (histogramReader.hasAnyValues() == false) {
                return null;
            }
            return docId -> {
                if (histogramReader.advanceExact(docId)) {
                    currentHistogram = histogramReader.histogramValue();
                    return true;
                }
                currentHistogram = null;
                return false;
            };
        }

        @Override
        public boolean hasValue() {
            return currentHistogram != null;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            if (currentHistogram == null) {
                return;
            }
            ExponentialHistogramXContent.serialize(b, currentHistogram);
        }

        @Override
        public String fieldName() {
            return fullPath();
        }

        @Override
        public long valueCount() {
            return currentHistogram != null ? 1 : 0;
        }
    };
}
