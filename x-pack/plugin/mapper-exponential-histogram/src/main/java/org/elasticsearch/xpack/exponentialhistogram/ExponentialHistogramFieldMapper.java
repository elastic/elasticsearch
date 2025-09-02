/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.exponentialhistogram.ZeroBucket;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.CompositeSyntheticFieldLoader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IgnoreMalformedStoredValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.CopyingXContentParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentSubParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;

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
 * <p>Example histogram data for a full histogram value:</p>
 * <pre><code>{
 *   "my_histo": {
 *     "scale": 12,
 *     "sum": 1234,
 *     "min": -123.456,
 *     "max": 456.456,
 *     "zero": {
 *       "threshold": 0.123456,
 *       "count": 42
 *     },
 *     "positive": {
 *       "indices": [-1000000, -10, 25, 26, 99999999],
 *       "counts": [1, 2, 3, 4, 5]
 *     },
 *     "negative": {
 *       "indices": [-123, 0, 12345],
 *       "counts": [20, 30, 40]
 *     }
 *    }
 * </code></pre>
 */
public class ExponentialHistogramFieldMapper extends FieldMapper {

    public static final FeatureFlag EXPONENTIAL_HISTOGRAM_FEATURE = new FeatureFlag("exponential_histogram");

    public static final String CONTENT_TYPE = "exponential_histogram";

    public static final ParseField SCALE_FIELD = new ParseField(ExponentialHistogramXContent.SCALE_FIELD);
    public static final ParseField SUM_FIELD = new ParseField(ExponentialHistogramXContent.SUM_FIELD);
    public static final ParseField MIN_FIELD = new ParseField(ExponentialHistogramXContent.MIN_FIELD);
    public static final ParseField MAX_FIELD = new ParseField(ExponentialHistogramXContent.MAX_FIELD);
    public static final ParseField ZERO_FIELD = new ParseField(ExponentialHistogramXContent.ZERO_FIELD);
    public static final ParseField ZERO_COUNT_FIELD = new ParseField(ExponentialHistogramXContent.ZERO_COUNT_FIELD);
    public static final ParseField ZERO_THRESHOLD_FIELD = new ParseField(ExponentialHistogramXContent.ZERO_THRESHOLD_FIELD);

    public static final ParseField POSITIVE_FIELD = new ParseField(ExponentialHistogramXContent.POSITIVE_FIELD);
    public static final ParseField NEGATIVE_FIELD = new ParseField(ExponentialHistogramXContent.NEGATIVE_FIELD);
    public static final ParseField BUCKET_INDICES_FIELD = new ParseField(ExponentialHistogramXContent.BUCKET_INDICES_FIELD);
    public static final ParseField BUCKET_COUNTS_FIELD = new ParseField(ExponentialHistogramXContent.BUCKET_COUNTS_FIELD);

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
    private static String zeroThresholdSubFieldName(String fullPath) {
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

    static class Builder extends FieldMapper.Builder {

        private final FieldMapper.Parameter<Map<String, String>> meta = FieldMapper.Parameter.metaParam();
        private final FieldMapper.Parameter<Explicit<Boolean>> ignoreMalformed;

        Builder(String name, boolean ignoreMalformedByDefault) {
            super(name);
            this.ignoreMalformed = FieldMapper.Parameter.explicitBoolParam(
                "ignore_malformed",
                true,
                m -> toType(m).ignoreMalformed,
                ignoreMalformedByDefault
            );
        }

        @Override
        protected FieldMapper.Parameter<?>[] getParameters() {
            return new FieldMapper.Parameter<?>[] { ignoreMalformed, meta };
        }

        @Override
        public ExponentialHistogramFieldMapper build(MapperBuilderContext context) {
            return new ExponentialHistogramFieldMapper(
                leafName(),
                new ExponentialHistogramFieldType(context.buildFullName(leafName()), meta.getValue()),
                builderParams(this, context),
                this
            );
        }
    }

    public static final FieldMapper.TypeParser PARSER = new FieldMapper.TypeParser(
        (n, c) -> new Builder(n, IGNORE_MALFORMED_SETTING.get(c.getSettings())),
        notInMultiFields(CONTENT_TYPE)
    );

    private final Explicit<Boolean> ignoreMalformed;
    private final boolean ignoreMalformedByDefault;

    ExponentialHistogramFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        FieldMapper.BuilderParams builderParams,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, builderParams);
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.ignoreMalformedByDefault = builder.ignoreMalformed.getDefaultValue().value();
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
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), ignoreMalformedByDefault).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    static class ExponentialHistogramFieldType extends MappedFieldType {

        ExponentialHistogramFieldType(String name, Map<String, String> meta) {
            super(name, false, false, true, TextSearchInfo.NONE, meta);
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
        public boolean isAggregatable() {
            return false;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("The [" + CONTENT_TYPE + "] field does not support this operation currently");
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

            Double sum = null;
            Double min = null;
            Double max = null;
            Integer scale = null;
            ParsedZeroBucket zeroBucket = ParsedZeroBucket.DEFAULT;
            List<IndexWithCount> negativeBuckets = Collections.emptyList();
            List<IndexWithCount> positiveBuckets = Collections.emptyList();

            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, context.parser());
            if (shouldStoreMalformedDataForSyntheticSource) {
                var copyingParser = new CopyingXContentParser(context.parser());
                malformedDataForSyntheticSource = copyingParser.getBuilder();
                subParser = new XContentSubParser(copyingParser);
            } else {
                subParser = new XContentSubParser(context.parser());
            }
            token = subParser.nextToken();
            while (token != XContentParser.Token.END_OBJECT) {

                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, subParser);
                String fieldName = subParser.currentName();
                if (fieldName.equals(SCALE_FIELD.getPreferredName())) {
                    token = subParser.nextToken();

                    ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser);
                    scale = subParser.intValue();
                    if (scale > ExponentialHistogram.MAX_SCALE || scale < ExponentialHistogram.MIN_SCALE) {
                        throw new DocumentParsingException(
                            subParser.getTokenLocation(),
                            "error parsing field ["
                                + fullPath()
                                + "], scale field must be in "
                                + "range ["
                                + ExponentialHistogram.MIN_SCALE
                                + ", "
                                + ExponentialHistogram.MAX_SCALE
                                + "] but got "
                                + scale
                        );
                    }
                } else if (fieldName.equals(SUM_FIELD.getPreferredName())) {
                    sum = parseDoubleAllowingInfinity(subParser);
                } else if (fieldName.equals(MIN_FIELD.getPreferredName())) {
                    min = parseDoubleAllowingInfinity(subParser);
                } else if (fieldName.equals(MAX_FIELD.getPreferredName())) {
                    max = parseDoubleAllowingInfinity(subParser);
                } else if (fieldName.equals(ZERO_FIELD.getPreferredName())) {
                    zeroBucket = parseZeroBucket(subParser);
                } else if (fieldName.equals(POSITIVE_FIELD.getPreferredName())) {
                    positiveBuckets = readAndValidateBuckets(POSITIVE_FIELD.getPreferredName(), subParser);
                } else if (fieldName.equals(NEGATIVE_FIELD.getPreferredName())) {
                    negativeBuckets = readAndValidateBuckets(NEGATIVE_FIELD.getPreferredName(), subParser);
                } else {
                    throw new DocumentParsingException(
                        subParser.getTokenLocation(),
                        "error parsing field [" + fullPath() + "], with unknown parameter [" + fieldName + "]"
                    );
                }
                token = subParser.nextToken();
            }
            if (scale == null) {
                throw new DocumentParsingException(
                    subParser.getTokenLocation(),
                    "error parsing field [" + fullPath() + "], expected field called [" + SCALE_FIELD.getPreferredName() + "]"
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

            long totalValueCount;
            try {
                totalValueCount = getTotalValueCount(zeroBucket, positiveBuckets, negativeBuckets);
            } catch (ArithmeticException e) {
                throw new IllegalArgumentException(
                    "Field [" + fullPath() + "] has a total value count exceeding the allowed maximum value of " + Long.MAX_VALUE
                );
            }

            sum = validateOrEstimateSum(sum, scale, negativeBuckets, positiveBuckets, totalValueCount, subParser);
            min = validateOrEstimateMin(min, zeroBucket, scale, negativeBuckets, positiveBuckets, totalValueCount, subParser);
            max = validateOrEstimateMax(max, zeroBucket, scale, negativeBuckets, positiveBuckets, totalValueCount, subParser);

            BytesStreamOutput histogramBytesOutput = new BytesStreamOutput();
            CompressedExponentialHistogram.writeHistogramBytes(histogramBytesOutput, scale, negativeBuckets, positiveBuckets);
            BytesRef histoBytes = histogramBytesOutput.bytes().toBytesRef();

            Field histoField = new BinaryDocValuesField(fullPath(), histoBytes);
            long thresholdAsLong = NumericUtils.doubleToSortableLong(zeroBucket.threshold());
            NumericDocValuesField zeroThresholdField = new NumericDocValuesField(zeroThresholdSubFieldName(fullPath()), thresholdAsLong);
            NumericDocValuesField valuesCountField = new NumericDocValuesField(valuesCountSubFieldName(fullPath()), totalValueCount);
            NumericDocValuesField sumField = new NumericDocValuesField(
                valuesSumSubFieldName(fullPath()),
                NumericUtils.doubleToSortableLong(sum)
            );

            context.doc().addWithKey(fieldType().name(), histoField);
            context.doc().add(zeroThresholdField);
            context.doc().add(valuesCountField);
            context.doc().add(sumField);
            if (min != null) {
                NumericDocValuesField minField = new NumericDocValuesField(
                    valuesMinSubFieldName(fullPath()),
                    NumericUtils.doubleToSortableLong(min)
                );
                context.doc().add(minField);
            }
            if (max != null) {
                NumericDocValuesField maxField = new NumericDocValuesField(
                    valuesMaxSubFieldName(fullPath()),
                    NumericUtils.doubleToSortableLong(max)
                );
                context.doc().add(maxField);
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

    private Double validateOrEstimateSum(
        Double sum,
        Integer scale,
        List<IndexWithCount> negativeBuckets,
        List<IndexWithCount> positiveBuckets,
        long totalValueCount,
        XContentSubParser subParser
    ) {
        if (sum == null) {
            return ExponentialHistogramUtils.estimateSum(
                IndexWithCount.asBuckets(scale, negativeBuckets).iterator(),
                IndexWithCount.asBuckets(scale, positiveBuckets).iterator()
            );
        }
        if (totalValueCount == 0 && sum != 0.0) {
            throw new DocumentParsingException(
                subParser.getTokenLocation(),
                "error parsing field [" + fullPath() + "], sum field must be zero if the histogram is empty, but got " + sum
            );
        }
        return sum;
    }

    private Double validateOrEstimateMin(
        Double parsedMin,
        ParsedZeroBucket zeroBucket,
        Integer scale,
        List<IndexWithCount> negativeBuckets,
        List<IndexWithCount> positiveBuckets,
        long totalValueCount,
        XContentSubParser subParser
    ) {
        if (parsedMin == null) {
            OptionalDouble estimatedMin = ExponentialHistogramUtils.estimateMin(
                ZeroBucket.create(zeroBucket.threshold(), zeroBucket.count),
                IndexWithCount.asBuckets(scale, negativeBuckets),
                IndexWithCount.asBuckets(scale, positiveBuckets)
            );
            return estimatedMin.isPresent() ? estimatedMin.getAsDouble() : null;
        }
        if (totalValueCount == 0) {
            throw new DocumentParsingException(
                subParser.getTokenLocation(),
                "error parsing field [" + fullPath() + "], min field must be null if the histogram is empty, but got " + parsedMin
            );
        }
        return parsedMin;
    }

    private Double validateOrEstimateMax(
        Double parsedMax,
        ParsedZeroBucket zeroBucket,
        Integer scale,
        List<IndexWithCount> negativeBuckets,
        List<IndexWithCount> positiveBuckets,
        long totalValueCount,
        XContentSubParser subParser
    ) {
        if (parsedMax == null) {
            OptionalDouble estimatedMax = ExponentialHistogramUtils.estimateMax(
                ZeroBucket.create(zeroBucket.threshold(), zeroBucket.count),
                IndexWithCount.asBuckets(scale, negativeBuckets),
                IndexWithCount.asBuckets(scale, positiveBuckets)
            );
            return estimatedMax.isPresent() ? estimatedMax.getAsDouble() : null;
        }
        if (totalValueCount == 0) {
            throw new DocumentParsingException(
                subParser.getTokenLocation(),
                "error parsing field [" + fullPath() + "], max field must be null if the histogram is empty, but got " + parsedMax
            );
        }
        return parsedMax;
    }

    private double parseDoubleAllowingInfinity(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        boolean isValidNumber = token == XContentParser.Token.VALUE_NUMBER;
        if (token == XContentParser.Token.VALUE_STRING) {
            String text = parser.text();
            if (text.equals("-Infinity") || text.equals("Infinity")) {
                isValidNumber = true;
            }
        }
        if (isValidNumber) {
            return parser.doubleValue();
        }
        throw new DocumentParsingException(
            parser.getTokenLocation(),
            "error parsing field [" + fullPath() + "], expected a number but got " + token
        );
    }

    private static long getTotalValueCount(
        ParsedZeroBucket zeroBucket,
        List<IndexWithCount> positiveBuckets,
        List<IndexWithCount> negativeBuckets
    ) {
        long totalValueCount = zeroBucket.count();
        for (IndexWithCount bucket : positiveBuckets) {
            totalValueCount = Math.addExact(totalValueCount, bucket.count());
        }
        for (IndexWithCount bucket : negativeBuckets) {
            totalValueCount = Math.addExact(totalValueCount, bucket.count());
        }
        return totalValueCount;
    }

    private ParsedZeroBucket parseZeroBucket(XContentSubParser subParser) throws IOException {
        long zeroCount = ParsedZeroBucket.DEFAULT.count();
        double zeroThreshold = ParsedZeroBucket.DEFAULT.threshold();

        XContentParser.Token token = subParser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, subParser);
        token = subParser.nextToken();
        while (token != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, subParser);
            String fieldName = subParser.currentName();
            if (fieldName.equals(ZERO_THRESHOLD_FIELD.getPreferredName())) {
                token = subParser.nextToken();
                ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser);
                zeroThreshold = subParser.doubleValue();
                if (zeroThreshold < 0.0 || Double.isFinite(zeroThreshold) == false) {
                    throw new DocumentParsingException(
                        subParser.getTokenLocation(),
                        "error parsing field ["
                            + fullPath()
                            + "], zero.threshold field must be a non-negative, finite number but got "
                            + zeroThreshold
                    );
                }
            } else if (fieldName.equals(ZERO_COUNT_FIELD.getPreferredName())) {
                token = subParser.nextToken();
                ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser);
                zeroCount = subParser.longValue();
                if (zeroCount < 0) {
                    throw new DocumentParsingException(
                        subParser.getTokenLocation(),
                        "error parsing field [" + fullPath() + "], zero.count field must be a non-negative number but got " + zeroCount
                    );
                }
            } else {
                throw new DocumentParsingException(
                    subParser.getTokenLocation(),
                    "error parsing field [" + fullPath() + "], with unknown parameter for zero sub-object [" + fieldName + "]"
                );
            }
            token = subParser.nextToken();
        }
        return new ParsedZeroBucket(zeroCount, zeroThreshold);
    }

    private List<IndexWithCount> readAndValidateBuckets(String containerFieldName, XContentSubParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        token = parser.nextToken();

        List<Long> indices = Collections.emptyList();
        List<Long> counts = Collections.emptyList();

        while (token != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String fieldName = parser.currentName();
            if (fieldName.equals(BUCKET_INDICES_FIELD.getPreferredName())) {
                indices = new ArrayList<>();
                token = parser.nextToken();
                // should be an array
                ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
                token = parser.nextToken();
                while (token != XContentParser.Token.END_ARRAY) {
                    ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                    long index = parser.longValue();
                    if (index < MIN_INDEX || index > MAX_INDEX) {
                        throw new DocumentParsingException(
                            parser.getTokenLocation(),
                            "error parsing field ["
                                + fullPath()
                                + "], "
                                + containerFieldName
                                + "."
                                + BUCKET_INDICES_FIELD.getPreferredName()
                                + " values must all be in range ["
                                + MIN_INDEX
                                + ", "
                                + MAX_INDEX
                                + "] but got "
                                + index
                        );
                    }
                    indices.add(index);
                    token = parser.nextToken();
                }
            } else if (fieldName.equals(BUCKET_COUNTS_FIELD.getPreferredName())) {
                counts = new ArrayList<>();
                token = parser.nextToken();
                // should be an array
                ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
                token = parser.nextToken();
                while (token != XContentParser.Token.END_ARRAY) {
                    ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                    long count = parser.longValue();
                    if (count <= 0) {
                        throw new DocumentParsingException(
                            parser.getTokenLocation(),
                            "error parsing field ["
                                + fullPath()
                                + "], "
                                + containerFieldName
                                + "."
                                + BUCKET_COUNTS_FIELD.getPreferredName()
                                + " values must all be greater than zero but got "
                                + count
                        );
                    }
                    counts.add(count);
                    token = parser.nextToken();
                }
            } else {
                throw new DocumentParsingException(
                    parser.getTokenLocation(),
                    "error parsing field ["
                        + fullPath()
                        + "], with unknown parameter for "
                        + containerFieldName
                        + " sub-object ["
                        + fieldName
                        + "]"
                );
            }
            token = parser.nextToken();
        }

        if (indices.size() != counts.size()) {
            throw new DocumentParsingException(
                parser.getTokenLocation(),
                "error parsing field ["
                    + fullPath()
                    + "], expected same length from ["
                    + containerFieldName
                    + "."
                    + BUCKET_INDICES_FIELD.getPreferredName()
                    + "] and "
                    + "["
                    + containerFieldName
                    + "."
                    + BUCKET_COUNTS_FIELD.getPreferredName()
                    + "] but got ["
                    + indices.size()
                    + " != "
                    + counts.size()
                    + "]"
            );
        }

        List<IndexWithCount> results = new ArrayList<>(indices.size());
        for (int i = 0; i < indices.size(); i++) {
            results.add(new IndexWithCount(indices.get(i), counts.get(i)));
        }
        results.sort(Comparator.comparing(IndexWithCount::index));

        for (int i = 1; i < results.size(); i++) {
            long index = results.get(i).index();
            if (index == results.get(i - 1).index()) {
                throw new DocumentParsingException(
                    parser.getTokenLocation(),
                    "error parsing field ["
                        + fullPath()
                        + "], expected entries of ["
                        + containerFieldName
                        + "."
                        + BUCKET_INDICES_FIELD.getPreferredName()
                        + "] to be unique, but got "
                        + index
                        + " multiple times"
                );
            }
        }
        return results;
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

    private class ExponentialHistogramSyntheticFieldLoader implements CompositeSyntheticFieldLoader.DocValuesLayer {

        private final CompressedExponentialHistogram histogram = new CompressedExponentialHistogram();
        private BytesRef binaryValue;
        private double zeroThreshold;
        private long valueCount;
        private double valueSum;
        private double valueMin;
        private double valueMax;

        @Override
        public SourceLoader.SyntheticFieldLoader.DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf)
            throws IOException {
            BinaryDocValues histoDocValues = leafReader.getBinaryDocValues(fieldType().name());
            if (histoDocValues == null) {
                // No values in this leaf
                binaryValue = null;
                return null;
            }
            NumericDocValues zeroThresholds = leafReader.getNumericDocValues(zeroThresholdSubFieldName(fullPath()));
            NumericDocValues valueCounts = leafReader.getNumericDocValues(valuesCountSubFieldName(fullPath()));
            NumericDocValues valueSums = leafReader.getNumericDocValues(valuesSumSubFieldName(fullPath()));
            NumericDocValues valueMinima = leafReader.getNumericDocValues(valuesMinSubFieldName(fullPath()));
            NumericDocValues valueMaxima = leafReader.getNumericDocValues(valuesMaxSubFieldName(fullPath()));
            assert zeroThresholds != null;
            assert valueCounts != null;
            assert valueSums != null;
            return docId -> {
                if (histoDocValues.advanceExact(docId)) {

                    boolean zeroThresholdPresent = zeroThresholds.advanceExact(docId);
                    boolean valueCountsPresent = valueCounts.advanceExact(docId);
                    boolean valueSumsPresent = valueSums.advanceExact(docId);
                    assert zeroThresholdPresent && valueCountsPresent && valueSumsPresent;

                    binaryValue = histoDocValues.binaryValue();
                    zeroThreshold = NumericUtils.sortableLongToDouble(zeroThresholds.longValue());
                    valueCount = valueCounts.longValue();
                    valueSum = NumericUtils.sortableLongToDouble(valueSums.longValue());

                    if (valueMinima != null && valueMinima.advanceExact(docId)) {
                        valueMin = NumericUtils.sortableLongToDouble(valueMinima.longValue());
                    } else {
                        valueMin = Double.NaN;
                    }
                    if (valueMaxima != null && valueMaxima.advanceExact(docId)) {
                        valueMax = NumericUtils.sortableLongToDouble(valueMaxima.longValue());
                    } else {
                        valueMax = Double.NaN;
                    }
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

            histogram.reset(zeroThreshold, valueCount, valueSum, valueMin, valueMax, binaryValue);
            ExponentialHistogramXContent.serialize(b, histogram);
        }

        @Override
        public String fieldName() {
            return fullPath();
        }

        @Override
        public long valueCount() {
            return binaryValue != null ? 1 : 0;
        }
    };

    private record ParsedZeroBucket(long count, double threshold) {
        static final ParsedZeroBucket DEFAULT = new ParsedZeroBucket(0, 0);
    }
}
