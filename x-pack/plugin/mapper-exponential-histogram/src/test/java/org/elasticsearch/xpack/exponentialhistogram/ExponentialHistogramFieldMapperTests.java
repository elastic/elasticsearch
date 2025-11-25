/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Types;
import org.elasticsearch.exponentialhistogram.CompressedExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramTestUtils;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils;
import org.elasticsearch.exponentialhistogram.ZeroBucket;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.analytics.mapper.ExponentialHistogramParser;
import org.elasticsearch.xpack.analytics.mapper.HistogramParser;
import org.elasticsearch.xpack.analytics.mapper.IndexWithCount;
import org.elasticsearch.xpack.analytics.mapper.ParsedHistogramConverter;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.ExponentialHistogramAggregatorTestCase;
import org.junit.AssumptionViolatedException;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.stream.IntStream;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_SCALE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramFieldMapperTests extends MapperTestCase {

    @Before
    public void setup() {
        assumeTrue(
            "Only when exponential_histogram feature flag is enabled",
            ExponentialHistogramParser.EXPONENTIAL_HISTOGRAM_FEATURE.isEnabled()
        );
    }

    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new ExponentialHistogramMapperPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", ExponentialHistogramFieldMapper.CONTENT_TYPE);
    }

    @Override
    protected Object getSampleValueForDocument() {
        return Map.of(
            "scale",
            10,
            "zero",
            Map.of("count", 42, "threshold", 1.234),
            "positive",
            Map.of("indices", List.of(-1, 0, 1), "counts", List.of(2, 3, 4)),
            "negative",
            Map.of("indices", List.of(-100, 100), "counts", List.of(1000, 2000))
        );
    }

    @Override
    protected Object getSampleObjectForDocument() {
        return getSampleValueForDocument();
    }

    @Override
    protected boolean supportsSearchLookup() {
        return false;
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return true;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true), m -> assertTrue(m.ignoreMalformed()));
        checker.registerUpdateCheck(b -> b.field("coerce", false), m -> assertFalse(((ExponentialHistogramFieldMapper) m).coerce()));
    }

    public void testCoerce() throws IOException {
        List<Double> centroids = randomDoubles().map(val -> val * 1_000_000 - 500_000)
            .map(val -> randomBoolean() ? val : 0)
            .distinct()
            .limit(randomIntBetween(0, 100))
            .sorted()
            .boxed()
            .toList();
        List<Long> counts = IntStream.range(0, centroids.size()).mapToLong(i -> randomIntBetween(0, 100)).boxed().toList();

        HistogramParser.ParsedHistogram input = new HistogramParser.ParsedHistogram(centroids, counts);

        XContentBuilder inputJson = XContentFactory.jsonBuilder();
        inputJson.startObject()
            .field("field")
            .startObject()
            .array("values", centroids.toArray())
            .array("counts", counts.toArray())
            .endObject()
            .endObject();
        BytesReference inputDocBytes = BytesReference.bytes(inputJson);

        ExponentialHistogramParser.ParsedExponentialHistogram expectedCoerced = ParsedHistogramConverter.tDigestToExponential(input);

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("1", inputDocBytes, XContentType.JSON));
        ExponentialHistogramParser.ParsedExponentialHistogram ingestedHisto = docValueToParsedHistogram(doc, "field");
        assertThat(ingestedHisto, equalTo(expectedCoerced));

        DocumentMapper coerceDisabledMapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "exponential_histogram").field("coerce", false))
        );
        ThrowingRunnable runnable = () -> coerceDisabledMapper.parse(new SourceToParse("1", inputDocBytes, XContentType.JSON));
        DocumentParsingException e = expectThrows(DocumentParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString("unknown parameter [values]"));
    }

    private static IndexableField getSingleField(ParsedDocument doc, String fieldName) {
        List<IndexableField> fields = doc.rootDoc().getFields(fieldName);
        assertThat(fields.size(), equalTo(1));
        return fields.getFirst();
    }

    private static ExponentialHistogramParser.ParsedExponentialHistogram docValueToParsedHistogram(ParsedDocument doc, String fieldName) {
        BytesRef encodedBytes = getSingleField(doc, fieldName).binaryValue();
        long valueCount = getSingleField(doc, ExponentialHistogramFieldMapper.valuesCountSubFieldName(fieldName)).numericValue()
            .longValue();
        double zeroThreshold = NumericUtils.sortableLongToDouble(
            getSingleField(doc, ExponentialHistogramFieldMapper.zeroThresholdSubFieldName(fieldName)).numericValue().longValue()
        );

        // min max and sum are not relevant for these tests, so we use fake ones
        double min = valueCount == 0 ? Double.NaN : 0.0;
        double max = valueCount == 0 ? Double.NaN : 0.0;
        double sum = 0;

        CompressedExponentialHistogram histogram = new CompressedExponentialHistogram();
        try {
            histogram.reset(zeroThreshold, valueCount, sum, min, max, encodedBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new ExponentialHistogramParser.ParsedExponentialHistogram(
            histogram.scale(),
            histogram.zeroBucket().zeroThreshold(),
            histogram.zeroBucket().count(),
            IndexWithCount.fromIterator(histogram.negativeBuckets().iterator()),
            IndexWithCount.fromIterator(histogram.positiveBuckets().iterator()),
            null,
            null,
            null
        );
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        throw new AssumptionViolatedException("Exponential histograms currently don't support fielddata");
    }

    private static Map<String, Object> createRandomHistogramValue(int maxBucketCount) {
        int scale = randomIntBetween(MIN_SCALE, MAX_SCALE);
        long maxCounts = Long.MAX_VALUE / (maxBucketCount + 1);
        long zeroCount = randomBoolean() ? 0 : randomLongBetween(0, maxCounts);
        double zeroThreshold = randomBoolean() ? 0 : randomDouble();
        List<Long> positiveIndices = new ArrayList<>();
        List<Long> positiveCounts = new ArrayList<>();
        List<Long> negativeIndices = new ArrayList<>();
        List<Long> negativeCounts = new ArrayList<>();
        if (randomBoolean()) {
            fillBucketsRandomly(positiveIndices, positiveCounts, maxBucketCount / 2);
        }
        if (randomBoolean()) {
            fillBucketsRandomly(negativeIndices, negativeCounts, maxBucketCount / 2);
        }

        Map<String, Object> result = new HashMap<>(
            Map.of(
                "scale",
                scale,
                "zero",
                Map.of("count", zeroCount, "threshold", zeroThreshold),
                "positive",
                Map.of("indices", positiveIndices, "counts", positiveCounts),
                "negative",
                Map.of("indices", negativeIndices, "counts", negativeCounts)
            )
        );
        if ((positiveIndices.isEmpty() == false || negativeIndices.isEmpty() == false)) {
            if (randomBoolean()) {
                result.put("sum", randomDoubleBetween(-1000, 1000, true));
            }
            if (randomBoolean()) {
                result.put("min", randomDoubleBetween(-1000, 1000, true));
            }
            if (randomBoolean()) {
                result.put("max", randomDoubleBetween(-1000, 1000, true));
            }
        }
        return result;
    }

    private static void fillBucketsRandomly(List<Long> indices, List<Long> counts, int maxBucketCount) {
        int bucketCount = randomIntBetween(0, maxBucketCount);
        long maxCounts = Long.MAX_VALUE / (maxBucketCount * 2L + 1);
        boolean useDense = randomBoolean();
        if (useDense) {
            // Use dense indices, i.e., indices are sequential and start at MIN_INDEX
            long startIndex = randomLongBetween(MIN_INDEX, MAX_INDEX - bucketCount);
            for (int i = 0; i < bucketCount; i++) {
                indices.add(startIndex + i);
                counts.add(randomLongBetween(1, maxCounts));
            }
        } else {
            Set<Long> usedIndices = new HashSet<>();
            for (int i = 0; i < bucketCount; i++) {
                long index;
                do {
                    index = randomLongBetween(MIN_INDEX, MAX_INDEX);
                } while (usedIndices.add(index) == false);
                indices.add(index);
                counts.add(randomLongBetween(1, maxCounts));
            }
        }
    }

    @Override
    protected List<ExampleMalformedValue> exampleMalformedValues() {
        var randomString = randomAlphaOfLengthBetween(1, 10);
        var randomLong = randomLong();
        var randomDouble = randomDouble();
        var randomBoolean = randomBoolean();

        return List.of(
            // Basic type validation - non-object values
            exampleMalformedValue(b -> b.value(randomString)).errorMatches(
                "Failed to parse object: expecting token of type [START_OBJECT]"
            ),
            exampleMalformedValue(b -> b.value(randomLong)).errorMatches("Failed to parse object: expecting token of type [START_OBJECT]"),
            exampleMalformedValue(b -> b.value(randomDouble)).errorMatches(
                "Failed to parse object: expecting token of type [START_OBJECT]"
            ),
            exampleMalformedValue(b -> b.value(randomBoolean)).errorMatches(
                "Failed to parse object: expecting token of type [START_OBJECT]"
            ),

            // Missing scale field
            exampleMalformedValue(b -> b.startObject().endObject()).errorMatches("expected field called [scale]"),

            // Scale field validation
            exampleMalformedValue(b -> b.startObject().field("scale", "foo").endObject()).errorMatches(
                "Failed to parse object: expecting token of type [VALUE_NUMBER]"
            ),
            exampleMalformedValue(b -> b.startObject().field("scale", MIN_SCALE - 1).endObject()).errorMatches(
                "scale field must be in range [" + MIN_SCALE + ", " + MAX_SCALE + "] but got " + (MIN_SCALE - 1)
            ),
            exampleMalformedValue(b -> b.startObject().field("scale", MAX_SCALE + 1).endObject()).errorMatches(
                "scale field must be in range [" + MIN_SCALE + ", " + MAX_SCALE + "] but got " + (MAX_SCALE + 1)
            ),

            // Zero field validation - wrong token type
            exampleMalformedValue(b -> b.startObject().field("scale", 0).field("zero", "not_an_object").endObject()).errorMatches(
                "Failed to parse object: expecting token of type [START_OBJECT]"
            ),

            // Zero.threshold field validation
            exampleMalformedValue(
                b -> b.startObject().field("scale", 0).startObject("zero").field("threshold", "not_a_number").endObject().endObject()
            ).errorMatches("Failed to parse object: expecting token of type [VALUE_NUMBER]"),
            exampleMalformedValue(
                b -> b.startObject().field("scale", 0).startObject("zero").field("threshold", -1.0).endObject().endObject()
            ).errorMatches("zero.threshold field must be a non-negative, finite number but got -1.0"),
            // Zero.count field validation
            exampleMalformedValue(
                b -> b.startObject().field("scale", 0).startObject("zero").field("count", "not_a_number").endObject().endObject()
            ).errorMatches("Failed to parse object: expecting token of type [VALUE_NUMBER]"),
            exampleMalformedValue(b -> b.startObject().field("scale", 0).startObject("zero").field("count", -1).endObject().endObject())
                .errorMatches("zero.count field must be a non-negative number but got -1"),

            // Unknown field in zero sub-object
            exampleMalformedValue(
                b -> b.startObject().field("scale", 0).startObject("zero").field("unknown_field", 123).endObject().endObject()
            ).errorMatches("with unknown parameter for zero sub-object [unknown_field]"),

            // Positive/negative field validation - wrong token type
            exampleMalformedValue(b -> b.startObject().field("scale", 0).field("positive", "not_an_object").endObject()).errorMatches(
                "Failed to parse object: expecting token of type [START_OBJECT]"
            ),
            exampleMalformedValue(b -> b.startObject().field("scale", 0).field("negative", "not_an_object").endObject()).errorMatches(
                "Failed to parse object: expecting token of type [START_OBJECT]"
            ),

            // indices validation - wrong token type
            exampleMalformedValue(
                b -> b.startObject().field("scale", 0).startObject("positive").field("indices", "not_an_array").endObject().endObject()
            ).errorMatches("Failed to parse object: expecting token of type [START_ARRAY]"),

            // counts validation - wrong token type
            exampleMalformedValue(
                b -> b.startObject().field("scale", 0).startObject("positive").field("counts", "not_an_array").endObject().endObject()
            ).errorMatches("Failed to parse object: expecting token of type [START_ARRAY]"),

            // indices array element validation - wrong token type
            exampleMalformedValue(
                b -> b.startObject()
                    .field("scale", 0)
                    .startObject("positive")
                    .startArray("indices")
                    .value("not_a_number")
                    .endArray()
                    .startArray("counts")
                    .value(1)
                    .endArray()
                    .endObject()
                    .endObject()
            ).errorMatches("Failed to parse object: expecting token of type [VALUE_NUMBER]"),

            // counts array element validation - wrong token type
            exampleMalformedValue(
                b -> b.startObject()
                    .field("scale", 0)
                    .startObject("positive")
                    .startArray("indices")
                    .value(1)
                    .endArray()
                    .startArray("counts")
                    .value("not_a_number")
                    .endArray()
                    .endObject()
                    .endObject()
            ).errorMatches("Failed to parse object: expecting token of type [VALUE_NUMBER]"),

            // indices value range validation
            exampleMalformedValue(
                b -> b.startObject()
                    .field("scale", 0)
                    .startObject("positive")
                    .startArray("indices")
                    .value(MIN_INDEX - 1)
                    .endArray()
                    .startArray("counts")
                    .value(1)
                    .endArray()
                    .endObject()
                    .endObject()
            ).errorMatches(
                "positive.indices values must all be in range [" + MIN_INDEX + ", " + MAX_INDEX + "] but got " + (MIN_INDEX - 1)
            ),

            exampleMalformedValue(
                b -> b.startObject()
                    .field("scale", 0)
                    .startObject("positive")
                    .startArray("indices")
                    .value(MAX_INDEX + 1)
                    .endArray()
                    .startArray("counts")
                    .value(1)
                    .endArray()
                    .endObject()
                    .endObject()
            ).errorMatches(
                "positive.indices values must all be in range [" + MIN_INDEX + ", " + MAX_INDEX + "] but got " + (MAX_INDEX + 1)
            ),

            // counts value validation - zero or negative
            exampleMalformedValue(
                b -> b.startObject()
                    .field("scale", 0)
                    .startObject("positive")
                    .startArray("indices")
                    .value(1)
                    .endArray()
                    .startArray("counts")
                    .value(0)
                    .endArray()
                    .endObject()
                    .endObject()
            ).errorMatches("positive.counts values must all be greater than zero but got 0"),
            exampleMalformedValue(
                b -> b.startObject()
                    .field("scale", 0)
                    .startObject("positive")
                    .startArray("indices")
                    .value(1)
                    .endArray()
                    .startArray("counts")
                    .value(-1)
                    .endArray()
                    .endObject()
                    .endObject()
            ).errorMatches("positive.counts values must all be greater than zero but got -1"),

            // Mismatched array lengths
            exampleMalformedValue(
                b -> b.startObject()
                    .field("scale", 0)
                    .startObject("positive")
                    .startArray("indices")
                    .value(1)
                    .value(2)
                    .endArray()
                    .startArray("counts")
                    .value(1)
                    .endArray()
                    .endObject()
                    .endObject()
            ).errorMatches("expected same length from [positive.indices] and [positive.counts] but got [2 != 1]"),

            // Duplicate indices
            exampleMalformedValue(
                b -> b.startObject()
                    .field("scale", 0)
                    .startObject("positive")
                    .startArray("indices")
                    .value(1)
                    .value(1)
                    .endArray()
                    .startArray("counts")
                    .value(1)
                    .value(2)
                    .endArray()
                    .endObject()
                    .endObject()
            ).errorMatches("expected entries of [positive.indices] to be unique, but got 1 multiple times"),

            // Unknown field in positive/negative sub-object
            exampleMalformedValue(
                b -> b.startObject().field("scale", 0).startObject("positive").field("unknown_field", 123).endObject().endObject()
            ).errorMatches("with unknown parameter for positive sub-object [unknown_field]"),

            exampleMalformedValue(
                b -> b.startObject().field("scale", 0).startObject("negative").field("unknown_field", 123).endObject().endObject()
            ).errorMatches("with unknown parameter for negative sub-object [unknown_field]"),

            // Unknown top-level field
            exampleMalformedValue(b -> b.startObject().field("scale", 0).field("unknown_field", 123).endObject()).errorMatches(
                "with unknown parameter [unknown_field]"
            ),

            // Overflow of total value counts
            exampleMalformedValue(
                b -> b.startObject()
                    .field("scale", 0)
                    .startObject("zero")
                    .field("count", 1)
                    .endObject()
                    .startObject("positive")
                    .startArray("indices")
                    .value(1)
                    .endArray()
                    .startArray("counts")
                    .value(Long.MAX_VALUE)
                    .endArray()
                    .endObject()
                    .endObject()
            ).errorMatches("has a total value count exceeding the allowed maximum value of " + Long.MAX_VALUE),

            // Non-Zero sum for empty histogram
            exampleMalformedValue(b -> b.startObject().field("scale", 0).field("sum", 42.0).endObject()).errorMatches(
                "sum field must be zero if the histogram is empty, but got 42.0"
            ),

            // Min provided for empty histogram
            exampleMalformedValue(b -> b.startObject().field("scale", 0).field("min", 42.0).endObject()).errorMatches(
                "min field must be null if the histogram is empty, but got 42.0"
            ),

            // Max provided for empty histogram
            exampleMalformedValue(b -> b.startObject().field("scale", 0).field("max", 42.0).endObject()).errorMatches(
                "max field must be null if the histogram is empty, but got 42.0"
            )
        );
    }

    public void testCannotBeUsedInMultifields() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("hist");
            b.field("type", "exponential_histogram");
            b.endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("Field [hist] of type [exponential_histogram] can't be used in multifields"));
    }

    public void testCannotUseHistogramInArrays() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse source = source(
            b -> b.startArray("field").startObject().field("scale", 1).endObject().startObject().field("scale", 2).endObject().endArray()
        );
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source));
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "Field [field] of type [exponential_histogram] doesn't support"
                    + " indexing multiple values for the same field in the same document"
            )
        );
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new SyntheticSourceSupport() {
            @Override
            public SyntheticSourceExample example(int maxValues) {
                Map<String, Object> histogram = createRandomHistogramValue(maxValues);
                return new SyntheticSourceExample(histogram, convertHistogramToCanonicalForm(histogram), this::mapping);
            }

            private Map<String, Object> convertHistogramToCanonicalForm(Map<String, Object> histogram) {
                Map<String, Object> result = new LinkedHashMap<>();
                int scale = (Integer) histogram.get("scale");
                result.put("scale", scale);

                List<IndexWithCount> positive = parseBuckets(Types.forciblyCast(histogram.get("positive")));
                List<IndexWithCount> negative = parseBuckets(Types.forciblyCast(histogram.get("negative")));

                Map<String, Object> zeroBucket = convertZeroBucketToCanonicalForm(Types.forciblyCast(histogram.get("zero")));

                Number sum = (Number) histogram.get("sum");
                ExponentialHistogram.Buckets negativeBuckets = IndexWithCount.asBuckets(scale, negative);
                ExponentialHistogram.Buckets positiveBuckets = IndexWithCount.asBuckets(scale, positive);

                boolean isEmpty = negativeBuckets.iterator().hasNext() == false
                    && positiveBuckets.iterator().hasNext() == false
                    && (zeroBucket == null || Types.<Number>forciblyCast(zeroBucket.getOrDefault("count", 0L)).longValue() == 0L);

                // we allow 0.0 as sum for input histograms, but output null in canonical form in that case
                if (isEmpty && (sum == null || sum.doubleValue() == 0.0)) {
                    sum = null;
                } else if (sum == null) {
                    sum = ExponentialHistogramUtils.estimateSum(negativeBuckets.iterator(), positiveBuckets.iterator());
                }
                if (sum != null) {
                    result.put("sum", sum);
                }

                Object min = histogram.get("min");
                if (min == null) {
                    OptionalDouble estimatedMin = ExponentialHistogramUtils.estimateMin(
                        mapToZeroBucket(zeroBucket),
                        negativeBuckets,
                        positiveBuckets
                    );
                    if (estimatedMin.isPresent()) {
                        min = estimatedMin.getAsDouble();
                    }
                }
                if (min != null) {
                    result.put("min", min);
                }

                Object max = histogram.get("max");
                if (max == null) {
                    OptionalDouble estimatedMax = ExponentialHistogramUtils.estimateMax(
                        mapToZeroBucket(zeroBucket),
                        negativeBuckets,
                        positiveBuckets
                    );
                    if (estimatedMax.isPresent()) {
                        max = estimatedMax.getAsDouble();
                    }
                }
                if (max != null) {
                    result.put("max", max);
                }

                if (zeroBucket != null) {
                    result.put("zero", zeroBucket);
                }
                if (positive.isEmpty() == false) {
                    result.put("positive", writeBucketsInCanonicalForm(positive));
                }
                if (negative.isEmpty() == false) {
                    result.put("negative", writeBucketsInCanonicalForm(negative));
                }

                return result;
            }

            private ZeroBucket mapToZeroBucket(Map<String, Object> zeroBucket) {
                if (zeroBucket == null) {
                    return ZeroBucket.minimalEmpty();
                }
                Number threshold = Types.forciblyCast(zeroBucket.get("threshold"));
                Number count = Types.forciblyCast(zeroBucket.get("count"));
                if (threshold != null && count != null) {
                    return ZeroBucket.create(threshold.doubleValue(), count.longValue());
                } else if (threshold != null) {
                    return ZeroBucket.create(threshold.doubleValue(), 0);
                } else if (count != null) {
                    return ZeroBucket.minimalWithCount(count.longValue());
                } else {
                    return ZeroBucket.minimalEmpty();
                }
            }

            private List<IndexWithCount> parseBuckets(Map<String, Object> buckets) {
                if (buckets == null) {
                    return List.of();
                }
                List<? extends Number> indices = Types.forciblyCast(buckets.get("indices"));
                List<? extends Number> counts = Types.forciblyCast(buckets.get("counts"));
                if (indices == null || indices.isEmpty()) {
                    return List.of();
                }
                List<IndexWithCount> indexWithCounts = new ArrayList<>();
                for (int i = 0; i < indices.size(); i++) {
                    indexWithCounts.add(new IndexWithCount(indices.get(i).longValue(), counts.get(i).longValue()));
                }
                indexWithCounts.sort(Comparator.comparing(IndexWithCount::index));
                return indexWithCounts;
            }

            private Map<String, Object> writeBucketsInCanonicalForm(List<IndexWithCount> buckets) {
                List<Long> resultIndices = new ArrayList<>();
                List<Long> resultCounts = new ArrayList<>();
                for (IndexWithCount indexWithCount : buckets) {
                    resultIndices.add(indexWithCount.index());
                    resultCounts.add(indexWithCount.count());
                }
                LinkedHashMap<String, Object> result = new LinkedHashMap<>();
                result.put("indices", resultIndices);
                result.put("counts", resultCounts);
                return result;
            }

            private Map<String, Object> convertZeroBucketToCanonicalForm(Map<String, Object> zeroBucket) {
                if (zeroBucket == null) {
                    return null;
                }
                Map<String, Object> result = new HashMap<>();
                Number threshold = Types.forciblyCast(zeroBucket.get("threshold"));
                if (threshold != null && threshold.doubleValue() != 0) {
                    result.put("threshold", threshold);
                }
                Number count = Types.forciblyCast(zeroBucket.get("count"));
                if (count != null && count.longValue() != 0) {
                    result.put("count", count);
                }
                return result.isEmpty() ? null : result;
            }

            private void mapping(XContentBuilder b) throws IOException {
                b.field("type", ExponentialHistogramFieldMapper.CONTENT_TYPE);
                if (ignoreMalformed) {
                    b.field("ignore_malformed", true);
                }
            }

            @Override
            public List<SyntheticSourceInvalidExample> invalidExample() {
                // We always support synthetic source independent of the configured mapping, so this test does not apply
                return List.of();
            }
        };
    }

    public void testFormattedDocValues() throws IOException {
        try (Directory directory = newDirectory()) {
            ExponentialHistogramCircuitBreaker noopBreaker = ExponentialHistogramCircuitBreaker.noop();

            List<? extends ExponentialHistogram> inputHistograms = IntStream.range(0, randomIntBetween(1, 100))
                .mapToObj(i -> ExponentialHistogramTestUtils.randomHistogram(noopBreaker))
                .map(
                    histo -> ExponentialHistogram.builder(histo, noopBreaker)
                        // make sure we have a double-based zero bucket, as we can only serialize those exactly
                        .zeroBucket(ZeroBucket.create(histo.zeroBucket().zeroThreshold(), histo.zeroBucket().count()))
                        .build()
                )
                .map(histogram -> randomBoolean() ? null : histogram)
                .toList();

            IndexWriterConfig config = LuceneTestCase.newIndexWriterConfig(random(), new MockAnalyzer(random()));
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, config);
            inputHistograms.forEach(histo -> ExponentialHistogramAggregatorTestCase.addHistogramDoc(indexWriter, "field", histo));
            indexWriter.close();

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                for (int i = 0; i < reader.leaves().size(); i++) {
                    LeafReaderContext leaf = reader.leaves().get(i);
                    int docBase = leaf.docBase;
                    LeafReader leafReader = leaf.reader();
                    int maxDoc = leafReader.maxDoc();
                    FormattedDocValues docValues = ExponentialHistogramFieldMapper.createFormattedDocValues(leafReader, "field");
                    for (int j = 0; j < maxDoc; j++) {
                        var expectedHistogram = inputHistograms.get(docBase + j);
                        if (expectedHistogram == null) {
                            assertThat(docValues.advanceExact(j), equalTo(false));
                            expectThrows(IllegalStateException.class, docValues::nextValue);
                        } else {
                            assertThat(docValues.advanceExact(j), equalTo(true));
                            assertThat(docValues.docValueCount(), equalTo(1));
                            Object actualHistogram = docValues.nextValue();
                            assertThat(actualHistogram, equalTo(expectedHistogram));
                            expectThrows(IllegalStateException.class, docValues::nextValue);
                        }
                    }
                }
            }
        }
    }

    public void testMetricType() throws IOException {
        // Test default setting
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        ExponentialHistogramFieldMapper.ExponentialHistogramFieldType ft =
            (ExponentialHistogramFieldMapper.ExponentialHistogramFieldType) mapperService.fieldType("field");
        assertNull(ft.getMetricType());

        assertMetricType("histogram", ExponentialHistogramFieldMapper.ExponentialHistogramFieldType::getMetricType);

        {
            String unsupportedMetricTypes = randomFrom("counter", "gauge", "position");
            // Test invalid metric type for this field type
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_metric", unsupportedMetricTypes);
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString(
                    "Unknown value [" + unsupportedMetricTypes + "] for field [time_series_metric] - accepted values are [histogram]"
                )
            );
        }
        {
            // Test invalid metric type
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_metric", "unknown");
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Unknown value [unknown] for field [time_series_metric] - accepted values are [histogram]")
            );
        }
    }

    @Override
    public void testSyntheticSourceKeepArrays() {
        // exponential_histogram can't be used within an array
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not yet implemented");
    }

    @Override
    protected List<SortShortcutSupport> getSortShortcutSupport() {
        return List.of();
    }
}
