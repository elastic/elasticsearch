/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexHistogramFieldData;
import org.elasticsearch.index.fielddata.LeafHistogramFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentSubParser;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * <p>A field mapper for an exponential histogram. The exponential histogram compresses bucket boundaries using
 * an exponential formula, making it suitable for conveying high dynamic range data with small relative error,
 * compared with alternative representations of similar size.</p>
 */
public class ExponentialHistogramFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "exponential_histogram";
    // public static final String COUNT_FIELD_NAME_SUFFIX = "_count";
    //
    // TODO(axw) count, sum, min, max

    public static final ParseField SCALE_FIELD = new ParseField("scale");
    public static final ParseField POSITIVE_FIELD = new ParseField("positive");
    public static final ParseField NEGATIVE_FIELD = new ParseField("negative");
    public static final ParseField OFFSET_FIELD = new ParseField("offset");
    public static final ParseField COUNTS_FIELD = new ParseField("counts");

    private static class ExponentialHistogramFieldType extends MappedFieldType {
        ExponentialHistogramFieldType(String name, Map<String, String> meta) {
            super(name, false, false, true, TextSearchInfo.NONE, meta);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException(
                "[" + CONTENT_TYPE + "] field do not support searching, " + "use dedicated aggregations instead: [" + name() + "]"
            );
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return (cache, breakerService) -> new IndexHistogramFieldData(name(), AnalyticsValuesSourceType.HISTOGRAM) {
                @Override
                public LeafHistogramFieldData load(LeafReaderContext context) {
                    return new LeafHistogramFieldData() {
                        @Override
                        public HistogramValues getHistogramValues() throws IOException {
                            try {
                                final BinaryDocValues values = DocValues.getBinary(context.reader(), fieldName);
                                final ExponentialHistogramValue value = new ExponentialHistogramValue();
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
                                final ExponentialHistogramValue value = new ExponentialHistogramValue();
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

                        @Override
                        public void close() {
                            // nothing to close
                        }
                    };
                }

                @Override
                public LeafHistogramFieldData loadDirect(LeafReaderContext context) {
                    return load(context);
                }

                @Override
                public SortField sortField(
                    Object missingValue,
                    MultiValueMode sortMode,
                    XFieldComparatorSource.Nested nested,
                    boolean reverse
                ) {
                    throw new UnsupportedOperationException("can't sort on the [" + CONTENT_TYPE + "] field");
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
    }

    public static class Builder extends FieldMapper.Builder {
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        protected Builder(String name) {
            super(name);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta };
        }

        @Override
        public ExponentialHistogramFieldMapper build(MapperBuilderContext context) {
            return new ExponentialHistogramFieldMapper(
                name,
                new ExponentialHistogramFieldType(context.buildFullName(name), meta.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo
            );
        }
    }

    public static TypeParser PARSER = new TypeParser((n, c) -> new ExponentialHistogramFieldMapper.Builder(n));

    protected ExponentialHistogramFieldMapper(String simpleName, MappedFieldType mappedFieldType, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    @Override
    public boolean supportsParsingObject() {
        return true;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    @Override
    public void parse(DocumentParserContext context) throws IOException {
        context.path().add(simpleName());
        try {
            Field field = parseExponentialHistogramField(context.parser());
            if (field != null) {
                if (context.doc().getByKey(fieldType().name()) != null) {
                    throw new IllegalArgumentException(
                        "Field ["
                            + name()
                            + "] of type ["
                            + typeName()
                            + "] doesn't not support indexing multiple values for the same field in the same document"
                    );
                }
                context.doc().addWithKey(fieldType().name(), field);
            }
        } catch (Exception ex) {
            throw new DocumentParsingException(
                context.parser().getTokenLocation(),
                "failed to parse field [" + fieldType().name() + "] of type [" + fieldType().typeName() + "]",
                ex
            );
        }
        context.path().remove();
    }

    private Field parseExponentialHistogramField(XContentParser parser) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        }

        boolean haveScale = false;
        int scale = 0;
        ExponentialHistogramBuckets positive = null;
        ExponentialHistogramBuckets negative = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        XContentParser subParser = new XContentSubParser(parser);
        token = subParser.nextToken();
        while (token != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, subParser);
            String fieldName = subParser.currentName();
            if (fieldName.equals(SCALE_FIELD.getPreferredName())) {
                token = subParser.nextToken();
                ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser);
                scale = subParser.intValue();
                haveScale = true;
            } else if (fieldName.equals(POSITIVE_FIELD.getPreferredName())) {
                token = subParser.nextToken();
                positive = ExponentialHistogramBuckets.parse(subParser);
            } else if (fieldName.equals(NEGATIVE_FIELD.getPreferredName())) {
                token = subParser.nextToken();
                negative = ExponentialHistogramBuckets.parse(subParser);
            } else {
                throw new DocumentParsingException(
                    subParser.getTokenLocation(),
                    "error parsing field [" + name() + "], with unknown parameter [" + fieldName + "]"
                );
            }
            token = subParser.nextToken();
        }

        if (haveScale == false) {
            throw new DocumentParsingException(
                subParser.getTokenLocation(),
                "error parsing field [" + name() + "], expected field called [" + SCALE_FIELD.getPreferredName() + "]"
            );
        }

        final int numPositiveCounts = positive == null ? 0 : positive.counts.size();
        final int numNegativeCounts = negative == null ? 0 : negative.counts.size();
        if (numPositiveCounts == 0 && numNegativeCounts == 0) {
            throw new DocumentParsingException(
                subParser.getTokenLocation(),
                "error parsing field ["
                    + name()
                    + "], "
                    + "expected at least one the fields called ["
                    + POSITIVE_FIELD.getPreferredName()
                    + "] or ["
                    + NEGATIVE_FIELD.getPreferredName()
                    + "] to be specified and non-empty"
            );
        }

        BytesStreamOutput streamOutput = new BytesStreamOutput();

        /*
         * TODO(axw) consider a more compressed format for buckets with run-length encoding for
         * contiguous zero counts. The current encoding is based directly on OpenTelemetry protobuf:
         * https://github.com/open-telemetry/opentelemetry-proto/blob/ea449ae0e9b282f96ec12a09e796dbb3d390ed4f/opentelemetry/proto/metrics/v1/metrics.proto#L539-L557
         */
        streamOutput.writeVInt(scale);
        streamOutput.writeVInt(numNegativeCounts);
        streamOutput.writeVInt(numPositiveCounts);
        if (numNegativeCounts > 0) {
            streamOutput.writeVInt(negative.offset);
        }
        if (numPositiveCounts > 0) {
            streamOutput.writeVInt(positive.offset);
        }
        if (numNegativeCounts > 0) {
            for (long count : negative.counts) {
                streamOutput.writeVLong(count);
            }
        }
        if (numPositiveCounts > 0) {
            for (long count : positive.counts) {
                streamOutput.writeVLong(count);
            }
        }

        BytesRef docValue = streamOutput.bytes().toBytesRef();
        return new BinaryDocValuesField(name(), docValue);
    }

    private static class ExponentialHistogramBuckets {
        int offset;
        ArrayList<Long> counts;

        ExponentialHistogramBuckets(int offset, ArrayList<Long> counts) {
            this.offset = offset;
            this.counts = counts;
        }

        static ExponentialHistogramBuckets parse(XContentParser parser) throws Exception {
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                return null;
            }

            int offset = 0;
            ArrayList<Long> counts = null;

            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
            token = parser.nextToken();
            while (token != XContentParser.Token.END_OBJECT) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
                String fieldName = parser.currentName();
                if (fieldName.equals(OFFSET_FIELD.getPreferredName())) {
                    token = parser.nextToken();
                    ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                    offset = parser.intValue();
                } else if (fieldName.equals(COUNTS_FIELD.getPreferredName())) {
                    token = parser.nextToken();
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
                    counts = new ArrayList<>();
                    token = parser.nextToken();
                    while (token != XContentParser.Token.END_ARRAY) {
                        ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                        long count = parser.longValue();
                        if (count < 0) {
                            throw new DocumentParsingException(
                                parser.getTokenLocation(),
                                "error parsing exponential histogram buckets, counts must be >= 0 but got " + count
                            );
                        }
                        counts.add(count);
                        token = parser.nextToken();
                    }
                } else {
                    throw new DocumentParsingException(
                        parser.getTokenLocation(),
                        "error parsing exponential histogram buckets, with unknown parameter [" + fieldName + "]"
                    );
                }
                token = parser.nextToken();
            }

            if (counts == null) {
                throw new DocumentParsingException(
                    parser.getTokenLocation(),
                    "error parsing exponential histogram buckets, expected field called [" + COUNTS_FIELD.getPreferredName() + "]"
                );
            }

            return new ExponentialHistogramBuckets(offset, counts);
        }
    }

    /*
    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> mappers = new ArrayList<>();
        Iterator<Mapper> m = super.iterator();
        while (m.hasNext()) {
            mappers.add(m.next());
        }
        mappers.add(countFieldMapper);
        return mappers.iterator();
    }
     */

    /*
     * ExponentialHistogramValue is an implementation of HistogramValue that
     * iterates over values recorded in an exponential histogram field.
     */
    private static class ExponentialHistogramValue extends HistogramValue {
        final ByteArrayStreamInput streamInput;
        int scale;
        double scaleBase;
        int numNegativeCounts;
        int numPositiveCounts;
        int negativeOffset;
        int positiveOffset;

        // Current iterator position. This differs from index in that
        // it does not take the offsets into account, and can be used
        // to identify the position across both negative and positive
        // ranges.
        int iterator;

        // Current bucket index, after taking offset into account.
        int index;

        // Current bucket count.
        long count;

        ExponentialHistogramValue() {
            streamInput = new ByteArrayStreamInput();
            numNegativeCounts = 0;
            numPositiveCounts = 0;
            iterator = -1;
        }

        void reset(BytesRef bytesRef) throws IOException {
            streamInput.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            scale = streamInput.readVInt();
            scaleBase = Math.pow(2, Math.pow(2, -scale));

            numNegativeCounts = streamInput.readVInt();
            numPositiveCounts = streamInput.readVInt();
            negativeOffset = numNegativeCounts > 0 ? streamInput.readVInt() : 0;
            positiveOffset = numPositiveCounts > 0 ? streamInput.readVInt() : 0;
            iterator = -1;
        }

        private boolean done() {
            return iterator+1 == numNegativeCounts+numPositiveCounts;
        }

        @Override
        public boolean next() throws IOException {
            if (done()) {
                return false;
            }
            iterator++;
            count = streamInput.readVLong();
            if (iterator < numNegativeCounts) {
                index = negativeOffset + iterator;
            } else {
                index = positiveOffset + iterator - numNegativeCounts;
            }
            return true;
        }

        @Override
        public double value() {
            if (done()) {
                throw new IllegalArgumentException("histogram already exhausted");
            }

            /*
             * From https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram:
             *     The ExponentialHistogram bucket identified by index, a signed integer,
             *     represents values in the population that are greater than base**index
             *     and less than or equal to base**(index+1).
             */
            final double value = Math.pow(scaleBase, index+1);
            return value;
        }

        @Override
        public long count() {
            if (done()) {
                throw new IllegalArgumentException("histogram already exhausted");
            }
            return count;
        }
    }
}
