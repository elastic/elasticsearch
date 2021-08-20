/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.mapper;


import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.IntArrayList;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentSubParser;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexHistogramFieldData;
import org.elasticsearch.index.fielddata.LeafHistogramFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Field Mapper for pre-aggregated histograms.
 */
public class HistogramFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "histogram";

    public static final ParseField COUNTS_FIELD = new ParseField("counts");
    public static final ParseField VALUES_FIELD = new ParseField("values");

    private static HistogramFieldMapper toType(FieldMapper in) {
        return (HistogramFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<Explicit<Boolean>> ignoreMalformed;

        /**
         * Parameter that marks this field as a time series metric defining its time series metric type.
         * For {@link HistogramFieldMapper} fields only the histogram metric type is supported.
         */
        private final Parameter<String> metric = TimeSeriesParams.metricParam(
            m -> toType(m).metricType != null ? toType(m).metricType.name() : null,
            null, // This field type always stores its subfields as doc_values
            null,
            TimeSeriesParams.MetricType.histogram.name()
        );

        public Builder(String name, boolean ignoreMalformedByDefault) {
            super(name);
            this.ignoreMalformed
                = Parameter.explicitBoolParam("ignore_malformed", true, m -> toType(m).ignoreMalformed, ignoreMalformedByDefault);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(ignoreMalformed, meta, metric);
        }

        @Override
        public HistogramFieldMapper build(ContentPath contentPath) {
            return new HistogramFieldMapper(
                name,
                new HistogramFieldType(
                    buildFullName(contentPath),
                    meta.getValue(),
                    TimeSeriesParams.MetricType.fromString(metric.getValue())
                ),
                multiFieldsBuilder.build(this, contentPath),
                copyTo.build(),
                this
            );
        }
    }

    public static final TypeParser PARSER
        = new TypeParser((n, c) -> new Builder(n, IGNORE_MALFORMED_SETTING.get(c.getSettings())), notInMultiFields(CONTENT_TYPE));

    private final Explicit<Boolean> ignoreMalformed;
    private final boolean ignoreMalformedByDefault;

    /** The metric type (gauge, counter, summary) if  field is a time series metric */
    private final TimeSeriesParams.MetricType metricType;

    public HistogramFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                                MultiFields multiFields, CopyTo copyTo, Builder builder) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.ignoreMalformedByDefault = builder.ignoreMalformed.getDefaultValue().value();
        this.metricType = TimeSeriesParams.MetricType.fromString(builder.metric.getValue());
    }

    boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), ignoreMalformedByDefault).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    public static class HistogramFieldType extends MappedFieldType {

        private final TimeSeriesParams.MetricType metricType;

        public HistogramFieldType(String name, Map<String, String> meta, TimeSeriesParams.MetricType metricType) {
            super(name, false, false, true, TextSearchInfo.NONE, meta);
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
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return (cache, breakerService) -> new IndexHistogramFieldData(name(), AnalyticsValuesSourceType.HISTOGRAM) {

                @Override
                public LeafHistogramFieldData load(LeafReaderContext context) {
                    return new LeafHistogramFieldData() {
                        @Override
                        public HistogramValues getHistogramValues() throws IOException {
                            try {
                                final BinaryDocValues values = DocValues.getBinary(context.reader(), fieldName);
                                final InternalHistogramValue value = new InternalHistogramValue();
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
                        public ScriptDocValues<?> getScriptValues() {
                            throw new UnsupportedOperationException("The [" + CONTENT_TYPE + "] field does not " +
                                "support scripts");
                        }

                        @Override
                        public SortedBinaryDocValues getBytesValues() {
                            throw new UnsupportedOperationException("String representation of doc values " +
                                "for [" + CONTENT_TYPE + "] fields is not supported");
                        }

                        @Override
                        public long ramBytesUsed() {
                            return 0; // Unknown
                        }

                        @Override
                        public void close() {

                        }
                    };
                }

                @Override
                public LeafHistogramFieldData loadDirect(LeafReaderContext context) {
                    return load(context);
                }

                @Override
                public SortField sortField(Object missingValue, MultiValueMode sortMode,
                                           Nested nested, boolean reverse) {
                    throw new UnsupportedOperationException("can't sort on the [" + CONTENT_TYPE + "] field");
                }

                @Override
                public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode,
                        Nested nested, SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
                    throw new IllegalArgumentException("can't sort on the [" + CONTENT_TYPE + "] field");
                }
            };
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("[" + CONTENT_TYPE + "] field do not support searching, " +
                "use dedicated aggregations instead: [" + name() + "]");
        }

        /**
         * If field is a time series metric field, returns its metric type
         * @return the metric type or null
         */
        public TimeSeriesParams.MetricType getMetricType() {
            return metricType;
        }
    }

    @Override
    public void parse(DocumentParserContext context) throws IOException {
        context.path().add(simpleName());
        XContentParser.Token token;
        XContentSubParser subParser = null;
        try {
            token = context.parser().currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                context.path().remove();
                return;
            }
            DoubleArrayList values = null;
            IntArrayList counts = null;
            // should be an object
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, context.parser());
            subParser = new XContentSubParser(context.parser());
            token = subParser.nextToken();
            while (token != XContentParser.Token.END_OBJECT) {
                // should be an field
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, subParser);
                String fieldName = subParser.currentName();
                if (fieldName.equals(VALUES_FIELD.getPreferredName())) {
                    token = subParser.nextToken();
                    // should be an array
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, token, subParser);
                    values = new DoubleArrayList();
                    token = subParser.nextToken();
                    double previousVal = -Double.MAX_VALUE;
                    while (token != XContentParser.Token.END_ARRAY) {
                        // should be a number
                        ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser);
                        double val = subParser.doubleValue();
                        if (val < previousVal) {
                            // values must be in increasing order
                            throw new MapperParsingException("error parsing field ["
                                + name() + "], ["+ VALUES_FIELD + "] values must be in increasing order, got [" + val +
                                "] but previous value was [" + previousVal +"]");
                        }
                        values.add(val);
                        previousVal = val;
                        token = subParser.nextToken();
                    }
                } else if (fieldName.equals(COUNTS_FIELD.getPreferredName())) {
                    token = subParser.nextToken();
                    // should be an array
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, token, subParser);
                    counts = new IntArrayList();
                    token = subParser.nextToken();
                    while (token != XContentParser.Token.END_ARRAY) {
                        // should be a number
                        ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser);
                        counts.add(subParser.intValue());
                        token = subParser.nextToken();
                    }
                } else {
                    throw new MapperParsingException("error parsing field [" +
                        name() + "], with unknown parameter [" + fieldName + "]");
                }
                token = subParser.nextToken();
            }
            if (values == null) {
                throw new MapperParsingException("error parsing field ["
                    + name() + "], expected field called [" + VALUES_FIELD.getPreferredName() + "]");
            }
            if (counts == null) {
                throw new MapperParsingException("error parsing field ["
                    + name() + "], expected field called [" + COUNTS_FIELD.getPreferredName() + "]");
            }
            if (values.size() != counts.size()) {
                throw new MapperParsingException("error parsing field ["
                    + name() + "], expected same length from [" + VALUES_FIELD.getPreferredName() +"] and " +
                    "[" + COUNTS_FIELD.getPreferredName() +"] but got [" + values.size() + " != " + counts.size() +"]");
            }
            ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
            for (int i = 0; i < values.size(); i++) {
                int count = counts.get(i);
                if (count < 0) {
                    throw new MapperParsingException("error parsing field ["
                        + name() + "], ["+ COUNTS_FIELD + "] elements must be >= 0 but got " + counts.get(i));
                } else if (count > 0) {
                    // we do not add elements with count == 0
                    dataOutput.writeVInt(count);
                    dataOutput.writeLong(Double.doubleToRawLongBits(values.get(i)));
                }
            }
            BytesRef docValue = new BytesRef(dataOutput.toArrayCopy(), 0, Math.toIntExact(dataOutput.size()));
            Field field = new BinaryDocValuesField(name(), docValue);
            if (context.doc().getByKey(fieldType().name()) != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() +
                    "] doesn't not support indexing multiple values for the same field in the same document");
            }
            context.doc().addWithKey(fieldType().name(), field);

        } catch (Exception ex) {
            if (ignoreMalformed.value() == false) {
                throw new MapperParsingException("failed to parse field [{}] of type [{}]",
                    ex, fieldType().name(), fieldType().typeName());
            }

            if (subParser != null) {
                // close the subParser so we advance to the end of the object
                subParser.close();
            }
            context.addIgnoredField(fieldType().name());
        }
        context.path().remove();
    }

    /** re-usable {@link HistogramValue} implementation */
    private static class InternalHistogramValue extends HistogramValue {
        double value;
        int count;
        boolean isExhausted;
        ByteArrayDataInput dataInput;

        InternalHistogramValue() {
            dataInput = new ByteArrayDataInput();
        }

        /** reset the value for the histogram */
        void reset(BytesRef bytesRef) {
            dataInput.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            isExhausted = false;
            value = 0;
            count = 0;
        }

        @Override
        public boolean next() {
            if (dataInput.eof() == false) {
                count = dataInput.readVInt();
                value = Double.longBitsToDouble(dataInput.readLong());
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
        public int count() {
            if (isExhausted) {
                throw new IllegalArgumentException("histogram already exhausted");
            }
            return count;
        }
    }
}
