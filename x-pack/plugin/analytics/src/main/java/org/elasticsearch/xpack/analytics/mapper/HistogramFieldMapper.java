/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexHistogramFieldData;
import org.elasticsearch.index.fielddata.LeafHistogramFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SourceLoader;
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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentSubParser;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Stream;

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

        public Builder(String name, boolean ignoreMalformedByDefault) {
            super(name);
            this.ignoreMalformed = Parameter.explicitBoolParam(
                "ignore_malformed",
                true,
                m -> toType(m).ignoreMalformed,
                ignoreMalformedByDefault
            );
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { ignoreMalformed, meta };
        }

        @Override
        public HistogramFieldMapper build(MapperBuilderContext context) {
            return new HistogramFieldMapper(
                name,
                new HistogramFieldType(context.buildFullName(name), meta.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
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

    public HistogramFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
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
        return new Builder(simpleName(), ignoreMalformedByDefault).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    public static class HistogramFieldType extends MappedFieldType {

        public HistogramFieldType(String name, Map<String, String> meta) {
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
                        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
                            throw new UnsupportedOperationException("The [" + CONTENT_TYPE + "] field does not " + "support scripts");
                        }

                        @Override
                        public FormattedDocValues getFormattedValues(DocValueFormat format) {
                            try {
                                final BinaryDocValues values = DocValues.getBinary(context.reader(), fieldName);
                                final InternalHistogramValue value = new InternalHistogramValue();
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

                        }
                    };
                }

                @Override
                public LeafHistogramFieldData loadDirect(LeafReaderContext context) {
                    return load(context);
                }

                @Override
                public SortField sortField(Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
                    throw new UnsupportedOperationException("can't sort on the [" + CONTENT_TYPE + "] field");
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
            ArrayList<Double> values = null;
            ArrayList<Integer> counts = null;
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
                    values = new ArrayList<>();
                    token = subParser.nextToken();
                    double previousVal = -Double.MAX_VALUE;
                    while (token != XContentParser.Token.END_ARRAY) {
                        // should be a number
                        ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser);
                        double val = subParser.doubleValue();
                        if (val < previousVal) {
                            // values must be in increasing order
                            throw new DocumentParsingException(
                                subParser.getTokenLocation(),
                                "error parsing field ["
                                    + name()
                                    + "], ["
                                    + VALUES_FIELD
                                    + "] values must be in increasing order, got ["
                                    + val
                                    + "] but previous value was ["
                                    + previousVal
                                    + "]"
                            );
                        }
                        values.add(val);
                        previousVal = val;
                        token = subParser.nextToken();
                    }
                } else if (fieldName.equals(COUNTS_FIELD.getPreferredName())) {
                    token = subParser.nextToken();
                    // should be an array
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, token, subParser);
                    counts = new ArrayList<>();
                    token = subParser.nextToken();
                    while (token != XContentParser.Token.END_ARRAY) {
                        // should be a number
                        ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser);
                        counts.add(subParser.intValue());
                        token = subParser.nextToken();
                    }
                } else {
                    throw new DocumentParsingException(
                        subParser.getTokenLocation(),
                        "error parsing field [" + name() + "], with unknown parameter [" + fieldName + "]"
                    );
                }
                token = subParser.nextToken();
            }
            if (values == null) {
                throw new DocumentParsingException(
                    subParser.getTokenLocation(),
                    "error parsing field [" + name() + "], expected field called [" + VALUES_FIELD.getPreferredName() + "]"
                );
            }
            if (counts == null) {
                throw new DocumentParsingException(
                    subParser.getTokenLocation(),
                    "error parsing field [" + name() + "], expected field called [" + COUNTS_FIELD.getPreferredName() + "]"
                );
            }
            if (values.size() != counts.size()) {
                throw new DocumentParsingException(
                    subParser.getTokenLocation(),
                    "error parsing field ["
                        + name()
                        + "], expected same length from ["
                        + VALUES_FIELD.getPreferredName()
                        + "] and "
                        + "["
                        + COUNTS_FIELD.getPreferredName()
                        + "] but got ["
                        + values.size()
                        + " != "
                        + counts.size()
                        + "]"
                );
            }
            BytesStreamOutput streamOutput = new BytesStreamOutput();
            for (int i = 0; i < values.size(); i++) {
                int count = counts.get(i);
                if (count < 0) {
                    throw new DocumentParsingException(
                        subParser.getTokenLocation(),
                        "error parsing field [" + name() + "], [" + COUNTS_FIELD + "] elements must be >= 0 but got " + counts.get(i)
                    );
                } else if (count > 0) {
                    // we do not add elements with count == 0
                    streamOutput.writeVInt(count);
                    streamOutput.writeLong(Double.doubleToRawLongBits(values.get(i)));
                }
            }
            BytesRef docValue = streamOutput.bytes().toBytesRef();
            Field field = new BinaryDocValuesField(name(), docValue);
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
        final ByteArrayStreamInput streamInput;

        InternalHistogramValue() {
            streamInput = new ByteArrayStreamInput();
        }

        /** reset the value for the histogram */
        void reset(BytesRef bytesRef) {
            streamInput.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            isExhausted = false;
            value = 0;
            count = 0;
        }

        @Override
        public boolean next() throws IOException {
            if (streamInput.available() > 0) {
                count = streamInput.readVInt();
                value = Double.longBitsToDouble(streamInput.readLong());
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

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        if (ignoreMalformed.value()) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [histogram] doesn't support synthetic source because it ignores malformed histograms"
            );
        }
        if (copyTo.copyToFields().isEmpty() != true) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [histogram] doesn't support synthetic source because it declares copy_to"
            );
        }
        return new SourceLoader.SyntheticFieldLoader() {
            private final InternalHistogramValue value = new InternalHistogramValue();
            private BytesRef binaryValue;

            @Override
            public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
                return Stream.of();
            }

            @Override
            public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
                BinaryDocValues docValues = leafReader.getBinaryDocValues(fieldType().name());
                if (docValues == null) {
                    // No values in this leaf
                    binaryValue = null;
                    return null;
                }
                return docId -> {
                    if (docValues.advanceExact(docId)) {
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
                b.startObject(simpleName());

                value.reset(binaryValue);
                b.startArray("values");
                while (value.next()) {
                    b.value(value.value());
                }
                b.endArray();

                value.reset(binaryValue);
                b.startArray("counts");
                while (value.next()) {
                    b.value(value.count());
                }
                b.endArray();

                b.endObject();
            }
        };
    }
}
