/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.mapper;


import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentSubParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AtomicHistogramFieldData;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexHistogramFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TypeParsers;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Field Mapper for pre-aggregated histograms.
 */
public class HistogramFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "histogram";

    public static class Names {
        public static final String IGNORE_MALFORMED = "ignore_malformed";
    }

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final HistogramFieldType FIELD_TYPE = new HistogramFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.freeze();
        }
    }

    public static final ParseField COUNTS_FIELD = new ParseField("counts");
    public static final ParseField VALUES_FIELD = new ParseField("values");

    public static class Builder extends FieldMapper.Builder<Builder, HistogramFieldMapper> {
        protected Boolean ignoreMalformed;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return builder;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return HistogramFieldMapper.Defaults.IGNORE_MALFORMED;
        }

        public HistogramFieldMapper build(BuilderContext context, String simpleName, MappedFieldType fieldType,
                                          MappedFieldType defaultFieldType, Settings indexSettings,
                                          MultiFields multiFields, Explicit<Boolean> ignoreMalformed, CopyTo copyTo) {
            setupFieldType(context);
            return new HistogramFieldMapper(simpleName, fieldType, defaultFieldType, indexSettings, multiFields,
                ignoreMalformed, copyTo);
        }

        @Override
        public HistogramFieldMapper build(BuilderContext context) {
            return build(context, name, fieldType, defaultFieldType, context.indexSettings(),
                multiFieldsBuilder.build(this, context), ignoreMalformed(context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<Builder, HistogramFieldMapper> parse(String name,
                                                                   Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            Builder builder = new HistogramFieldMapper.Builder(name);
            TypeParsers.parseMeta(builder, name, node);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals(Names.IGNORE_MALFORMED)) {
                    builder.ignoreMalformed(XContentMapValues.nodeBooleanValue(propNode, name + "." + Names.IGNORE_MALFORMED));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    protected Explicit<Boolean> ignoreMalformed;

    public HistogramFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                Settings indexSettings, MultiFields multiFields, Explicit<Boolean> ignoreMalformed, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);
        HistogramFieldMapper gpfmMergeWith = (HistogramFieldMapper) mergeWith;
        if (gpfmMergeWith.ignoreMalformed.explicit()) {
            this.ignoreMalformed = gpfmMergeWith.ignoreMalformed;
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    public static class HistogramFieldType extends MappedFieldType {

        public HistogramFieldType() {
        }

        HistogramFieldType(HistogramFieldType ref) {
            super(ref);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public MappedFieldType clone() {
            return new HistogramFieldType(this);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new IndexFieldData.Builder() {

                @Override
                public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                                               CircuitBreakerService breakerService, MapperService mapperService) {

                    return new IndexHistogramFieldData(indexSettings.getIndex(), fieldType.name()) {

                        @Override
                        public AtomicHistogramFieldData load(LeafReaderContext context) {
                            return new AtomicHistogramFieldData() {
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
                        public AtomicHistogramFieldData loadDirect(LeafReaderContext context) throws Exception {
                            return load(context);
                        }

                        @Override
                        public SortField sortField(Object missingValue, MultiValueMode sortMode,
                                                   XFieldComparatorSource.Nested nested, boolean reverse) {
                            throw new UnsupportedOperationException("can't sort on the [" + CONTENT_TYPE + "] field");
                        }
                    };
                }
            };
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                throw new QueryShardException(context, "field  " + name() + " of type [" + CONTENT_TYPE + "] " +
                    "has no doc values and cannot be searched");
            }
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "[" + CONTENT_TYPE + "] field do not support searching, " +
                "use dedicated aggregations instead: ["
                + name() + "]");
        }
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        if (context.externalValueSet()) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] can't be used in multi-fields");
        }
        context.path().add(simpleName());
        XContentParser.Token token = null;
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
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, context.parser()::getTokenLocation);
            subParser = new XContentSubParser(context.parser());
            token = subParser.nextToken();
            while (token != XContentParser.Token.END_OBJECT) {
                // should be an field
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, subParser::getTokenLocation);
                String fieldName = subParser.currentName();
                if (fieldName.equals(VALUES_FIELD.getPreferredName())) {
                    token = subParser.nextToken();
                    // should be an array
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, token, subParser::getTokenLocation);
                    values = new DoubleArrayList();
                    token = subParser.nextToken();
                    double previousVal = -Double.MAX_VALUE;
                    while (token != XContentParser.Token.END_ARRAY) {
                        // should be a number
                        ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser::getTokenLocation);
                        double val = subParser.doubleValue();
                        if (val < previousVal) {
                            // values must be in increasing order
                            throw new MapperParsingException("error parsing field ["
                                + name() + "], ["+ COUNTS_FIELD + "] values must be in increasing order, got [" + val +
                                "] but previous value was [" + previousVal +"]");
                        }
                        values.add(val);
                        previousVal = val;
                        token = subParser.nextToken();
                    }
                } else if (fieldName.equals(COUNTS_FIELD.getPreferredName())) {
                    token = subParser.nextToken();
                    // should be an array
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, token, subParser::getTokenLocation);
                    counts = new IntArrayList();
                    token = subParser.nextToken();
                    while (token != XContentParser.Token.END_ARRAY) {
                        // should be a number
                        ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser::getTokenLocation);
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
            if (fieldType().hasDocValues()) {
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
                Field field = new BinaryDocValuesField(simpleName(), docValue);
                if (context.doc().getByKey(fieldType().name()) != null) {
                    throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() +
                        "] doesn't not support indexing multiple values for the same field in the same document");
                }
                context.doc().addWithKey(fieldType().name(), field);
            }

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

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(Names.IGNORE_MALFORMED, ignoreMalformed.value());
        }
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
