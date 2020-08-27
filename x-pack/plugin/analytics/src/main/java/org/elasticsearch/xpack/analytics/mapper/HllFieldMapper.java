/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.mapper;

import com.carrotsearch.hppc.ByteArrayList;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentSubParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.TypeParsers;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLog;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;
import org.elasticsearch.xpack.analytics.mapper.fielddata.HllValue;
import org.elasticsearch.xpack.analytics.mapper.fielddata.HllValues;
import org.elasticsearch.xpack.analytics.mapper.fielddata.IndexHllFieldData;
import org.elasticsearch.xpack.analytics.mapper.fielddata.LeafHllFieldData;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Field Mapper for pre-aggregated HyperLogLog sketches.
 */
public class HllFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "hll";

    public static class Names {
        public static final String IGNORE_MALFORMED = "ignore_malformed";
        public static final String PRECISION = "precision";
    }

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final Explicit<Integer> PRECISION = new Explicit<>(10, false);
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.freeze();
        }
    }

    public static final ParseField SKETCH_FIELD = new ParseField("sketch");

    // compression modes for doc values
    private static final byte FIXED_LENGTH = 0;
    private static final byte RUN_LENGTH = 1;
    private static final byte CUSTOM = 2;

    public static class Builder extends FieldMapper.Builder<Builder> {
        protected Boolean ignoreMalformed;
        protected Integer precision;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return builder;
        }

        public Builder precision(int precision) {
            if (precision < 4) {
                throw new IllegalArgumentException("precision must be >= 4");
            }
            if (precision > 18) {
                throw new IllegalArgumentException("precision must be <= 18");
            }
            this.precision = precision;
            return builder;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return HllFieldMapper.Defaults.IGNORE_MALFORMED;
        }

        protected Explicit<Integer> precision() {
            if (precision != null) {
                return new Explicit<>(precision, true);
            }
            return HllFieldMapper.Defaults.PRECISION;
        }

        @Override
        public HllFieldMapper build(BuilderContext context) {
            Explicit<Boolean> ignoreMalformed = ignoreMalformed(context);
            final HllFieldType mappedFieldType
                = new HllFieldType(buildFullName(context), hasDocValues, precision().value(), ignoreMalformed.value(), meta);
            return new HllFieldMapper(name, fieldType, mappedFieldType, multiFieldsBuilder.build(this, context),
                ignoreMalformed, precision(), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<Builder> parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            Builder builder = new HllFieldMapper.Builder(name);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals(Names.IGNORE_MALFORMED)) {
                    builder.ignoreMalformed(XContentMapValues.nodeBooleanValue(propNode, name + "." + Names.IGNORE_MALFORMED));
                    iterator.remove();
                } else if (propName.equals(Names.PRECISION)) {
                    builder.precision(XContentMapValues.nodeIntegerValue(propNode, Defaults.PRECISION.value()));
                    iterator.remove();
                } else if (propName.equals("meta")) {
                    builder.meta(TypeParsers.parseMeta(propName, propNode));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    protected Explicit<Boolean> ignoreMalformed;
    protected Explicit<Integer> precision;
    private int m;

    public HllFieldMapper(String simpleName, FieldType fieldType, MappedFieldType mappedFieldType,
                          MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                          Explicit<Integer> precision, CopyTo copyTo) {
        super(simpleName, fieldType, mappedFieldType, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
        this.precision = precision;
        this.m = 1 << precision.value();
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        HllFieldMapper gpfmMergeWith = (HllFieldMapper) other;
        if (gpfmMergeWith.ignoreMalformed.explicit()) {
            this.ignoreMalformed = gpfmMergeWith.ignoreMalformed;
        }
        if (precision.value() != gpfmMergeWith.precision.value()) {
            conflicts.add("mapper [" + name() + "] has different [precision]");
        } else if (precision.explicit() == false && gpfmMergeWith.precision.explicit()) {
            this.precision = gpfmMergeWith.precision;
            this.m = 1 << precision.value();
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    @Override
    public ValueFetcher valueFetcher(MapperService mapperService, String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
        }
        return new SourceValueFetcher(name(), mapperService, parsesArrayValue()) {
            @Override
            protected Object parseSourceValue(Object value) {
                return value;
            }
        };
    }

    public static class HllFieldType extends MappedFieldType {

        private final int precision;
        private final boolean ignoreMalformed;

        public HllFieldType(String name, boolean hasDocValues, int precision, boolean ignoreMalformed, Map<String, String> meta) {
            super(name, false, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            this.precision = precision;
            this.ignoreMalformed = ignoreMalformed;
        }

        public int precision() {
            return precision;
        }

        public boolean ignoreMalformed() {
            return ignoreMalformed;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return (cache, breakerService, mapperService) ->
                new IndexHllFieldData(name(), AnalyticsValuesSourceType.CARDINALITY) {

                @Override
                public LeafHllFieldData load(LeafReaderContext context) {

                    return new LeafHllFieldData() {
                        @Override
                        public HllValues getHllValues() throws IOException {
                            try {
                                final BinaryDocValues values = DocValues.getBinary(context.reader(), fieldName);
                                final ByteArrayDataInput dataInput = new ByteArrayDataInput();
                                final InternalFixedLengthHllValue fixedValue = new InternalFixedLengthHllValue();
                                final InternalRunLenHllValue runLenValue = new InternalRunLenHllValue();
                                final InternalCustomHllValue customValue = new InternalCustomHllValue();
                                return new HllValues() {

                                    @Override
                                    public boolean advanceExact(int doc) throws IOException {
                                        return values.advanceExact(doc);
                                    }

                                    @Override
                                    public HllValue hllValue() throws IOException {
                                        try {
                                            BytesRef bytesRef = values.binaryValue();
                                            dataInput.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                                            byte mode = dataInput.readByte();
                                            switch (mode) {
                                                case FIXED_LENGTH:
                                                    fixedValue.reset(dataInput);
                                                    return fixedValue;
                                                case RUN_LENGTH:
                                                    runLenValue.reset(dataInput);
                                                    return runLenValue;
                                                case CUSTOM:
                                                    customValue.reset(dataInput);
                                                    return customValue;
                                                default:
                                                    throw new IOException("Unknown compression mode: " + mode);
                                            }
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
                public LeafHllFieldData loadDirect(LeafReaderContext context) throws Exception {
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

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            HllFieldType that = (HllFieldType) o;
            return precision == that.precision;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), precision);
        }
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        if (context.externalValueSet()) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] can't be used in multi-fields");
        }
        context.path().add(simpleName());
        XContentParser.Token token;
        XContentSubParser subParser = null;
        try {
            token = context.parser().currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                context.path().remove();
                return;
            }
            ByteArrayList runLens = null;
            // should be an object
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, context.parser()::getTokenLocation);
            subParser = new XContentSubParser(context.parser());
            token = subParser.nextToken();
            while (token != XContentParser.Token.END_OBJECT) {
                // should be a field
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, subParser::getTokenLocation);
                String fieldName = subParser.currentName();
                if (fieldName.equals(SKETCH_FIELD.getPreferredName())) {
                    token = subParser.nextToken();
                    // should be an array
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, token, subParser::getTokenLocation);
                    runLens = new ByteArrayList();
                    token = subParser.nextToken();
                    while (token != XContentParser.Token.END_ARRAY) {
                        // should be a number
                        ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser::getTokenLocation);
                        int newValue = subParser.intValue();
                        if (newValue < 0) {
                            throw new MapperParsingException("error parsing field ["
                                + name() + "], ["+ SKETCH_FIELD + "] elements must be >= 0 but got " + newValue);
                        }
                        if (newValue > Byte.MAX_VALUE) { // is that correct? what is the real max value for a runLen?
                            throw new MapperParsingException("error parsing field ["
                                + name() + "], ["+ SKETCH_FIELD + "] elements must be <= " + Byte.MAX_VALUE + " but got " + newValue);
                        }
                        runLens.add((byte) newValue);
                        token = subParser.nextToken();
                    }
                } else {
                    throw new MapperParsingException("error parsing field [" +
                        name() + "], with unknown parameter [" + fieldName + "]");
                }
                token = subParser.nextToken();
            }
            if (runLens == null) {
                throw new MapperParsingException("error parsing field ["
                    + name() + "], expected field called [" + SKETCH_FIELD.getPreferredName() + "]");
            }
            if (runLens.size() != m) {
                throw new MapperParsingException("error parsing field ["
                    + name() + "], expected length from [" + SKETCH_FIELD.getPreferredName() +"] is " + m + ""
                    + ", got [" + runLens.size() + "]");
            }
            if (fieldType().hasDocValues()) {
                final ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
                writeCompressed(runLens, dataOutput);
                final BytesRef docValue = new BytesRef(dataOutput.toArrayCopy(), 0, Math.toIntExact(dataOutput.size()));
                final Field field = new BinaryDocValuesField(name(), docValue);
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

    private void writeCompressed(ByteArrayList runLens, ByteBuffersDataOutput dataOutput) throws IOException {
        // chose the compression to use
        final int fixedLength = m;
        final int runLenLength = runLenLength(runLens);
        final int customLength = customLength(runLens);
        if (fixedLength <= runLenLength) {
            if (fixedLength <= customLength) {
                writeFixedLen(runLens, dataOutput);
                assert dataOutput.size() == fixedLength + 1;
            } else {
                writeCustom(runLens, dataOutput);
                assert dataOutput.size() == customLength + 1;
            }
        } else if (runLenLength < customLength) {
            writeRunLen(runLens, dataOutput);
            assert dataOutput.size() < fixedLength + 1;
        } else {
            writeCustom(runLens, dataOutput);
            assert dataOutput.size() == customLength + 1;
        }
    }

    private void writeFixedLen(ByteArrayList runLens, ByteBuffersDataOutput dataOutput) {
        dataOutput.writeByte(FIXED_LENGTH);
        for (int i = 0; i < m; i++) {
            dataOutput.writeByte(runLens.get(i));
        }
    }

    private int runLenLength(ByteArrayList runLens) {
        int compressionLength = 0;
        byte value = runLens.get(0);
        for (int i = 1; i < runLens.size(); i++) {
            byte nextValue = runLens.get(i);
            if (nextValue != value) {
                compressionLength += 2;
                value = nextValue;
            }
        }
        compressionLength += 2;
        return compressionLength;
    }

    private void writeRunLen(ByteArrayList runLens, ByteBuffersDataOutput dataOutput) throws IOException {
        dataOutput.writeByte(RUN_LENGTH);
        int length = 1;
        byte value = runLens.get(0);
        for (int i = 1; i < runLens.size(); i++) {
            byte nextValue = runLens.get(i);
            if (nextValue != value) {
                dataOutput.writeVInt(length);
                dataOutput.writeByte(value);
                length = 1;
                value = nextValue;
            } else {
                length++;
            }
        }
        dataOutput.writeVInt(length);
        dataOutput.writeByte(value);
    }

    private int customLength(ByteArrayList runLens) {
        int compressionLength = 0;
        byte length = 1;
        byte value = runLens.get(0);
        for (int i = 1; i < runLens.size(); i++) {
            byte nextValue = runLens.get(i);
            if (nextValue != value || length == Byte.MAX_VALUE) {
                if (length > 1) {
                    compressionLength++;
                }
                compressionLength++;
                length = 1;
                value = nextValue;
            } else {
                length++;
            }
        }
        if (length > 1) {
            compressionLength++;
        }
        compressionLength++;
        return compressionLength;
    }

    private void writeCustom(ByteArrayList runLens, ByteBuffersDataOutput dataOutput) throws IOException {
        dataOutput.writeByte(CUSTOM);
        byte length = 1;
        byte value = runLens.get(0);
        for (int i = 1; i < runLens.size(); i++) {
            byte nextValue = runLens.get(i);
            if (nextValue != value || length == Byte.MAX_VALUE) {
                if (length > 1) {
                    dataOutput.writeByte((byte) -length);
                }
                dataOutput.writeByte(value);
                length = 1;
                value = nextValue;
            } else {
                length++;
            }
        }
        if (length > 1) {
            dataOutput.writeByte((byte) -length);
        }
        dataOutput.writeByte(value);
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(Names.IGNORE_MALFORMED, ignoreMalformed.value());
        }
        if (includeDefaults || precision.explicit()) {
            builder.field(Names.PRECISION, precision.value());
        }
    }

    @Override
    protected boolean indexedByDefault() {
        return false;
    }

    /** re-usable {@link HllValue} implementation. HLL sketch is compressed as fixed length array */
    private static class InternalFixedLengthHllValue extends HllValue implements AbstractHyperLogLog.RunLenIterator {
        private byte value;
        private boolean isExhausted;
        private ByteArrayDataInput dataInput;

        InternalFixedLengthHllValue() {
        }

        /** reset the value for the HLL sketch */
        void reset(ByteArrayDataInput dataInput) {
            this.dataInput = dataInput;
            isExhausted = false;
            value = 0;
        }

        @Override
        public boolean next() {
            if (dataInput.eof() == false) {
                value = dataInput.readByte();
                return true;
            }
            isExhausted = true;
            return false;
        }

        @Override
        public byte value() {
            if (isExhausted) {
                throw new IllegalArgumentException("HyperLogLog sketch already exhausted");
            }
            return value;
        }

        @Override
        public void skip(int bytes) {
            dataInput.skipBytes(bytes);
        }
    }

    /** re-usable {@link HllValue} implementation. HLL sketch is compressed using run length compression. */
    private static class InternalRunLenHllValue extends HllValue implements AbstractHyperLogLog.RunLenIterator {
        private byte value;
        private boolean isExhausted;
        private ByteArrayDataInput dataInput;
        private int valuesInBuffer;

        InternalRunLenHllValue() {
        }

        /** reset the value for the HLL sketch */
        void reset(ByteArrayDataInput dataInput) {
            this.dataInput = dataInput;
            isExhausted = false;
            value = 0;
            valuesInBuffer = 0;
        }

        @Override
        public boolean next() {
            if (valuesInBuffer > 0) {
                valuesInBuffer--;
                return true;
            }
            if (dataInput.eof() == false) {
                valuesInBuffer = dataInput.readVInt() - 1;
                value = dataInput.readByte();
                return true;
            }
            isExhausted = true;
            return false;
        }

        @Override
        public byte value() {
            if (isExhausted) {
                throw new IllegalArgumentException("HyperLogLog sketch already exhausted");
            }
            return value;
        }

        @Override
        public void skip(int bytes) {
            if (valuesInBuffer >= bytes) {
                valuesInBuffer -= bytes;
            } else {
                int valuesLeft = valuesInBuffer;
                valuesInBuffer = 0;
                next();
                skip(bytes - valuesLeft - 1);
            }
        }
    }

    /** re-usable {@link HllValue} implementation. HLL sketch is compressed using custom compression. Negative values
     * indicate repeated values. */
    private static class InternalCustomHllValue extends HllValue implements AbstractHyperLogLog.RunLenIterator {
        private byte value;
        private boolean isExhausted;
        private ByteArrayDataInput dataInput;
        private int valuesInBuffer;

        InternalCustomHllValue() {
        }

        /** reset the value for the HLL sketch */
        void reset(ByteArrayDataInput dataInput) {
            this.dataInput = dataInput;
            isExhausted = false;
            value = 0;
            valuesInBuffer = 0;
        }

        @Override
        public boolean next() {
            if (valuesInBuffer > 0) {
                valuesInBuffer--;
                return true;
            }
            if (dataInput.eof() == false) {
                byte b = dataInput.readByte();
                if (b < 0) {
                    valuesInBuffer = -b - 1;
                    value = dataInput.readByte();
                } else {
                    value = b;
                }
                return true;
            }
            isExhausted = true;
            return false;
        }

        @Override
        public byte value() {
            if (isExhausted) {
                throw new IllegalArgumentException("HyperLogLog sketch already exhausted");
            }
            return value;
        }

        @Override
        public void skip(int bytes) {
            if (valuesInBuffer >= bytes) {
                valuesInBuffer -= bytes;
            } else {
                int valuesLeft = valuesInBuffer;
                valuesInBuffer = 0;
                next();
                skip(bytes - valuesLeft - 1);
            }
        }
    }
}
