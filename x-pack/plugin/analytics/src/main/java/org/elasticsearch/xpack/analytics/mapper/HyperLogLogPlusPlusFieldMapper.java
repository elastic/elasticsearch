/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.mapper;

import com.carrotsearch.hppc.ByteArrayList;
import com.carrotsearch.hppc.IntArrayList;
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
import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.metrics.AbstractLinearCounting;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;
import org.elasticsearch.xpack.analytics.mapper.fielddata.HyperLogLogPlusPlusValue;
import org.elasticsearch.xpack.analytics.mapper.fielddata.HyperLogLogPlusPlusValues;
import org.elasticsearch.xpack.analytics.mapper.fielddata.IndexHyperLogLogPlusPlusFieldData;
import org.elasticsearch.xpack.analytics.mapper.fielddata.LeafHyperLogLogPlusPlusFieldData;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Field Mapper for pre-aggregated HyperLogLogPlusPlus sketches.
 */
public class HyperLogLogPlusPlusFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "hll++";

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

    public static final ParseField HLL_FIELD = new ParseField("hll");
    public static final ParseField LC_FIELD = new ParseField("lc");
    public static final ParseField MURMUR3_FIELD = new ParseField("murmur3");

    public static class Builder extends FieldMapper.Builder {
        protected Boolean ignoreMalformed;
        protected Integer precision;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return this;
        }

        public Builder precision(int precision) {
            if (precision < AbstractHyperLogLogPlusPlus.MIN_PRECISION) {
                throw new IllegalArgumentException("precision must be >= " + AbstractHyperLogLogPlusPlus.MIN_PRECISION);
            }
            if (precision > AbstractHyperLogLogPlusPlus.MAX_PRECISION) {
                throw new IllegalArgumentException("precision must be <= " + AbstractHyperLogLogPlusPlus.MAX_PRECISION);
            }
            this.precision = precision;
            return this;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return HyperLogLogPlusPlusFieldMapper.Defaults.IGNORE_MALFORMED;
        }

        protected Explicit<Integer> precision() {
            if (precision != null) {
                return new Explicit<>(precision, true);
            }
            return HyperLogLogPlusPlusFieldMapper.Defaults.PRECISION;
        }

        @Override
        public HyperLogLogPlusPlusFieldMapper build(BuilderContext context) {
            Explicit<Boolean> ignoreMalformed = ignoreMalformed(context);
            final HyperLogLogPlusPlusFieldType mappedFieldType
                = new HyperLogLogPlusPlusFieldType(buildFullName(context),
                hasDocValues, precision().value(), ignoreMalformed.value(), meta);
            return new HyperLogLogPlusPlusFieldMapper(name, fieldType, mappedFieldType, multiFieldsBuilder.build(this, context),
                ignoreMalformed, precision(), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            Builder builder = new HyperLogLogPlusPlusFieldMapper.Builder(name);
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

    public HyperLogLogPlusPlusFieldMapper(String simpleName, FieldType fieldType, MappedFieldType mappedFieldType,
                                          MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                          Explicit<Integer> precision, CopyTo copyTo) {
        super(simpleName, fieldType, mappedFieldType, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
        this.precision = precision;
        this.m = 1 << precision.value();
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        HyperLogLogPlusPlusFieldMapper gpfmMergeWith = (HyperLogLogPlusPlusFieldMapper) other;
        if (gpfmMergeWith.ignoreMalformed.explicit()) {
            this.ignoreMalformed = gpfmMergeWith.ignoreMalformed;
        }
        if (precision.value() != gpfmMergeWith.precision.value()) {
            conflicts.add("mapper [" + name() + "] has different [" + Names.PRECISION + "]");
        } else if (precision.explicit() == false && gpfmMergeWith.precision.explicit()) {
            this.precision = gpfmMergeWith.precision;
            this.m = 1 << precision.value();
        }
    }

    public boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    public static class HyperLogLogPlusPlusFieldType extends MappedFieldType {

        private final int precision;
        private final boolean ignoreMalformed;

        public HyperLogLogPlusPlusFieldType(String name, boolean hasDocValues, int precision,
                                            boolean ignoreMalformed, Map<String, String> meta) {
            super(name, false, false, hasDocValues, TextSearchInfo.NONE, meta);
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
            return (cache, breakerService) ->
                new IndexHyperLogLogPlusPlusFieldData(name(), AnalyticsValuesSourceType.HYPERLOGLOGPLUSPLUS) {

                @Override
                public LeafHyperLogLogPlusPlusFieldData load(LeafReaderContext context) {

                    return new LeafHyperLogLogPlusPlusFieldData() {
                        @Override
                        public HyperLogLogPlusPlusValues getHllValues() throws IOException {
                            try {
                                final BinaryDocValues values = DocValues.getBinary(context.reader(), fieldName);
                                final ByteArrayDataInput dataInput = new ByteArrayDataInput();
                                final HyperLogLogPlusPlusDocValuesBuilder builder = new HyperLogLogPlusPlusDocValuesBuilder(precision);
                                return new HyperLogLogPlusPlusValues() {

                                    @Override
                                    public boolean advanceExact(int doc) throws IOException {
                                        return values.advanceExact(doc);
                                    }

                                    @Override
                                    public HyperLogLogPlusPlusValue hllValue() throws IOException {
                                        try {
                                            BytesRef bytesRef = values.binaryValue();
                                            dataInput.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                                            return builder.decode(dataInput);
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
                public LeafHyperLogLogPlusPlusFieldData loadDirect(LeafReaderContext context) {
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
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup searchLookup, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            return new SourceValueFetcher(name(), mapperService, null) {
                @Override
                protected Object parseSourceValue(Object value) {
                    return value;
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
            throw new IllegalArgumentException("[" + CONTENT_TYPE + "] field do not support searching, " +
                "use dedicated aggregations instead: [" + name() + "]");
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            HyperLogLogPlusPlusFieldType that = (HyperLogLogPlusPlusFieldType) o;
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
        if (context.doc().getByKey(fieldType().name()) != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() +
                "] doesn't not support indexing multiple values for the same field in the same document");
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
            IntArrayList hashes = null;
            // should be an object
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, context.parser());
            subParser = new XContentSubParser(context.parser());
            token = subParser.nextToken();
            while (token != XContentParser.Token.END_OBJECT) {
                // should be a field
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, subParser);
                String fieldName = subParser.currentName();
                if (fieldName.equals(HLL_FIELD.getPreferredName())) {
                    maybeThrowErrorDuplicateFields(runLens, hashes);
                    runLens = parseHLLSketch(subParser);
                } else if (fieldName.equals(LC_FIELD.getPreferredName())) {
                    maybeThrowErrorDuplicateFields(runLens, hashes);
                    hashes = parseLCSketch(subParser);
                } else if (fieldName.equals(MURMUR3_FIELD.getPreferredName())) {
                    maybeThrowErrorDuplicateFields(runLens, hashes);
                    hashes = parseMurmur3(subParser);
                } else {
                    throw new MapperParsingException("error parsing field [" +
                        name() + "], with unknown parameter [" + fieldName + "]");
                }
                token = subParser.nextToken();
            }
            if (runLens == null && hashes == null) {
                throw new MapperParsingException("error parsing field [" + name() + "], expected field called either " +
                    "[" + HLL_FIELD.getPreferredName() + "], [" + LC_FIELD + "] or [" + MURMUR3_FIELD + "]");
            }
            if (runLens != null  && runLens.size() != m) {
                throw new MapperParsingException("error parsing field ["
                    + name() + "], expected length from [" + HLL_FIELD.getPreferredName() +"] is " + m + ""
                    + ", got [" + runLens.size() + "]");
            }
            if (fieldType().hasDocValues()) {
                final ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
                if (runLens != null) {
                    HyperLogLogPlusPlusDocValuesBuilder.writeHLL(runLens, dataOutput);
                } else {
                    HyperLogLogPlusPlusDocValuesBuilder.writeLC(hashes, dataOutput);
                }
                final BytesRef docValue = new BytesRef(dataOutput.toArrayCopy(), 0, Math.toIntExact(dataOutput.size()));
                final Field field = new BinaryDocValuesField(name(), docValue);
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

    private void maybeThrowErrorDuplicateFields(ByteArrayList runLens, IntArrayList hashes) {
        if (runLens != null || hashes != null) {
            throw new MapperParsingException("error parsing field [" + name() + "], expected only one field from "
                + "[" + HLL_FIELD.getPreferredName() + "], [" + LC_FIELD + "] and [" + MURMUR3_FIELD + "]");
        }
    }

    private ByteArrayList parseHLLSketch(XContentSubParser subParser) throws IOException {
        XContentParser.Token token = subParser.nextToken();
        // should be an array
        ensureExpectedToken(XContentParser.Token.START_ARRAY, token, subParser);
        final ByteArrayList runLens = new ByteArrayList(m);
        token = subParser.nextToken();
        while (token != XContentParser.Token.END_ARRAY) {
            // should be a number
            ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser);
            final int runLen = subParser.intValue();
            if (runLen < 0) {
                throw new MapperParsingException("error parsing field ["
                    + name() + "], ["+ HLL_FIELD + "] elements must be >= 0 but got " + runLen);
            }
            if (runLen > Byte.MAX_VALUE) { // is that correct? what is the real max value for a runLen?
                throw new MapperParsingException("error parsing field ["
                    + name() + "], ["+ HLL_FIELD + "] elements must be <= " + Byte.MAX_VALUE + " but got " + runLen);
            }
            runLens.add((byte) runLen);
            token = subParser.nextToken();
        }
        return runLens;
    }

    private IntArrayList parseLCSketch(XContentSubParser subParser) throws IOException {
        XContentParser.Token token = subParser.nextToken();
        // should be an array
        ensureExpectedToken(XContentParser.Token.START_ARRAY, token, subParser);
        final IntArrayList hashes = new IntArrayList();
        token = subParser.nextToken();
        while (token != XContentParser.Token.END_ARRAY) {
            // should be a number
            ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser);
            final int encodedHash = subParser.intValue();
            if (encodedHash == 0) {
                throw new MapperParsingException("error parsing field ["
                    + name() + "], ["+ LC_FIELD + "] cannot be 0");
            }
            final int register = AbstractHyperLogLog.decodeIndex(encodedHash, precision.value());
            final int runLen = AbstractHyperLogLog.decodeRunLen(encodedHash, precision.value());
            if (register < 0 || register >= m || runLen < 0 || runLen > Byte.MAX_VALUE) {
                throw new MapperParsingException("error parsing field ["
                    + name() + "], ["+ LC_FIELD + "] value is invalid for [" + encodedHash + "]");
            }
            hashes.add(encodedHash);
            token = subParser.nextToken();
        }
        return hashes;
    }

    private IntArrayList parseMurmur3(XContentSubParser subParser) throws IOException {
        XContentParser.Token token = subParser.nextToken();
        // should be an array
        ensureExpectedToken(XContentParser.Token.START_ARRAY, token, subParser);
        final IntArrayList hashes = new IntArrayList();
        token = subParser.nextToken();
        while (token != XContentParser.Token.END_ARRAY) {
            // should be a number
            ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser);
            final long hash = subParser.longValue();
            hashes.add(AbstractLinearCounting.encodeHash(hash, precision.value()));
            token = subParser.nextToken();
        }
        return hashes;
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

}
