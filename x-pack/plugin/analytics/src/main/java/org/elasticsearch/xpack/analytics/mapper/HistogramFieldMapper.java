/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.mapper;


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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AtomicHistogramFieldData;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexHistogramFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.ArrayValueMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

/**
 * Field Mapper for pre-aggregated histograms.
 *
 */
public class HistogramFieldMapper extends FieldMapper implements ArrayValueMapperParser {
    public static final String CONTENT_TYPE = "histogram";

    public static class Names {
        public static final String IGNORE_MALFORMED = "ignore_malformed";
    }

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final HDRPercentilesFieldType FIELD_TYPE = new HDRPercentilesFieldType();

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

        @Override
        public Builder store(boolean store) {
            if (store) {
                throw new IllegalArgumentException("The [" + CONTENT_TYPE + "] field does not support " +
                    "stored fields");
            }
            return super.store(false);
        }

        @Override
        public Builder index(boolean index) {
            if (index) {
                throw new IllegalArgumentException("The [" + CONTENT_TYPE + "] field does not support indexing");
            }
            return super.store(false);
        }

        @Override
        public Builder indexOptions(IndexOptions indexOptions) {
            if (indexOptions.equals(IndexOptions.NONE) == false) {
                throw new IllegalArgumentException("The [" + CONTENT_TYPE + "] field does not support " +
                    "index options, got [index_options]=" + indexOptionToString(indexOptions));
            }
            return super.indexOptions(indexOptions);
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
        public Mapper.Builder<Builder, HistogramFieldMapper> parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            Builder builder = new HistogramFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);
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

    public static class HDRPercentilesFieldType extends MappedFieldType {
        public HDRPercentilesFieldType() {
        }

        HDRPercentilesFieldType(HDRPercentilesFieldType ref) {
            super(ref);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public MappedFieldType clone() {
            return new HDRPercentilesFieldType(this);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new IndexFieldData.Builder() {

                @Override
                public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache, CircuitBreakerService breakerService, MapperService mapperService) {

                    return new IndexHistogramFieldData(indexSettings.getIndex(), fieldType.name()) {

                        @Override
                        public AtomicHistogramFieldData load(LeafReaderContext context) {
                            return new AtomicHistogramFieldData() {
                                @Override
                                public HistogramValues getHistogramValues() {
                                    try {
                                        final BinaryDocValues values = DocValues.getBinary(context.reader(), fieldName);
                                        return new HistogramValues() {
                                            @Override
                                            public boolean advanceExact(int doc) throws IOException {
                                                return values.advanceExact(doc);
                                            }

                                            @Override
                                            public HistogramValue histogram() {
                                                try {
                                                    return getHistogramValue(values.binaryValue());
                                                } catch (IOException e) {
                                                    throw new IllegalStateException("Cannot load doc value", e);
                                                }
                                            }
                                        };
                                    } catch (IOException e) {
                                        throw new IllegalStateException("Cannot load doc values", e);
                                    }

                                }

                                @Override
                                public ScriptDocValues<?> getScriptValues() {
                                    return new ScriptDocValues.Strings(getBytesValues());
                                }

                                @Override
                                public SortedBinaryDocValues getBytesValues() {
                                    try {
                                        final BinaryDocValues values = DocValues.getBinary(context.reader(), fieldName);
                                        return FieldData.singleton(values);
                                    } catch (IOException e) {
                                        throw new IllegalStateException("Cannot load doc values", e);
                                    }
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
                        public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
                            return null;
                        }
                    };
                }

                private HistogramValue getHistogramValue(final BytesRef bytesRef) throws IOException {
                    final ByteBufferStreamInput streamInput = new ByteBufferStreamInput(ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length));
                    final int numValues = streamInput.readVInt();
                    return new HistogramValue() {
                        double value;
                        int count;
                        int position;
                        boolean isExhausted;

                        @Override
                        public boolean next() throws IOException {
                            if (position < numValues) {
                                position++;
                                value = streamInput.readDouble();
                                count = streamInput.readVInt();
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
                    };
                }

            };
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                throw new QueryShardException(context, "field  " + name() + " of type [" + CONTENT_TYPE + "] has no doc values and cannot be searched");
            }
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "[" + CONTENT_TYPE + "] field do not support searching, use dedicated aggregations instead: ["
                + name() + "]");
        }
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        context.path().add(simpleName());
        try {
            List<Double> values = null;
            List<Integer> counts = null;
            XContentParser.Token token = context.parser().currentToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new MapperParsingException("error parsing field ["
                    + name() + "], expected an [" + XContentParser.Token.START_OBJECT.name() +
                    "] but got [" + token.name() + "]");
            }
            token = context.parser().nextToken();
            while (token != XContentParser.Token.END_OBJECT) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new MapperParsingException("error parsing field ["
                        + name() + "], expected a field but got " + context.parser().currentName());
                }
                String fieldName = context.parser().currentName();
                if (fieldName.equals(VALUES_FIELD.getPreferredName())) {
                    token = context.parser().nextToken();
                    //should be an array
                    if (token != XContentParser.Token.START_ARRAY) {
                        throw new MapperParsingException("error parsing field ["
                            + name() + "], expected an [" + XContentParser.Token.START_ARRAY.name() +
                            "] but got [" + token.name() + "]");
                    }
                    values = new ArrayList<>();
                    token = context.parser().nextToken();
                    while (token != XContentParser.Token.END_ARRAY) {
                        values.add(context.parser().doubleValue());
                        token = context.parser().nextToken();
                    }
                } else if (fieldName.equals(COUNTS_FIELD.getPreferredName())) {
                    token = context.parser().nextToken();
                    //should be an array
                    if (token != XContentParser.Token.START_ARRAY) {
                        throw new MapperParsingException("error parsing field ["
                            + name() + "], expected an [" + XContentParser.Token.START_ARRAY.name() +
                            "] but got [" + token.name() + "]");
                    }
                    counts = new ArrayList<>();
                    token = context.parser().nextToken();
                    while (token != XContentParser.Token.END_ARRAY) {
                        counts.add(context.parser().intValue());
                        token = context.parser().nextToken();
                    }
                } else {
                    throw new MapperParsingException("error parsing field [" +
                        name() + "], with unknown parameter [" + fieldName + "]");
                }
                token = context.parser().nextToken();
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
            if (values.size() == 0) {
                throw new MapperParsingException("error parsing field ["
                    + name() + "], arrays for values and counts cannot be empty");
            }
            if (fieldType().hasDocValues()) {
                BytesStreamOutput streamOutput = new BytesStreamOutput();
                streamOutput.writeVInt(values.size());
                for (int i = 0; i < values.size(); i++) {
                    streamOutput.writeDouble(values.get(i));
                    if (counts.get(i) < 0) {
                        throw new MapperParsingException("error parsing field ["
                            + name() + "], ["+ COUNTS_FIELD + "] elements must be >= 0 but got " + counts.get(i));
                    }
                    streamOutput.writeVInt(counts.get(i));
                }

                Field field = new BinaryDocValuesField(simpleName(), streamOutput.bytes().toBytesRef());
                streamOutput.close();
                context.doc().add(field);
            }

        } catch (Exception ex) {
            if (ignoreMalformed.value()) {
                return;
            }
            throw new MapperParsingException("failed to parse field [{}] of type [{}]", ex, fieldType().name(), fieldType().typeName());
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
}
