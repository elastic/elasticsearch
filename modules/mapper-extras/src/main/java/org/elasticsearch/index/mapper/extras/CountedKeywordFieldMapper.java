/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.common.lucene.Lucene.KEYWORD_ANALYZER;

/**
 *
 * Example:
 *
 * PUT /test
 * {
 *   "settings": {
 *     "number_of_shards": 1
 *   },
 *   "mappings": {
 *     "properties": {
 *       "profiler_stack_trace_ids": { "type": "counted_keyword" }
 *     }
 *   }
 * }
 *
 * POST test/_doc
 * {
 *   "profiler_stack_trace_ids": ["a", "a", "a", "b", "c"]
 * }
 */
public class CountedKeywordFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "counted_keyword";

    private static class CountedKeywordFieldType extends StringFieldType {

        CountedKeywordFieldType(
            String name,
            boolean isIndexed,
            boolean isStored,
            boolean hasDocValues,
            TextSearchInfo textSearchInfo,
            Map<String, String> meta
        ) {
            super(name, isIndexed, isStored, hasDocValues, textSearchInfo, meta);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            // Copied from HistogramFieldMapper
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            // TODO: Return custom field data including the count - see HistogramFieldType
            // new field data - extend AbstractIndexOrdinalsFieldData
            // next value ->
            return (cache, breakerService) -> new IndexFieldData<>() {
                @Override
                public String getFieldName() {
                    return getFieldName();
                }

                @Override
                public ValuesSourceType getValuesSourceType() {
                    return ExtendedSourceType.COUNTED_KEYWORD;
                }

                @Override
                public LeafFieldData load(LeafReaderContext context) {
                    BinaryDocValues values;
                    try {
                        values = DocValues.getBinary(context.reader(), getFieldName());
                        InternalValue value = new InternalValue();
                        value.reset(values.binaryValue());
                    } catch (IOException e) {
                        throw new UncheckedIOException("Unable to load " + CONTENT_TYPE + " doc values", e);
                    }


                    return new LeafFieldData() {
                        @Override
                        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
                            // TODO
                            return null;
                        }

                        @Override
                        public SortedBinaryDocValues getBytesValues() {
                            return new CountedKeywordSortedBinaryDocValues(values);
                        }

                        @Override
                        public long ramBytesUsed() {
                            return 0; // unknown
                        }

                        @Override
                        public void close() {

                        }
                    };
                }

                @Override
                public LeafFieldData loadDirect(LeafReaderContext context) {
                    return load(context);
                }
                /*
                @Override
                public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
                    return load(context);
                } */


                /*
                @Override
                public LeafOrdinalsFieldData load(LeafReaderContext context) {
                    try {
                        final BinaryDocValues values = DocValues.getBinary(context.reader(), getFieldName());
                    } catch (IOException e) {
                        throw new UncheckedIOException("Unable to load " + CONTENT_TYPE + " doc values", e);
                    }
                    return new LeafOrdinalsFieldData() {
                        @Override
                        public SortedSetDocValues getOrdinalsValues() {
                            return null;
                        }

                        @Override
                        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
                            return null;
                        }

                        @Override
                        public SortedBinaryDocValues getBytesValues() {
                            return null;
                        }

                        @Override
                        public long ramBytesUsed() {
                            // unknown
                            return 0;
                        }

                        @Override
                        public void close() {
                            //No op?
                        }
                    };
                }
                 */

                @Override
                public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
                    throw new UnsupportedOperationException("can't sort on the [" + CONTENT_TYPE + "] field");
                }

                @Override
                public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
                    throw new IllegalArgumentException("can't sort on the [" + CONTENT_TYPE + "] field");
                }
            };
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

    private static class CountedKeywordSortedBinaryDocValues extends SortedBinaryDocValues {
        private final BinaryDocValues values;
        private final InternalValue value;

        private BytesRef current;
        private int count;

        CountedKeywordSortedBinaryDocValues(BinaryDocValues values) {
            this.values = values;
            this.value = new InternalValue();
            try {
                this.value.reset(values.binaryValue());
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to load " + CONTENT_TYPE + " doc values", e);
            }
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            // TODO: Not sure what to do here
            return values.advanceExact(doc);
        }

        @Override
        public int docValueCount() {
            // TODO: Should we store the sum at the beginning of binary doc values?
            // sum of all counts
            return 0;
        }

        @Override
        public BytesRef nextValue() throws IOException {
            if (count == 0) {
                if (value.next() == false) {
                    throw new NoSuchElementException();
                }
                count = value.count;
                current = BytesRefs.toBytesRef(value.value);
            }
            count--;
            return current;
        }
    }

    // taken from InternalHistogramValue
    private static class InternalValue {
        String value;
        int count;
        boolean isExhausted;
        final ByteArrayStreamInput streamInput;

        InternalValue() {
            streamInput = new ByteArrayStreamInput();
        }

        void reset(BytesRef bytesRef) {
            streamInput.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            isExhausted = false;
            value = null;
            count = 0;
        }

        public boolean next() throws IOException {
            if (streamInput.available() > 0) {
                // TODO: We should store / read this as bytesref
                value = streamInput.readString();
                count = streamInput.readVInt();
                return true;
            }
            isExhausted = true;
            return false;
        }

        public String value() {
            if (isExhausted) {
                throw new IllegalArgumentException("value already exhausted");
            }
            return value;
        }

        public int count() {
            if (isExhausted) {
                throw new IllegalArgumentException("value already exhausted");
            }
            return count;
        }
    }

    public static class Builder extends FieldMapper.Builder {
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        /**
         * Creates a new Builder with a field name
         *
         * @param name
         */
        protected Builder(String name) {
            super(name);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] {meta};
        }

        @Override
        public FieldMapper build(MapperBuilderContext context) {
            // TODO: We can reuse a single instance
            FieldType ft = new FieldType();
            ft.setDocValuesType(DocValuesType.BINARY);
            ft.setTokenized(false);
            ft.setOmitNorms(true);
            ft.setIndexOptions(IndexOptions.DOCS);
            ft.freeze();
            return new CountedKeywordFieldMapper(
                name,
                new CountedKeywordFieldType(
                    name,
                    true,
                    false,
                    true,
                    new TextSearchInfo(ft, null, KEYWORD_ANALYZER, KEYWORD_ANALYZER),
                    //TODO: Is this correct?
                    meta.getValue()
                ),
                multiFieldsBuilder.build(this, context),
                copyTo
            );
        }
    }

    public static TypeParser PARSER = new TypeParser((n, c) -> new CountedKeywordFieldMapper.Builder(n));

    protected CountedKeywordFieldMapper(String simpleName, MappedFieldType mappedFieldType, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
    }

    @Override
    public boolean parsesArrayValue() {
        return true;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        //TODO: Can we use a more efficient data structure?
        SortedMap<String, Integer> values = new TreeMap<>();
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return;
        }
        if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            parseArray(context, values);
        } else {
            throw new IllegalArgumentException("Encountered unexpected token [" + parser.currentToken() + "].");
        }
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        for (Map.Entry<String, Integer> value : values.entrySet()) {
            streamOutput.writeString(value.getKey());
            streamOutput.writeVInt(value.getValue());
        }
        BytesRef docValue = streamOutput.bytes().toBytesRef();
        Field field = new BinaryDocValuesField(name(), docValue);
        context.doc().add(field);
    }

    private void parseArray(DocumentParserContext context, SortedMap<String, Integer> values) throws IOException {
        XContentParser parser = context.parser();
        while (true) {
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.END_ARRAY) {
                return;
            }
            if (token == XContentParser.Token.VALUE_STRING) {
                String value = parser.text();
                if (values.containsKey(value) == false) {
                    values.put(value, 1);
                } else {
                    values.put(value, values.get(value) + 1);
                }
            } else {
                throw new IllegalArgumentException("Encountered unexpected token [" + token + "].");
            }
        }
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }



    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
