/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.countedkeyword;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.AbstractSortedSetDocValues;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractIndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.CustomDocValuesField;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.common.lucene.Lucene.KEYWORD_ANALYZER;

/**
 * <p>A special field mapper for multi-valued keywords that may contain duplicate values. If the associated <code>counted_terms</code>
 * aggregation is used, duplicates are considered in aggregation results. Consider the following values:</p>
 *
 * <ul>
 *     <li><code>["a", "a", "b"]</code></li>
 *     <li><code>["a", "b", "b"]</code></li>
 * </ul>
 *
 * <p>While a regular <code>keyword</code> and the corresponding <code>terms</code> aggregation deduplicates values and reports a count of
 * 2 for each key (one per document), a <code>counted_terms</code> aggregation on a <code>counted_keyword</code> field will consider
 * the actual count and report a count of 3 for each key.</p>
 *
 * <p>Synthetic source is fully supported.</p>
 */
public class CountedKeywordFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "counted_keyword";
    public static final String COUNT_FIELD_NAME_SUFFIX = "_count";

    private static final FieldType FIELD_TYPE_INDEXED;
    private static final FieldType FIELD_TYPE_NOT_INDEXED;

    static {
        FieldType indexed = new FieldType();
        indexed.setDocValuesType(DocValuesType.SORTED_SET);
        indexed.setTokenized(false);
        indexed.setOmitNorms(true);
        indexed.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE_INDEXED = freezeAndDeduplicateFieldType(indexed);

        FieldType notIndexed = new FieldType();
        notIndexed.setDocValuesType(DocValuesType.SORTED_SET);
        notIndexed.setTokenized(false);
        notIndexed.setOmitNorms(true);
        notIndexed.setIndexOptions(IndexOptions.NONE);
        FIELD_TYPE_NOT_INDEXED = freezeAndDeduplicateFieldType(notIndexed);

    }

    private static class CountedKeywordFieldType extends StringFieldType {

        private final MappedFieldType countFieldType;

        CountedKeywordFieldType(
            String name,
            boolean isIndexed,
            boolean isStored,
            boolean hasDocValues,
            TextSearchInfo textSearchInfo,
            Map<String, String> meta,
            MappedFieldType countFieldType
        ) {
            super(name, isIndexed, isStored, hasDocValues, textSearchInfo, meta);
            this.countFieldType = countFieldType;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();

            return (cache, breakerService) -> new AbstractIndexOrdinalsFieldData(
                name(),
                CoreValuesSourceType.KEYWORD,
                cache,
                breakerService,
                (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n)
            ) {

                @Override
                public LeafOrdinalsFieldData load(LeafReaderContext context) {
                    final SortedSetDocValues dvValues;
                    final BinaryDocValues dvCounts;
                    try {
                        dvValues = DocValues.getSortedSet(context.reader(), getFieldName());
                        dvCounts = DocValues.getBinary(context.reader(), countFieldType.name());
                    } catch (IOException e) {
                        throw new UncheckedIOException("Unable to load " + CONTENT_TYPE + " doc values", e);
                    }

                    return new AbstractLeafOrdinalsFieldData(toScriptFieldFactory) {

                        @Override
                        public SortedSetDocValues getOrdinalsValues() {
                            return new CountedKeywordSortedBinaryDocValues(dvValues, dvCounts);
                        }

                        @Override
                        public long ramBytesUsed() {
                            return 0; // Unknown
                        }

                    };
                }

                @Override
                public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) {
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

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

    static class CountedKeywordSortedBinaryDocValues extends AbstractSortedSetDocValues {
        private final SortedSetDocValues dvValues;
        private final BinaryDocValues dvCounts;
        private int sumCount;
        private Iterator<Long> ordsForThisDoc;
        private final ByteArrayStreamInput scratch = new ByteArrayStreamInput();

        CountedKeywordSortedBinaryDocValues(SortedSetDocValues dvValues, BinaryDocValues dvCounts) {
            this.dvValues = dvValues;
            this.dvCounts = dvCounts;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            sumCount = 0;
            if (dvValues.advanceExact(doc)) {
                boolean exactMatch = dvCounts.advanceExact(doc);
                assert exactMatch;

                BytesRef encodedValue = dvCounts.binaryValue();
                scratch.reset(encodedValue.bytes, encodedValue.offset, encodedValue.length);
                int[] counts = scratch.readVIntArray();
                assert counts.length == dvValues.docValueCount();

                List<Long> values = new ArrayList<>();
                for (int count : counts) {
                    this.sumCount += count;
                    long ord = dvValues.nextOrd();
                    for (int j = 0; j < count; j++) {
                        values.add(ord);
                    }
                }
                this.ordsForThisDoc = values.iterator();
                return true;
            } else {
                ordsForThisDoc = null;
                return false;
            }
        }

        @Override
        public int docValueCount() {
            return sumCount;
        }

        @Override
        public long nextOrd() {
            assert ordsForThisDoc.hasNext();
            return ordsForThisDoc.next();
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            return dvValues.lookupOrd(ord);
        }

        @Override
        public long getValueCount() {
            return dvValues.getValueCount();
        }

        @Override
        public TermsEnum termsEnum() throws IOException {
            return dvValues.termsEnum();
        }
    }

    private static CountedKeywordFieldMapper toType(FieldMapper in) {
        return (CountedKeywordFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {
        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).mappedFieldType.isIndexed(), true);
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final SourceKeepMode indexSourceKeepMode;

        protected Builder(String name, SourceKeepMode indexSourceKeepMode) {
            super(name);
            this.indexSourceKeepMode = indexSourceKeepMode;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta, indexed };
        }

        @Override
        public FieldMapper build(MapperBuilderContext context) {

            BinaryFieldMapper countFieldMapper = new BinaryFieldMapper.Builder(
                leafName() + COUNT_FIELD_NAME_SUFFIX,
                context.isSourceSynthetic()
            ).docValues(true).build(context);
            boolean isIndexed = indexed.getValue();
            FieldType ft = isIndexed ? FIELD_TYPE_INDEXED : FIELD_TYPE_NOT_INDEXED;
            return new CountedKeywordFieldMapper(
                leafName(),
                ft,
                new CountedKeywordFieldType(
                    context.buildFullName(leafName()),
                    isIndexed,
                    false,
                    true,
                    new TextSearchInfo(ft, null, KEYWORD_ANALYZER, KEYWORD_ANALYZER),
                    meta.getValue(),
                    countFieldMapper.fieldType()
                ),
                builderParams(this, context),
                countFieldMapper,
                indexSourceKeepMode
            );
        }
    }

    private static class CountedKeywordFieldSyntheticSourceLoader extends SourceLoader.DocValuesBasedSyntheticFieldLoader {
        private final String keywordsFieldName;
        private final String countsFieldName;
        private final String leafName;

        private SortedSetDocValues keywordsReader;
        private BinaryDocValues countsReader;
        private boolean hasValue;

        CountedKeywordFieldSyntheticSourceLoader(String keywordsFieldName, String countsFieldName, String leafName) {
            this.keywordsFieldName = keywordsFieldName;
            this.countsFieldName = countsFieldName;
            this.leafName = leafName;
        }

        @Override
        public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
            keywordsReader = leafReader.getSortedSetDocValues(keywordsFieldName);
            countsReader = leafReader.getBinaryDocValues(countsFieldName);

            if (keywordsReader == null || countsReader == null) {
                return null;
            }

            return docId -> {
                hasValue = keywordsReader.advanceExact(docId);
                if (hasValue == false) {
                    return false;
                }

                boolean countsHasValue = countsReader.advanceExact(docId);
                assert countsHasValue;

                return true;
            };
        }

        @Override
        public boolean hasValue() {
            return hasValue;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            if (hasValue == false) {
                return;
            }

            int[] counts = new BytesArray(countsReader.binaryValue()).streamInput().readVIntArray();
            boolean singleValue = counts.length == 1 && counts[0] == 1;

            if (singleValue) {
                b.field(leafName);
            } else {
                b.startArray(leafName);
            }

            for (int i = 0; i < keywordsReader.docValueCount(); i++) {
                BytesRef currKeyword = keywordsReader.lookupOrd(keywordsReader.nextOrd());
                for (int j = 0; j < counts[i]; j++) {
                    b.utf8Value(currKeyword.bytes, currKeyword.offset, currKeyword.length);
                }
            }

            if (singleValue == false) {
                b.endArray();
            }
        }

        @Override
        public String fieldName() {
            return keywordsFieldName;
        }
    }

    public static TypeParser PARSER = new TypeParser(
        (n, c) -> new CountedKeywordFieldMapper.Builder(n, c.getIndexSettings().sourceKeepMode())
    );

    private final FieldType fieldType;
    private final BinaryFieldMapper countFieldMapper;
    private final SourceKeepMode indexSourceKeepMode;

    protected CountedKeywordFieldMapper(
        String simpleName,
        FieldType fieldType,
        MappedFieldType mappedFieldType,
        BuilderParams builderParams,
        BinaryFieldMapper countFieldMapper,
        SourceKeepMode indexSourceKeepMode
    ) {
        super(simpleName, mappedFieldType, builderParams);
        this.fieldType = fieldType;
        this.countFieldMapper = countFieldMapper;
        this.indexSourceKeepMode = indexSourceKeepMode;
    }

    @Override
    public boolean parsesArrayValue() {
        return true;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        SortedMap<String, Integer> values = new TreeMap<>();
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return;
        }
        if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            parseArray(context, values);
        } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            parseValue(parser, values);
        } else {
            throw new IllegalArgumentException("Encountered unexpected token [" + parser.currentToken() + "].");
        }

        if (values.isEmpty()) {
            return;
        }

        for (String value : values.keySet()) {
            context.doc().add(new KeywordFieldMapper.KeywordField(fullPath(), new BytesRef(value), fieldType));
        }
        CountsBinaryDocValuesField field = (CountsBinaryDocValuesField) context.doc().getByKey(countFieldMapper.fieldType().name());
        if (field == null) {
            field = new CountsBinaryDocValuesField(countFieldMapper.fieldType().name());
            field.add(values);
            context.doc().addWithKey(countFieldMapper.fieldType().name(), field);
        } else {
            field.add(values);
        }
    }

    private void parseArray(DocumentParserContext context, SortedMap<String, Integer> values) throws IOException {
        XContentParser parser = context.parser();
        int arrDepth = 1;
        while (true) {
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.END_ARRAY) {
                arrDepth -= 1;
                if (arrDepth <= 0) {
                    return;
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                parseValue(parser, values);
            } else if (token == XContentParser.Token.START_ARRAY) {
                arrDepth += 1;
            } else if (token == XContentParser.Token.VALUE_NULL) {
                // ignore null values
            } else {
                throw new IllegalArgumentException("Encountered unexpected token [" + token + "].");
            }
        }
    }

    private static void parseValue(XContentParser parser, SortedMap<String, Integer> values) throws IOException {
        String value = parser.text();
        if (values.containsKey(value) == false) {
            values.put(value, 1);
        } else {
            values.put(value, values.get(value) + 1);
        }
    }

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

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), indexSourceKeepMode).init(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        var keepMode = sourceKeepMode().orElse(indexSourceKeepMode);
        if (keepMode != SourceKeepMode.NONE) {
            return super.syntheticSourceSupport();
        }

        return new SyntheticSourceSupport.Native(
            () -> new CountedKeywordFieldSyntheticSourceLoader(fullPath(), countFieldMapper.fullPath(), leafName())
        );
    }

    private class CountsBinaryDocValuesField extends CustomDocValuesField {
        private final SortedMap<String, Integer> counts;

        CountsBinaryDocValuesField(String name) {
            super(name);
            counts = new TreeMap<>();
        }

        public void add(SortedMap<String, Integer> newCounts) {
            for (Map.Entry<String, Integer> currCount : newCounts.entrySet()) {
                this.counts.put(currCount.getKey(), this.counts.getOrDefault(currCount.getKey(), 0) + currCount.getValue());
            }
        }

        @Override
        public BytesRef binaryValue() {
            try {
                int maxBytesPerVInt = 5;
                int bytesSize = (counts.size() + 1) * maxBytesPerVInt;
                BytesStreamOutput out = new BytesStreamOutput(bytesSize);
                int countsArr[] = new int[counts.size()];
                int i = 0;
                for (Integer currCount : counts.values()) {
                    countsArr[i++] = currCount;
                }
                out.writeVIntArray(countsArr);
                return out.bytes().toBytesRef();
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to get binary value", e);
            }
        }
    }

}
