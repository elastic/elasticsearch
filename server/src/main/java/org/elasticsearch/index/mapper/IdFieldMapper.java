/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A mapper for the _id field. It does nothing since _id is neither indexed nor
 * stored, but we need to keep it so that its FieldType can be used to generate
 * queries.
 */
public class IdFieldMapper extends MetadataFieldMapper {
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(IdFieldMapper.class));
    static final String ID_FIELD_DATA_DEPRECATION_MESSAGE =
        "Loading the fielddata on the _id field is deprecated and will be removed in future versions. "
            + "If you require sorting or aggregating on this field you should also include the id in the "
            + "body of your documents, and map this field as a keyword field that has [doc_values] enabled";

    public static final String NAME = "_id";

    public static final String CONTENT_TYPE = "_id";

    public static class Defaults {
        public static final String NAME = IdFieldMapper.NAME;

        public static final FieldType FIELD_TYPE = new FieldType();
        public static final FieldType NESTED_FIELD_TYPE;
        public static final MappedFieldType MAPPED_FIELD_TYPE = new IdFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();

            NESTED_FIELD_TYPE = new FieldType();
            NESTED_FIELD_TYPE.setTokenized(false);
            NESTED_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            NESTED_FIELD_TYPE.setStored(true);
            NESTED_FIELD_TYPE.setOmitNorms(true);
            NESTED_FIELD_TYPE.setStored(false);
            NESTED_FIELD_TYPE.freeze();
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?> parse(String name, Map<String, Object> node,
                                                 ParserContext parserContext) throws MapperParsingException {
            throw new MapperParsingException(NAME + " is not configurable");
        }

        @Override
        public MetadataFieldMapper getDefault(ParserContext context) {
            return new IdFieldMapper(Defaults.FIELD_TYPE);
        }
    }

    static final class IdFieldType extends TermBasedFieldType {

        public static final IdFieldType INSTANCE = new IdFieldType();

        private IdFieldType() {
            super(NAME, true, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        protected IdFieldType(IdFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new IdFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isSearchable() {
            // The _id field is always searchable.
            return true;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            return termsQuery(Arrays.asList(value), context);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new MatchAllDocsQuery();
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            failIfNotIndexed();
            BytesRef[] bytesRefs = new BytesRef[values.size()];
            for (int i = 0; i < bytesRefs.length; i++) {
                Object idObject = values.get(i);
                if (idObject instanceof BytesRef) {
                    idObject = ((BytesRef) idObject).utf8ToString();
                }
                bytesRefs[i] = Uid.encodeId(idObject.toString());
            }
            return new TermInSetQuery(name(), bytesRefs);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            final IndexFieldData.Builder fieldDataBuilder = new PagedBytesIndexFieldData.Builder(
                    TextFieldMapper.Defaults.FIELDDATA_MIN_FREQUENCY,
                    TextFieldMapper.Defaults.FIELDDATA_MAX_FREQUENCY,
                    TextFieldMapper.Defaults.FIELDDATA_MIN_SEGMENT_SIZE,
                    CoreValuesSourceType.BYTES);
            return new IndexFieldData.Builder() {
                @Override
                public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                        CircuitBreakerService breakerService, MapperService mapperService) {
                    if (mapperService.isIdFieldDataEnabled() == false) {
                        throw new IllegalArgumentException("Fielddata access on the _id field is disallowed, "
                            + "you can re-enable it by updating the dynamic cluster setting: "
                            + IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey());
                    }
                    deprecationLogger.deprecate("id_field_data", ID_FIELD_DATA_DEPRECATION_MESSAGE);
                    final IndexFieldData<?> fieldData = fieldDataBuilder.build(indexSettings, fieldType, cache,
                        breakerService, mapperService);
                    return new IndexFieldData<LeafFieldData>() {

                        @Override
                        public Index index() {
                            return fieldData.index();
                        }

                        @Override
                        public String getFieldName() {
                            return fieldData.getFieldName();
                        }

                        @Override
                        public ValuesSourceType getValuesSourceType() {
                            return fieldData.getValuesSourceType();
                        }

                        @Override
                        public LeafFieldData load(LeafReaderContext context) {
                            return wrap(fieldData.load(context));
                        }

                        @Override
                        public LeafFieldData loadDirect(LeafReaderContext context) throws Exception {
                            return wrap(fieldData.loadDirect(context));
                        }

                        @Override
                        public SortField sortField(Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
                            XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue,
                                sortMode, nested);
                            return new SortField(getFieldName(), source, reverse);
                        }

                        @Override
                        public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode,
                                Nested nested, SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
                            throw new UnsupportedOperationException("can't sort on the [" + CONTENT_TYPE + "] field");
                        }

                        @Override
                        public void clear() {
                            fieldData.clear();
                        }

                    };
                }
            };
        }
    }

    private static LeafFieldData wrap(LeafFieldData in) {
        return new LeafFieldData() {

            @Override
            public void close() {
                in.close();
            }

            @Override
            public long ramBytesUsed() {
                return in.ramBytesUsed();
            }

            @Override
            public ScriptDocValues<?> getScriptValues() {
                return new ScriptDocValues.Strings(getBytesValues());
            }

            @Override
            public SortedBinaryDocValues getBytesValues() {
                SortedBinaryDocValues inValues = in.getBytesValues();
                return new SortedBinaryDocValues() {

                    @Override
                    public BytesRef nextValue() throws IOException {
                        BytesRef encoded = inValues.nextValue();
                        return new BytesRef(Uid.decodeId(
                                Arrays.copyOfRange(encoded.bytes, encoded.offset, encoded.offset + encoded.length)));
                    }

                    @Override
                    public int docValueCount() {
                        final int count = inValues.docValueCount();
                        // If the count is not 1 then the impl is not correct as the binary representation
                        // does not preserve order. But id fields only have one value per doc so we are good.
                        assert count == 1;
                        return inValues.docValueCount();
                    }

                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        return inValues.advanceExact(doc);
                    }
                };
            }
        };
    }

    private IdFieldMapper(FieldType fieldType) {
        super(fieldType, new IdFieldType());
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            BytesRef id = Uid.encodeId(context.sourceToParse().id());
            context.doc().add(new Field(NAME, id, fieldType));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }
}
