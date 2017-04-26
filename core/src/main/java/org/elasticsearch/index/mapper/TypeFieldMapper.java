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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TypeFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_type";

    public static final String CONTENT_TYPE = "_type";

    public static class Defaults {
        public static final String NAME = TypeFieldMapper.NAME;

        public static final MappedFieldType FIELD_TYPE = new TypeFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setName(NAME);
            FIELD_TYPE.freeze();
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            throw new MapperParsingException(NAME + " is not configurable");
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext context) {
            final Settings indexSettings = context.mapperService().getIndexSettings().getSettings();
            return new TypeFieldMapper(indexSettings, fieldType);
        }
    }

    static final class TypeFieldType extends StringFieldType {
        private boolean fielddata;

        TypeFieldType() {
            this.fielddata = false;
        }

        protected TypeFieldType(TypeFieldType ref) {
            super(ref);
            this.fielddata = ref.fielddata;
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) {
                return false;
            }
            TypeFieldType that = (TypeFieldType) o;
            return fielddata == that.fielddata;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), fielddata);
        }

        @Override
        public MappedFieldType clone() {
            return new TypeFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public boolean fielddata() {
            return fielddata;
        }

        public void setFielddata(boolean fielddata) {
            checkIfFrozen();
            this.fielddata = fielddata;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder() {
            if (hasDocValues()) {
                return new DocValuesIndexFieldData.Builder();
            }
            assert indexOptions() != IndexOptions.NONE;
            if (fielddata) {
                return new PagedBytesIndexFieldData.Builder(TextFieldMapper.Defaults.FIELDDATA_MIN_FREQUENCY,
                    TextFieldMapper.Defaults.FIELDDATA_MAX_FREQUENCY,
                    TextFieldMapper.Defaults.FIELDDATA_MIN_SEGMENT_SIZE);
            }
            return super.fielddataBuilder();
        }

        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            if (indexOptions() == IndexOptions.NONE) {
                throw new AssertionError();
            }
            return new TypesQuery(indexedValueForSearch(value));
        }

        @Override
        public void checkCompatibility(MappedFieldType other,
                                       List<String> conflicts, boolean strict) {
            super.checkCompatibility(other, conflicts, strict);
            TypeFieldType otherType = (TypeFieldType) other;
            if (strict) {
                if (fielddata() != otherType.fielddata()) {
                    conflicts.add("mapper [" + name() + "] is used by multiple types. Set update_all_types to true to update [fielddata] "
                        + "across all types.");
                }
            }
        }
    }

    /**
     * Specialization for a disjunction over many _type
     */
    public static class TypesQuery extends Query {
        // Same threshold as TermInSetQuery
        private static final int BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD = 16;

        private final BytesRef[] types;

        public TypesQuery(BytesRef... types) {
            if (types == null) {
                throw new NullPointerException("types cannot be null.");
            }
            if (types.length == 0) {
                throw new IllegalArgumentException("types must contains at least one value.");
            }
            this.types = types;
        }

        public BytesRef[] getTerms() {
            return types;
        }

        @Override
        public Query rewrite(IndexReader reader) throws IOException {
            final int threshold = Math.min(BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD, BooleanQuery.getMaxClauseCount());
            if (types.length <= threshold) {
                Set<BytesRef> uniqueTypes = new HashSet<>();
                BooleanQuery.Builder bq = new BooleanQuery.Builder();
                int totalDocFreq = 0;
                for (BytesRef type : types) {
                    if (uniqueTypes.add(type)) {
                        Term term = new Term(CONTENT_TYPE, type);
                        TermContext context = TermContext.build(reader.getContext(), term);
                        if (context.docFreq() == 0) {
                            // this _type is not present in the reader
                            continue;
                        }
                        totalDocFreq += context.docFreq();
                        // strict equality should be enough ?
                        if (totalDocFreq >= reader.maxDoc()) {
                            assert totalDocFreq == reader.maxDoc();
                            // Matches all docs since _type is a single value field
                            // Using a match_all query will help Lucene perform some optimizations
                            // For instance, match_all queries as filter clauses are automatically removed
                            return new MatchAllDocsQuery();
                        }
                        bq.add(new TermQuery(term, context), BooleanClause.Occur.SHOULD);
                    }
                }
                return new ConstantScoreQuery(bq.build());
            }
            return new TermInSetQuery(CONTENT_TYPE, types);
        }

        @Override
        public boolean equals(Object obj) {
            if (sameClassAs(obj) == false) {
                return false;
            }
            TypesQuery that = (TypesQuery) obj;
            return Arrays.equals(types, that.types);
        }

        @Override
        public int hashCode() {
            return 31 * classHash() + Arrays.hashCode(types);
        }

        @Override
        public String toString(String field) {
            StringBuilder builder = new StringBuilder();
            for (BytesRef type : types) {
                if (builder.length() > 0) {
                    builder.append(' ');
                }
                builder.append(new Term(CONTENT_TYPE, type).toString());
            }
            return builder.toString();
        }
    }

    private TypeFieldMapper(Settings indexSettings, MappedFieldType existing) {
        this(existing == null ? defaultFieldType(indexSettings) : existing.clone(),
             indexSettings);
    }

    private TypeFieldMapper(MappedFieldType fieldType, Settings indexSettings) {
        super(NAME, fieldType, defaultFieldType(indexSettings), indexSettings);
    }

    private static MappedFieldType defaultFieldType(Settings indexSettings) {
        MappedFieldType defaultFieldType = Defaults.FIELD_TYPE.clone();
        defaultFieldType.setHasDocValues(true);
        return defaultFieldType;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        // we parse in pre parse
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        if (fieldType().indexOptions() == IndexOptions.NONE && !fieldType().stored()) {
            return;
        }
        fields.add(new Field(fieldType().name(), context.sourceToParse().type(), fieldType()));
        if (fieldType().hasDocValues()) {
            fields.add(new SortedSetDocValuesField(fieldType().name(), new BytesRef(context.sourceToParse().type())));
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

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        // do nothing here, no merging, but also no exception
    }
}
