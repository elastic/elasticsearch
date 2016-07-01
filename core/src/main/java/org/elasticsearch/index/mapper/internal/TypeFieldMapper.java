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

package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.core.TextFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
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
        public MetadataFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            throw new MapperParsingException(NAME + " is not configurable");
        }

        @Override
        public MetadataFieldMapper getDefault(Settings indexSettings, MappedFieldType fieldType, String typeName) {
            return new TypeFieldMapper(indexSettings, fieldType);
        }
    }

    static final class TypeFieldType extends StringFieldType {
        private boolean fielddata;

        public TypeFieldType() {
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
            return new TypeQuery(indexedValueForSearch(value));
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

    public static class TypeQuery extends Query {

        private final BytesRef type;

        public TypeQuery(BytesRef type) {
            this.type = Objects.requireNonNull(type);
        }

        @Override
        public Query rewrite(IndexReader reader) throws IOException {
            Term term = new Term(CONTENT_TYPE, type);
            TermContext context = TermContext.build(reader.getContext(), term);
            if (context.docFreq() == reader.maxDoc()) {
                // All docs have the same type.
                // Using a match_all query will help Lucene perform some optimizations
                // For instance, match_all queries as filter clauses are automatically removed
                return new MatchAllDocsQuery();
            } else {
                return new ConstantScoreQuery(new TermQuery(term, context));
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (sameClassAs(obj) == false) {
                return false;
            }
            TypeQuery that = (TypeQuery) obj;
            return type.equals(that.type);
        }

        @Override
        public int hashCode() {
            return 31 * classHash() + type.hashCode();
        }

        @Override
        public String toString(String field) {
            return "_type:" + type;
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
        Version indexCreated = Version.indexCreated(indexSettings);
        if (indexCreated.before(Version.V_2_1_0)) {
            // enables fielddata loading, doc values was disabled on _type between 2.0 and 2.1.
            ((TypeFieldType) defaultFieldType).setFielddata(true);
        } else {
            defaultFieldType.setHasDocValues(true);
        }
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
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
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
