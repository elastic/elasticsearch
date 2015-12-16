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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

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

    public static class Builder extends MetadataFieldMapper.Builder<Builder, TypeFieldMapper> {

        public Builder(MappedFieldType existing) {
            super(Defaults.NAME, existing == null ? Defaults.FIELD_TYPE : existing, Defaults.FIELD_TYPE);
            indexName = Defaults.NAME;
        }

        @Override
        public TypeFieldMapper build(BuilderContext context) {
            fieldType.setName(buildFullName(context));
            return new TypeFieldMapper(fieldType, context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            if (parserContext.indexVersionCreated().onOrAfter(Version.V_2_0_0_beta1)) {
                throw new MapperParsingException(NAME + " is not configurable");
            }
            Builder builder = new Builder(parserContext.mapperService().fullName(NAME));
            parseField(builder, builder.name, node, parserContext);
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(Settings indexSettings, MappedFieldType fieldType, String typeName) {
            return new TypeFieldMapper(indexSettings, fieldType);
        }
    }

    static final class TypeFieldType extends MappedFieldType {

        public TypeFieldType() {
            setFieldDataType(new FieldDataType("string"));
        }

        protected TypeFieldType(TypeFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new TypeFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public String value(Object value) {
            if (value == null) {
                return null;
            }
            return value.toString();
        }

        @Override
        public boolean useTermQueryWithQueryString() {
            return true;
        }

        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            if (indexOptions() == IndexOptions.NONE) {
                return new ConstantScoreQuery(new PrefixQuery(new Term(UidFieldMapper.NAME, Uid.typePrefixAsBytes(BytesRefs.toBytesRef(value)))));
            }
            return new ConstantScoreQuery(new TermQuery(createTerm(value)));
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
        if (indexCreated.onOrAfter(Version.V_2_1_0)) {
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
        fields.add(new Field(fieldType().name(), context.type(), fieldType()));
        if (fieldType().hasDocValues()) {
            fields.add(new SortedSetDocValuesField(fieldType().name(), new BytesRef(context.type())));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (indexCreatedBefore2x == false) {
            return builder;
        }
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        // if all are defaults, no sense to write it at all
        boolean indexed = fieldType().indexOptions() != IndexOptions.NONE;
        boolean defaultIndexed = Defaults.FIELD_TYPE.indexOptions() != IndexOptions.NONE;
        if (!includeDefaults && fieldType().stored() == Defaults.FIELD_TYPE.stored() && indexed == defaultIndexed) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (includeDefaults || fieldType().stored() != Defaults.FIELD_TYPE.stored()) {
            builder.field("store", fieldType().stored());
        }
        if (includeDefaults || indexed != defaultIndexed) {
            builder.field("index", indexTokenizeOptionToString(indexed, fieldType().tokenized()));
        }
        builder.endObject();
        return builder;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        // do nothing here, no merging, but also no exception
    }
}
