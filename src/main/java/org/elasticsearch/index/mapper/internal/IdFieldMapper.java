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

import com.google.common.collect.Iterables;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.InternalMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.RootMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.id;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 * 
 */
public class IdFieldMapper extends AbstractFieldMapper<String> implements InternalMapper, RootMapper {

    public static final String NAME = "_id";

    public static final String CONTENT_TYPE = "_id";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = IdFieldMapper.NAME;
        public static final String INDEX_NAME = IdFieldMapper.NAME;

        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }

        public static final String PATH = null;
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, IdFieldMapper> {

        private String path = Defaults.PATH;

        public Builder() {
            super(Defaults.NAME, new FieldType(Defaults.FIELD_TYPE));
            indexName = Defaults.INDEX_NAME;
        }

        public Builder path(String path) {
            this.path = path;
            return builder;
        }
        // if we are indexed we use DOCS
        @Override
        protected IndexOptions getDefaultIndexOption() {
            return IndexOptions.DOCS;
        }

        @Override
        public IdFieldMapper build(BuilderContext context) {
            return new IdFieldMapper(name, indexName, boost, fieldType, docValues, path, fieldDataSettings, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            if (parserContext.indexVersionCreated().onOrAfter(Version.V_2_0_0)) {
                throw new MapperParsingException(NAME + " is not configurable");
            }
            IdFieldMapper.Builder builder = id();
            parseField(builder, builder.name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("path")) {
                    builder.path(fieldNode.toString());
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    private final String path;

    public IdFieldMapper(Settings indexSettings) {
        this(Defaults.NAME, Defaults.INDEX_NAME, Defaults.BOOST, idFieldType(indexSettings), null, Defaults.PATH, null, indexSettings);
    }

    protected IdFieldMapper(String name, String indexName, float boost, FieldType fieldType, Boolean docValues, String path,
                            @Nullable Settings fieldDataSettings, Settings indexSettings) {
        super(new Names(name, indexName, indexName, name), boost, fieldType, docValues, Lucene.KEYWORD_ANALYZER,
                Lucene.KEYWORD_ANALYZER, null, null, fieldDataSettings, indexSettings);
        this.path = path;
    }
    
    private static FieldType idFieldType(Settings indexSettings) {
        FieldType fieldType = new FieldType(Defaults.FIELD_TYPE);
        boolean pre2x = Version.indexCreated(indexSettings).before(Version.V_2_0_0);
        if (pre2x && indexSettings.getAsBoolean("index.mapping._id.indexed", true) == false) {
            fieldType.setTokenized(false);
        }
        return fieldType;
    }

    public String path() {
        return this.path;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("string");
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
    public Query termQuery(Object value, @Nullable QueryParseContext context) {
        if (fieldType.indexOptions() != IndexOptions.NONE || context == null) {
            return super.termQuery(value, context);
        }
        // no need for constant score filter, since we don't cache the filter, and it always takes deletes into account
        return new ConstantScoreQuery(termFilter(value, context));
    }

    @Override
    public Filter termFilter(Object value, @Nullable QueryParseContext context) {
        if (fieldType.indexOptions() != IndexOptions.NONE || context == null) {
            return super.termFilter(value, context);
        }
        return Queries.wrap(new TermsQuery(UidFieldMapper.NAME, Uid.createTypeUids(context.queryTypes(), value)));
    }

    @Override
    public Filter termsFilter(List values, @Nullable QueryParseContext context) {
        if (fieldType.indexOptions() != IndexOptions.NONE || context == null) {
            return super.termsFilter(values, context);
        }
        return Queries.wrap(new TermsQuery(UidFieldMapper.NAME, Uid.createTypeUids(context.queryTypes(), values)));
    }

    @Override
    public Query prefixQuery(Object value, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context) {
        if (fieldType.indexOptions() != IndexOptions.NONE || context == null) {
            return super.prefixQuery(value, method, context);
        }
        Collection<String> queryTypes = context.queryTypes();
        BooleanQuery query = new BooleanQuery();
        for (String queryType : queryTypes) {
            PrefixQuery prefixQuery = new PrefixQuery(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(queryType, BytesRefs.toBytesRef(value))));
            if (method != null) {
                prefixQuery.setRewriteMethod(method);
            }
            query.add(prefixQuery, BooleanClause.Occur.SHOULD);
        }
        return query;
    }

    @Override
    public Filter prefixFilter(Object value, @Nullable QueryParseContext context) {
        if (fieldType.indexOptions() != IndexOptions.NONE || context == null) {
            return super.prefixFilter(value, context);
        }
        Collection<String> queryTypes = context.queryTypes();
        BooleanQuery filter = new BooleanQuery();
        for (String queryType : queryTypes) {
            filter.add(new PrefixQuery(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(queryType, BytesRefs.toBytesRef(value)))), BooleanClause.Occur.SHOULD);
        }
        return Queries.wrap(filter);
    }

    @Override
    public Query regexpQuery(Object value, int flags, int maxDeterminizedStates, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context) {
        if (fieldType.indexOptions() != IndexOptions.NONE || context == null) {
            return super.regexpQuery(value, flags, maxDeterminizedStates, method, context);
        }
        Collection<String> queryTypes = context.queryTypes();
        if (queryTypes.size() == 1) {
            RegexpQuery regexpQuery = new RegexpQuery(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(Iterables.getFirst(queryTypes, null), BytesRefs.toBytesRef(value))),
                                                      flags, maxDeterminizedStates);
            if (method != null) {
                regexpQuery.setRewriteMethod(method);
            }
            return regexpQuery;
        }
        BooleanQuery query = new BooleanQuery();
        for (String queryType : queryTypes) {
            RegexpQuery regexpQuery = new RegexpQuery(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(queryType, BytesRefs.toBytesRef(value))), flags, maxDeterminizedStates);
            if (method != null) {
                regexpQuery.setRewriteMethod(method);
            }
            query.add(regexpQuery, BooleanClause.Occur.SHOULD);
        }
        return query;
    }

    @Override
    public Filter regexpFilter(Object value, int flags, int maxDeterminizedStates, @Nullable QueryParseContext context) {
        if (fieldType.indexOptions() != IndexOptions.NONE || context == null) {
            return super.regexpFilter(value, flags, maxDeterminizedStates, context);
        }
        Collection<String> queryTypes = context.queryTypes();
        BooleanQuery filter = new BooleanQuery();
        for (String queryType : queryTypes) {
            filter.add(new RegexpQuery(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(queryType, BytesRefs.toBytesRef(value))),
                                        flags, maxDeterminizedStates), BooleanClause.Occur.SHOULD);
        }
        return Queries.wrap(filter);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        if (context.sourceToParse().id() != null) {
            context.id(context.sourceToParse().id());
            super.parse(context);
        }
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        if (context.id() == null && !context.sourceToParse().flyweight()) {
            throw new MapperParsingException("No id found while parsing the content source");
        }
        // it either get built in the preParse phase, or get parsed...
    }

    @Override
    public boolean includeInObject() {
        return true;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        XContentParser parser = context.parser();
        if (parser.currentName() != null && parser.currentName().equals(Defaults.NAME) && parser.currentToken().isValue()) {
            // we are in the parse Phase
            String id = parser.text();
            if (context.id() != null && !context.id().equals(id)) {
                throw new MapperParsingException("Provided id [" + context.id() + "] does not match the content one [" + id + "]");
            }
            context.id(id);
        } // else we are in the pre/post parse phase

        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            fields.add(new Field(names.indexName(), context.id(), fieldType));
        }
        if (hasDocValues()) {
            fields.add(new BinaryDocValuesField(names.indexName(), new BytesRef(context.id())));
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
        if (!includeDefaults && fieldType.stored() == Defaults.FIELD_TYPE.stored()
                && fieldType.indexOptions() == Defaults.FIELD_TYPE.indexOptions()
                && path == Defaults.PATH
                && customFieldDataSettings == null) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (includeDefaults || fieldType.stored() != Defaults.FIELD_TYPE.stored()) {
            builder.field("store", fieldType.stored());
        }
        if (includeDefaults || fieldType.indexOptions() != Defaults.FIELD_TYPE.indexOptions()) {
            builder.field("index", indexTokenizeOptionToString(fieldType.indexOptions() != IndexOptions.NONE, fieldType.tokenized()));
        }
        if (includeDefaults || path != Defaults.PATH) {
            builder.field("path", path);
        }

        if (customFieldDataSettings != null) {
            builder.field("fielddata", (Map) customFieldDataSettings.getAsMap());
        } else if (includeDefaults) {
            builder.field("fielddata", (Map) fieldDataType.getSettings().getAsMap());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        // do nothing here, no merging, but also no exception
    }
}
