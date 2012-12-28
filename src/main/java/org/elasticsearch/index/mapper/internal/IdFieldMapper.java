/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.RegexpFilter;
import org.elasticsearch.common.lucene.search.XBooleanFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.search.UidFilter;

import java.io.IOException;
import java.util.Collection;
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
            FIELD_TYPE.setIndexed(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_ONLY);
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

        @Override
        public IdFieldMapper build(BuilderContext context) {
            return new IdFieldMapper(name, indexName, boost, fieldType, path, provider);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            IdFieldMapper.Builder builder = id();
            parseField(builder, builder.name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("path")) {
                    builder.path(fieldNode.toString());
                }
            }
            return builder;
        }
    }

    private final String path;

    public IdFieldMapper() {
        this(Defaults.NAME, Defaults.INDEX_NAME, new FieldType(Defaults.FIELD_TYPE));
    }

    public IdFieldMapper(FieldType fieldType) {
        this(Defaults.NAME, Defaults.INDEX_NAME, fieldType);
    }

    protected IdFieldMapper(String name, String indexName, FieldType fieldType) {
        this(name, indexName, Defaults.BOOST, fieldType, Defaults.PATH, null);
    }

    protected IdFieldMapper(String name, String indexName, float boost, FieldType fieldType, String path,
                            PostingsFormatProvider provider) {
        super(new Names(name, indexName, indexName, name), boost, fieldType, Lucene.KEYWORD_ANALYZER,
                Lucene.KEYWORD_ANALYZER, provider, null);
        this.path = path;
    }

    public String path() {
        return this.path;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public String value(Object value) {
        return String.valueOf(value);
    }

    @Override
    public String valueFromString(String value) {
        return value;
    }

    @Override
    public boolean useTermQueryWithQueryString() {
        return true;
    }

    @Override
    public Query termQuery(String value, @Nullable QueryParseContext context) {
        if (fieldType.indexed() || context == null) {
            return super.termQuery(value, context);
        }
        UidFilter filter = new UidFilter(context.queryTypes(), ImmutableList.of(value));
        // no need for constant score filter, since we don't cache the filter, and it always takes deletes into account
        return new ConstantScoreQuery(filter);
    }

    @Override
    public Filter termFilter(String value, @Nullable QueryParseContext context) {
        if (fieldType.indexed() || context == null) {
            return super.termFilter(value, context);
        }
        return new UidFilter(context.queryTypes(), ImmutableList.of(value));
    }

    @Override
    public Query prefixQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context) {
        if (fieldType.indexed() || context == null) {
            return super.prefixQuery(value, method, context);
        }
        Collection<String> queryTypes = context.queryTypes();
        if (queryTypes.size() == 1) {
            PrefixQuery prefixQuery = new PrefixQuery(new Term(UidFieldMapper.NAME, Uid.createUid(Iterables.getFirst(queryTypes, null), value)));
            if (method != null) {
                prefixQuery.setRewriteMethod(method);
            }
            return prefixQuery;
        }
        BooleanQuery query = new BooleanQuery();
        for (String queryType : queryTypes) {
            PrefixQuery prefixQuery = new PrefixQuery(new Term(UidFieldMapper.NAME, Uid.createUid(queryType, value)));
            if (method != null) {
                prefixQuery.setRewriteMethod(method);
            }
            query.add(prefixQuery, BooleanClause.Occur.SHOULD);
        }
        return query;
    }

    @Override
    public Filter prefixFilter(String value, @Nullable QueryParseContext context) {
        if (fieldType.indexed() || context == null) {
            return super.prefixFilter(value, context);
        }
        Collection<String> queryTypes = context.queryTypes();
        if (queryTypes.size() == 1) {
            return new PrefixFilter(new Term(UidFieldMapper.NAME, Uid.createUid(Iterables.getFirst(queryTypes, null), value)));
        }
        XBooleanFilter filter = new XBooleanFilter();
        for (String queryType : queryTypes) {
            filter.add(new PrefixFilter(new Term(UidFieldMapper.NAME, Uid.createUid(queryType, value))), BooleanClause.Occur.SHOULD);
        }
        return filter;
    }

    @Override
    public Query regexpQuery(String value, int flags, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context) {
        if (fieldType.indexed() || context == null) {
            return super.regexpQuery(value, flags, method, context);
        }
        Collection<String> queryTypes = context.queryTypes();
        if (queryTypes.size() == 1) {
            RegexpQuery regexpQuery = new RegexpQuery(new Term(UidFieldMapper.NAME, Uid.createUid(Iterables.getFirst(queryTypes, null), value)), flags);
            if (method != null) {
                regexpQuery.setRewriteMethod(method);
            }
            return regexpQuery;
        }
        BooleanQuery query = new BooleanQuery();
        for (String queryType : queryTypes) {
            RegexpQuery regexpQuery = new RegexpQuery(new Term(UidFieldMapper.NAME, Uid.createUid(queryType, value)), flags);
            if (method != null) {
                regexpQuery.setRewriteMethod(method);
            }
            query.add(regexpQuery, BooleanClause.Occur.SHOULD);
        }
        return query;
    }

    public Filter regexpFilter(String value, int flags, @Nullable QueryParseContext context) {
        if (fieldType.indexed() || context == null) {
            return super.regexpFilter(value, flags, context);
        }
        Collection<String> queryTypes = context.queryTypes();
        if (queryTypes.size() == 1) {
            return new RegexpFilter(new Term(UidFieldMapper.NAME, Uid.createUid(Iterables.getFirst(queryTypes, null), value)), flags);
        }
        XBooleanFilter filter = new XBooleanFilter();
        for (String queryType : queryTypes) {
            filter.add(new RegexpFilter(new Term(UidFieldMapper.NAME, Uid.createUid(queryType, value)), flags), BooleanClause.Occur.SHOULD);
        }
        return filter;
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
    public void parse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void validate(ParseContext context) throws MapperParsingException {
    }

    @Override
    public boolean includeInObject() {
        return true;
    }

    @Override
    protected Field parseCreateField(ParseContext context) throws IOException {
        XContentParser parser = context.parser();
        if (parser.currentName() != null && parser.currentName().equals(Defaults.NAME) && parser.currentToken().isValue()) {
            // we are in the parse Phase
            String id = parser.text();
            if (context.id() != null && !context.id().equals(id)) {
                throw new MapperParsingException("Provided id [" + context.id() + "] does not match the content one [" + id + "]");
            }
            context.id(id);
            if (!fieldType.indexed() && !fieldType.stored()) {
                return null;
            }
            return new Field(names.indexName(), context.id(), fieldType);
        } else {
            // we are in the pre/post parse phase
            if (!fieldType.indexed() && !fieldType.stored()) {
                return null;
            }
            return new Field(names.indexName(), context.id(), fieldType);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // if all are defaults, no sense to write it at all
        if (fieldType.stored() == Defaults.FIELD_TYPE.stored() &&
                fieldType.indexed() == Defaults.FIELD_TYPE.indexed() && path == Defaults.PATH) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (fieldType.stored() != Defaults.FIELD_TYPE.stored()) {
            builder.field("store", fieldType.stored());
        }
        if (fieldType.indexed() != Defaults.FIELD_TYPE.indexed()) {
            builder.field("index", fieldType.indexed());
        }
        if (path != Defaults.PATH) {
            builder.field("path", path);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // do nothing here, no merging, but also no exception
    }
}
