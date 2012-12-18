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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.Term;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 *
 */
public class IndexFieldMapper extends AbstractFieldMapper<String> implements InternalMapper, RootMapper {

    public static final String NAME = "_index";

    public static final String CONTENT_TYPE = "_index";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = IndexFieldMapper.NAME;
        public static final String INDEX_NAME = IndexFieldMapper.NAME;

        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setIndexed(true);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_ONLY);
            FIELD_TYPE.freeze();
        }

        public static final boolean ENABLED = false;
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, IndexFieldMapper> {

        private boolean enabled = Defaults.ENABLED;

        public Builder() {
            super(Defaults.NAME, new FieldType(Defaults.FIELD_TYPE));
            indexName = Defaults.INDEX_NAME;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        @Override
        public IndexFieldMapper build(BuilderContext context) {
            return new IndexFieldMapper(name, indexName, boost, fieldType, enabled, provider);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            IndexFieldMapper.Builder builder = MapperBuilders.index();
            parseField(builder, builder.name, node, parserContext);

            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    builder.enabled(nodeBooleanValue(fieldNode));
                }
            }
            return builder;
        }
    }

    private final boolean enabled;

    public IndexFieldMapper() {
        this(Defaults.NAME, Defaults.INDEX_NAME);
    }

    protected IndexFieldMapper(String name, String indexName) {
        this(name, indexName, Defaults.BOOST, new FieldType(Defaults.FIELD_TYPE), Defaults.ENABLED, null);
    }

    public IndexFieldMapper(String name, String indexName, float boost, FieldType fieldType, boolean enabled,
                            PostingsFormatProvider provider) {
        super(new Names(name, indexName, indexName, name), boost, fieldType, Lucene.KEYWORD_ANALYZER,
                Lucene.KEYWORD_ANALYZER, provider, null);
        this.enabled = enabled;
    }

    public boolean enabled() {
        return this.enabled;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    public String value(Document document) {
        Field field = (Field) document.getField(names.indexName());
        return field == null ? null : value(field);
    }

    @Override
    public String value(Object value) {
        return String.valueOf(value);
    }

    @Override
    public String valueFromString(String value) {
        return value;
    }

    public Term term(String value) {
        return names().createIndexNameTerm(value);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        // we pre parse it and not in parse, since its not part of the root object
        super.parse(context);
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
    }

    @Override
    public void parse(ParseContext context) throws IOException {

    }

    @Override
    public void validate(ParseContext context) throws MapperParsingException {
    }

    @Override
    public boolean includeInObject() {
        return false;
    }

    @Override
    protected Field parseCreateField(ParseContext context) throws IOException {
        if (!enabled) {
            return null;
        }
        return new Field(names.indexName(), context.index(), fieldType);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // if all defaults, no need to write it at all
        if (stored() == Defaults.FIELD_TYPE.stored() && enabled == Defaults.ENABLED) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (stored() != Defaults.FIELD_TYPE.stored()) {
            builder.field("store", stored());
        }
        if (enabled != Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // do nothing here, no merging, but also no exception
    }
}
