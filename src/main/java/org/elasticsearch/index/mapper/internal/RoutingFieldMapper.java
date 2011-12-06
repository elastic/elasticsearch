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
import org.apache.lucene.document.Fieldable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.MapperBuilders.routing;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 *
 */
public class RoutingFieldMapper extends AbstractFieldMapper<String> implements InternalMapper, RootMapper {

    public static final String NAME = "_routing";
    public static final String CONTENT_TYPE = "_routing";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = "_routing";
        public static final Field.Index INDEX = Field.Index.NOT_ANALYZED;
        public static final Field.Store STORE = Field.Store.YES;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
        public static final boolean REQUIRED = false;
        public static final String PATH = null;
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, RoutingFieldMapper> {

        private boolean required = Defaults.REQUIRED;

        private String path = Defaults.PATH;

        public Builder() {
            super(Defaults.NAME);
            store = Defaults.STORE;
            index = Defaults.INDEX;
        }

        public Builder required(boolean required) {
            this.required = required;
            return builder;
        }

        public Builder path(String path) {
            this.path = path;
            return builder;
        }

        @Override
        public RoutingFieldMapper build(BuilderContext context) {
            return new RoutingFieldMapper(store, index, required, path);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            RoutingFieldMapper.Builder builder = routing();
            parseField(builder, builder.name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("required")) {
                    builder.required(nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("path")) {
                    builder.path(fieldNode.toString());
                }
            }
            return builder;
        }
    }


    private boolean required;

    private final String path;

    public RoutingFieldMapper() {
        this(Defaults.STORE, Defaults.INDEX, Defaults.REQUIRED, Defaults.PATH);
    }

    protected RoutingFieldMapper(Field.Store store, Field.Index index, boolean required, String path) {
        super(new Names(Defaults.NAME, Defaults.NAME, Defaults.NAME, Defaults.NAME), index, store, Defaults.TERM_VECTOR, 1.0f, Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS,
                Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER);
        this.required = required;
        this.path = path;
    }

    public void markAsRequired() {
        this.required = true;
    }

    public boolean required() {
        return this.required;
    }

    public String path() {
        return this.path;
    }

    public String value(Document document) {
        Fieldable field = document.getFieldable(names.indexName());
        return field == null ? null : value(field);
    }

    @Override
    public String value(Fieldable field) {
        return field.stringValue();
    }

    @Override
    public String valueFromString(String value) {
        return value;
    }

    @Override
    public String valueAsString(Fieldable field) {
        return value(field);
    }

    @Override
    public String indexedValue(String value) {
        return value;
    }

    @Override
    public void validate(ParseContext context) throws MapperParsingException {
        String routing = context.sourceToParse().routing();
        if (path != null && routing != null) {
            // we have a path, check if we can validate we have the same routing value as the one in the doc...
            String value = null;
            Fieldable field = context.doc().getFieldable(path);
            if (field != null) {
                value = field.stringValue();
                if (value == null) {
                    // maybe its a numeric field...
                    if (field instanceof NumberFieldMapper.CustomNumericField) {
                        value = ((NumberFieldMapper.CustomNumericField) field).numericAsString();
                    }
                }
            }
            if (value == null) {
                value = context.ignoredValue(path);
            }
            if (!routing.equals(value)) {
                throw new MapperParsingException("External routing [" + routing + "] and document path routing [" + value + "] mismatch");
            }
        }
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // no need ot parse here, we either get the routing in the sourceToParse
        // or we don't have routing, if we get it in sourceToParse, we process it in preParse
        // which will always be called
    }

    @Override
    public boolean includeInObject() {
        return true;
    }

    @Override
    protected Field parseCreateField(ParseContext context) throws IOException {
        if (context.sourceToParse().routing() != null) {
            String routing = context.sourceToParse().routing();
            if (routing != null) {
                if (!indexed() && !stored()) {
                    context.ignoredValue(names.indexName(), routing);
                    return null;
                }
                return new Field(names.indexName(), routing, store, index);
            }
        }
        return null;

    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // if all are defaults, no sense to write it at all
        if (index == Defaults.INDEX && store == Defaults.STORE && required == Defaults.REQUIRED && path == Defaults.PATH) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (index != Defaults.INDEX) {
            builder.field("index", index.name().toLowerCase());
        }
        if (store != Defaults.STORE) {
            builder.field("store", store.name().toLowerCase());
        }
        if (required != Defaults.REQUIRED) {
            builder.field("required", required);
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
