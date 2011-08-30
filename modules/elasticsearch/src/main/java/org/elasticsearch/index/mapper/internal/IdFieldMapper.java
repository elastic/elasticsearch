/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.InternalMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.RootMapper;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.*;
import static org.elasticsearch.index.mapper.core.TypeParsers.*;

/**
 * @author kimchy (shay.banon)
 */
public class IdFieldMapper extends AbstractFieldMapper<String> implements InternalMapper, RootMapper {

    public static final String NAME = "_id";

    public static final String CONTENT_TYPE = "_id";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = IdFieldMapper.NAME;
        public static final String INDEX_NAME = IdFieldMapper.NAME;
        public static final Field.Index INDEX = Field.Index.NO;
        public static final Field.Store STORE = Field.Store.NO;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
        public static final String PATH = null;
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, IdFieldMapper> {

        private String path = Defaults.PATH;

        public Builder() {
            super(Defaults.NAME);
            indexName = Defaults.INDEX_NAME;
            store = Defaults.STORE;
            index = Defaults.INDEX;
            omitNorms = Defaults.OMIT_NORMS;
            omitTermFreqAndPositions = Defaults.OMIT_TERM_FREQ_AND_POSITIONS;
        }

        public Builder path(String path) {
            this.path = path;
            return builder;
        }

        @Override public IdFieldMapper build(BuilderContext context) {
            return new IdFieldMapper(name, indexName, index, store, termVector, boost, omitNorms, omitTermFreqAndPositions, path);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
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
        this(Defaults.NAME, Defaults.INDEX_NAME, Defaults.INDEX);
    }

    public IdFieldMapper(Field.Index index) {
        this(Defaults.NAME, Defaults.INDEX_NAME, index);
    }

    protected IdFieldMapper(String name, String indexName, Field.Index index) {
        this(name, indexName, index, Defaults.STORE, Defaults.TERM_VECTOR, Defaults.BOOST,
                Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS, Defaults.PATH);
    }

    protected IdFieldMapper(String name, String indexName, Field.Index index, Field.Store store, Field.TermVector termVector,
                            float boost, boolean omitNorms, boolean omitTermFreqAndPositions, String path) {
        super(new Names(name, indexName, indexName, name), index, store, termVector, boost, omitNorms, omitTermFreqAndPositions,
                Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER);
        this.path = path;
    }

    public String path() {
        return this.path;
    }

    public String value(Document document) {
        Fieldable field = document.getFieldable(names.indexName());
        return field == null ? null : value(field);
    }

    @Override public String value(Fieldable field) {
        return field.stringValue();
    }

    @Override public String valueFromString(String value) {
        return value;
    }

    @Override public String valueAsString(Fieldable field) {
        return value(field);
    }

    @Override public String indexedValue(String value) {
        return value;
    }

    @Override public void preParse(ParseContext context) throws IOException {
        if (context.sourceToParse().id() != null) {
            context.id(context.sourceToParse().id());
            super.parse(context);
        }
    }

    @Override public void postParse(ParseContext context) throws IOException {
        if (context.id() == null && !context.sourceToParse().flyweight()) {
            throw new MapperParsingException("No id found while parsing the content source");
        }
        // it either get built in the preParse phase, or get parsed...
    }

    @Override public void parse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override public void validate(ParseContext context) throws MapperParsingException {
    }

    @Override public boolean includeInObject() {
        return true;
    }

    @Override protected Field parseCreateField(ParseContext context) throws IOException {
        XContentParser parser = context.parser();
        if (parser.currentName() != null && parser.currentName().equals(Defaults.NAME) && parser.currentToken().isValue()) {
            // we are in the parse Phase
            String id = parser.text();
            if (context.id() != null && !context.id().equals(id)) {
                throw new MapperParsingException("Provided id [" + context.id() + "] does not match the content one [" + id + "]");
            }
            context.id(id);
            if (index == Field.Index.NO && store == Field.Store.NO) {
                return null;
            }
            return new Field(names.indexName(), false, context.id(), store, index, termVector);
        } else {
            // we are in the pre/post parse phase
            if (index == Field.Index.NO && store == Field.Store.NO) {
                return null;
            }
            return new Field(names.indexName(), false, context.id(), store, index, termVector);
        }
    }

    @Override protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // if all are defaults, no sense to write it at all
        if (store == Defaults.STORE && index == Defaults.INDEX) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (store != Defaults.STORE) {
            builder.field("store", store.name().toLowerCase());
        }
        if (index != Defaults.INDEX) {
            builder.field("index", index.name().toLowerCase());
        }
        builder.endObject();
        return builder;
    }

    @Override public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // do nothing here, no merging, but also no exception
    }
}
