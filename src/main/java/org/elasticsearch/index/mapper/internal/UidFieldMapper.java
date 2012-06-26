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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.uid;

/**
 *
 */
public class UidFieldMapper extends AbstractFieldMapper<Uid> implements InternalMapper, RootMapper {

    public static final String NAME = "_uid".intern();

    public static final Term TERM_FACTORY = new Term(NAME, "");

    public static final String CONTENT_TYPE = "_uid";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = UidFieldMapper.NAME;
        public static final Field.Index INDEX = Field.Index.NOT_ANALYZED;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = false; // we store payload
    }

    public static class Builder extends Mapper.Builder<Builder, UidFieldMapper> {

        protected String indexName;

        public Builder() {
            super(Defaults.NAME);
            this.indexName = name;
        }

        @Override
        public UidFieldMapper build(BuilderContext context) {
            return new UidFieldMapper(name, indexName);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return uid();
        }
    }

    private ThreadLocal<UidField> fieldCache = new ThreadLocal<UidField>() {
        @Override
        protected UidField initialValue() {
            return new UidField(names().indexName(), "", 0);
        }
    };

    public UidFieldMapper() {
        this(Defaults.NAME);
    }

    protected UidFieldMapper(String name) {
        this(name, name);
    }

    protected UidFieldMapper(String name, String indexName) {
        super(new Names(name, indexName, indexName, name), Defaults.INDEX, Field.Store.YES, Defaults.TERM_VECTOR, Defaults.BOOST,
                Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        // if we have the id provided, fill it, and parse now
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
        // if we did not have the id as part of the sourceToParse, then we need to parse it here
        // it would have been filled in the _id parse phase
        if (context.sourceToParse().id() == null) {
            super.parse(context);
            // since we did not have the uid in the pre phase, we did not add it automatically to the nested docs
            // as they were created we need to make sure we add it to all the nested docs...
            if (context.docs().size() > 1) {
                UidField uidField = (UidField) context.rootDoc().getFieldable(UidFieldMapper.NAME);
                assert uidField != null;
                // we need to go over the docs and add it...
                for (int i = 1; i < context.docs().size(); i++) {
                    // we don't need to add it as a full uid field in nested docs, since we don't need versioning
                    context.docs().get(i).add(new Field(UidFieldMapper.NAME, uidField.uid(), Field.Store.NO, Field.Index.NOT_ANALYZED));
                }
            }
        }
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // nothing to do here, we either do it in post parse, or in pre parse.
    }

    @Override
    public void validate(ParseContext context) throws MapperParsingException {
    }

    @Override
    public boolean includeInObject() {
        return false;
    }

    @Override
    protected Fieldable parseCreateField(ParseContext context) throws IOException {
        context.uid(Uid.createUid(context.stringBuilder(), context.type(), context.id()));
        // so, caching uid stream and field is fine
        // since we don't do any mapping parsing without immediate indexing
        // and, when percolating, we don't index the uid
        UidField field = fieldCache.get();
        field.setUid(context.uid());
        return field; // version get updated by the engine
    }

    @Override
    public Uid value(Fieldable field) {
        return Uid.createUid(field.stringValue());
    }

    @Override
    public Uid valueFromString(String value) {
        return Uid.createUid(value);
    }

    @Override
    public String valueAsString(Fieldable field) {
        return field.stringValue();
    }

    @Override
    public String indexedValue(String value) {
        return value;
    }

    public Term term(String type, String id) {
        return term(Uid.createUid(type, id));
    }

    public Term term(String uid) {
        return names().createIndexNameTerm(uid);
    }

    @Override
    public void close() {
        fieldCache.remove();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // for now, don't output it at all
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // do nothing here, no merging, but also no exception
    }
}
