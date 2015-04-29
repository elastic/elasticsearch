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

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.InternalMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.RootMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.uid;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 *
 */
public class UidFieldMapper extends AbstractFieldMapper<Uid> implements InternalMapper, RootMapper {

    public static final String NAME = "_uid";

    public static final String CONTENT_TYPE = "_uid";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = UidFieldMapper.NAME;

        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);
        public static final FieldType NESTED_FIELD_TYPE;

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();

            NESTED_FIELD_TYPE = new FieldType(FIELD_TYPE);
            NESTED_FIELD_TYPE.setStored(false);
            NESTED_FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, UidFieldMapper> {

        public Builder() {
            super(Defaults.NAME, Defaults.FIELD_TYPE);
            indexName = Defaults.NAME;
        }

        @Override
        public UidFieldMapper build(BuilderContext context) {
            return new UidFieldMapper(name, indexName, docValues, fieldDataSettings, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = uid();
            if (parserContext.indexVersionCreated().onOrAfter(Version.V_2_0_0)) {
                throw new MapperParsingException(NAME + " is not configurable");
            }
            parseField(builder, builder.name, node, parserContext);
            return builder;
        }
    }

    public UidFieldMapper(Settings indexSettings) {
        this(Defaults.NAME, Defaults.NAME, null, null, indexSettings);
    }

    protected UidFieldMapper(String name, String indexName, Boolean docValues, @Nullable Settings fieldDataSettings, Settings indexSettings) {
        super(new Names(name, indexName, indexName, name), Defaults.BOOST, new FieldType(Defaults.FIELD_TYPE), docValuesEnabled(docValues, indexSettings),
                Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER, null, null, fieldDataSettings, indexSettings);
    }
    
    static Boolean docValuesEnabled(Boolean docValues, Settings indexSettings) {
        if (Version.indexCreated(indexSettings).onOrAfter(Version.V_2_0_0)) {
            return false; // explicitly disable doc values for 2.0+, for now
        }
        return docValues;
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
                final IndexableField uidField = context.rootDoc().getField(UidFieldMapper.NAME);
                assert uidField != null;
                // we need to go over the docs and add it...
                for (int i = 1; i < context.docs().size(); i++) {
                    final Document doc = context.docs().get(i);
                    doc.add(new Field(UidFieldMapper.NAME, uidField.stringValue(), Defaults.NESTED_FIELD_TYPE));
                }
            }
        }
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        // nothing to do here, we either do it in post parse, or in pre parse.
        return null;
    }

    @Override
    public boolean includeInObject() {
        return false;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        Field uid = new Field(NAME, Uid.createUid(context.stringBuilder(), context.type(), context.id()), Defaults.FIELD_TYPE);
        context.uid(uid);
        fields.add(uid);
        if (hasDocValues()) {
            fields.add(new BinaryDocValuesField(NAME, new BytesRef(uid.stringValue())));
        }
    }

    @Override
    public Uid value(Object value) {
        if (value == null) {
            return null;
        }
        return Uid.createUid(value.toString());
    }

    public Term term(String type, String id) {
        return term(Uid.createUid(type, id));
    }

    public Term term(String uid) {
        return names().createIndexNameTerm(uid);
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

        // if defaults, don't output
        if (!includeDefaults && customFieldDataSettings == null) {
            return builder;
        }

        builder.startObject(CONTENT_TYPE);

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
