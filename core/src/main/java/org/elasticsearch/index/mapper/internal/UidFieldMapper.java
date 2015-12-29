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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 *
 */
public class UidFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_uid";

    public static final String CONTENT_TYPE = "_uid";

    public static class Defaults {
        public static final String NAME = UidFieldMapper.NAME;

        public static final MappedFieldType FIELD_TYPE = new UidFieldType();
        public static final MappedFieldType NESTED_FIELD_TYPE;

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setName(NAME);
            FIELD_TYPE.freeze();

            NESTED_FIELD_TYPE = FIELD_TYPE.clone();
            NESTED_FIELD_TYPE.setStored(false);
            NESTED_FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, UidFieldMapper> {

        public Builder(MappedFieldType existing) {
            super(Defaults.NAME, existing == null ? Defaults.FIELD_TYPE : existing, Defaults.FIELD_TYPE);
            indexName = Defaults.NAME;
        }

        @Override
        public UidFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            fieldType.setHasDocValues(context.indexCreatedVersion().before(Version.V_2_0_0_beta1));
            return new UidFieldMapper(fieldType, defaultFieldType, context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            if (parserContext.indexVersionCreated().onOrAfter(Version.V_2_0_0_beta1)) {
                throw new MapperParsingException(NAME + " is not configurable");
            }
            Builder builder = new Builder(parserContext.mapperService().fullName(NAME));
            parseField(builder, builder.name, node, parserContext);
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(Settings indexSettings, MappedFieldType fieldType, String typeName) {
            return new UidFieldMapper(indexSettings, fieldType);
        }
    }

    static final class UidFieldType extends MappedFieldType {

        public UidFieldType() {
            setFieldDataType(new FieldDataType("string"));
        }

        protected UidFieldType(UidFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new UidFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Uid value(Object value) {
            if (value == null) {
                return null;
            }
            return Uid.createUid(value.toString());
        }
    }

    private UidFieldMapper(Settings indexSettings, MappedFieldType existing) {
        this(existing == null ? Defaults.FIELD_TYPE.clone() : existing, Defaults.FIELD_TYPE, indexSettings);
    }

    private UidFieldMapper(MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings) {
        super(NAME, fieldType, defaultFieldType, indexSettings);
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
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        Field uid = new Field(NAME, Uid.createUid(context.stringBuilder(), context.type(), context.id()), Defaults.FIELD_TYPE);
        context.uid(uid);
        fields.add(uid);
        if (fieldType().hasDocValues()) {
            fields.add(new BinaryDocValuesField(NAME, new BytesRef(uid.stringValue())));
        }
    }

    public Term term(String uid) {
        return new Term(fieldType().name(), fieldType().indexedValueForSearch(uid));
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
        if (!includeDefaults && hasCustomFieldDataSettings() == false) {
            return builder;
        }

        builder.startObject(CONTENT_TYPE);

        if (includeDefaults || hasCustomFieldDataSettings()) {
            builder.field("fielddata", (Map) fieldType().fieldDataType().getSettings().getAsMap());
        }

        builder.endObject();
        return builder;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        // do nothing here, no merging, but also no exception
    }
}
