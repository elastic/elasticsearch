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
import org.apache.lucene.document.XStringField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatService;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatService;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.ParseContext.Document;
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
        public static final String INDEX_NAME = UidFieldMapper.NAME;

        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);
        public static final FieldType NESTED_FIELD_TYPE;

        static {
            FIELD_TYPE.setIndexed(true);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY);
            FIELD_TYPE.freeze();

            NESTED_FIELD_TYPE = new FieldType(FIELD_TYPE);
            NESTED_FIELD_TYPE.setStored(false);
            NESTED_FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, UidFieldMapper> {

        public Builder() {
            super(Defaults.NAME, Defaults.FIELD_TYPE);
            indexName = Defaults.INDEX_NAME;
        }

        @Override
        public UidFieldMapper build(BuilderContext context) {
            return new UidFieldMapper(name, indexName, docValues, postingsProvider, docValuesProvider, fieldDataSettings, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = uid();
            parseField(builder, builder.name, node, parserContext);
            return builder;
        }
    }

    public UidFieldMapper() {
        this(Defaults.NAME);
    }

    protected UidFieldMapper(String name) {
        this(name, name, null, null, null, null, ImmutableSettings.EMPTY);
    }

    protected UidFieldMapper(String name, String indexName, Boolean docValues, PostingsFormatProvider postingsFormat, DocValuesFormatProvider docValuesFormat, @Nullable Settings fieldDataSettings, Settings indexSettings) {
        super(new Names(name, indexName, indexName, name), Defaults.BOOST, new FieldType(Defaults.FIELD_TYPE), docValues,
                Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER, postingsFormat, docValuesFormat, null, null, fieldDataSettings, indexSettings);
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
    protected String defaultPostingFormat() {
        return "default";
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
                    doc.add(new XStringField(UidFieldMapper.NAME, uidField.stringValue(), Defaults.NESTED_FIELD_TYPE));
                }
            }
        }
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // nothing to do here, we either do it in post parse, or in pre parse.
    }

    @Override
    public boolean includeInObject() {
        return false;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        Field uid = new XStringField(NAME, Uid.createUid(context.stringBuilder(), context.type(), context.id()), Defaults.FIELD_TYPE);
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
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        // if defaults, don't output
        if (!includeDefaults && customFieldDataSettings == null
                && (postingsFormat == null || postingsFormat.name().equals(defaultPostingFormat()))
                && (docValuesFormat == null || docValuesFormat.name().equals(defaultDocValuesFormat()))) {
            return builder;
        }

        builder.startObject(CONTENT_TYPE);

        if (postingsFormat != null) {
            if (includeDefaults || !postingsFormat.name().equals(defaultPostingFormat())) {
                builder.field("postings_format", postingsFormat.name());
            }
        } else if (includeDefaults) {
            String format = defaultPostingFormat();
            if (format == null) {
                format = PostingsFormatService.DEFAULT_FORMAT;
            }
            builder.field("postings_format", format);
        }

        if (docValuesFormat != null) {
            if (includeDefaults || !docValuesFormat.name().equals(defaultDocValuesFormat())) {
                builder.field(DOC_VALUES_FORMAT, docValuesFormat.name());
            }
        } else if (includeDefaults) {
            String format = defaultDocValuesFormat();
            if (format == null) {
                format = DocValuesFormatService.DEFAULT_FORMAT;
            }
            builder.field(DOC_VALUES_FORMAT, format);
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
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        AbstractFieldMapper<?> fieldMergeWith = (AbstractFieldMapper<?>) mergeWith;
        // do nothing here, no merging, but also no exception
        if (!mergeContext.mergeFlags().simulate()) {
            // apply changeable values
            if (fieldMergeWith.postingsFormatProvider() != null) {
                this.postingsFormat = fieldMergeWith.postingsFormatProvider();
            }
        }
    }
}
