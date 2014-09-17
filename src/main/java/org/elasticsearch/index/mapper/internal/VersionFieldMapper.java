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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatService;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.version;

/** Mapper for the _version field. */
public class VersionFieldMapper extends AbstractFieldMapper<Long> implements InternalMapper, RootMapper {

    public static final String NAME = "_version";
    public static final String CONTENT_TYPE = "_version";

    public static class Defaults {

        public static final String NAME = VersionFieldMapper.NAME;
        public static final float BOOST = 1.0f;
        public static final FieldType FIELD_TYPE = NumericDocValuesField.TYPE;

    }

    public static class Builder extends Mapper.Builder<Builder, VersionFieldMapper> {

        DocValuesFormatProvider docValuesFormat;

        public Builder() {
            super(Defaults.NAME);
        }

        @Override
        public VersionFieldMapper build(BuilderContext context) {
            return new VersionFieldMapper(docValuesFormat);
        }

        public Builder docValuesFormat(DocValuesFormatProvider docValuesFormat) {
            this.docValuesFormat = docValuesFormat;
            return this;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = version();
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals(DOC_VALUES_FORMAT)) {
                    String docValuesFormatName = fieldNode.toString();
                    builder.docValuesFormat(parserContext.docValuesFormatService().get(docValuesFormatName));
                }
            }
            return builder;
        }
    }

    private final ThreadLocal<Field> fieldCache = new ThreadLocal<Field>() {
        @Override
        protected Field initialValue() {
            return new NumericDocValuesField(NAME, -1L);
        }
    };

    public VersionFieldMapper() {
        this(null);
    }

    VersionFieldMapper(DocValuesFormatProvider docValuesFormat) {
        super(new Names(NAME, NAME, NAME, NAME), Defaults.BOOST, Defaults.FIELD_TYPE, null, null, null, null, docValuesFormat, null, null, null, ImmutableSettings.EMPTY);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        // see UidFieldMapper.parseCreateField
        final Field version = fieldCache.get();
        context.version(version);
        fields.add(version);
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // _version added in preparse
    }

    @Override
    public Long value(Object value) {
        if (value == null || (value instanceof Long)) {
            return (Long) value;
        } else {
            return Long.parseLong(value.toString());
        }
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        // In the case of nested docs, let's fill nested docs with version=0 so that Lucene doesn't write a Bitset for documents
        // that don't have the field
        for (int i = 1; i < context.docs().size(); i++) {
            final Document doc = context.docs().get(i);
            doc.add(new NumericDocValuesField(NAME, 0L));
        }
    }

    @Override
    public boolean includeInObject() {
        return false;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("long");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        if (!includeDefaults && (docValuesFormat == null || docValuesFormat.name().equals(defaultDocValuesFormat()))) {
            return builder;
        }

        builder.startObject(CONTENT_TYPE);
        if (docValuesFormat != null) {
            if (includeDefaults || !docValuesFormat.name().equals(defaultDocValuesFormat())) {
                builder.field(DOC_VALUES_FORMAT, docValuesFormat.name());
            }
        } else {
            String format = defaultDocValuesFormat();
            if (format == null) {
                format = DocValuesFormatService.DEFAULT_FORMAT;
            }
            builder.field(DOC_VALUES_FORMAT, format);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        if (mergeContext.mergeFlags().simulate()) {
            return;
        }
        AbstractFieldMapper<?> fieldMergeWith = (AbstractFieldMapper<?>) mergeWith;
        if (fieldMergeWith.docValuesFormatProvider() != null) {
            this.docValuesFormat = fieldMergeWith.docValuesFormatProvider();
        }
    }

    @Override
    public void close() {
        fieldCache.remove();
    }

    @Override
    public boolean hasDocValues() {
        return true;
    }
}
