/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;

import java.io.IOException;

/**
 * Encapsulates the logic for dynamically creating fields based on values parsed from incoming documents.
 */
abstract class DynamicFieldsBuilder {

    static final DynamicFieldsBuilder TEMPLATE_OR_CONCRETE = new TemplateOrDelegate(Concrete.INSTANCE);
    static final DynamicFieldsBuilder TEMPLATE_OR_RUNTIME = new TemplateOrDelegate(Runtime.INSTANCE);

    abstract void newDynamicStringField(ParseContext context, String name) throws IOException;

    abstract void newDynamicLongField(ParseContext context, String name) throws IOException;

    abstract void newDynamicDoubleField(ParseContext context, String name) throws IOException;

    abstract void newDynamicBooleanField(ParseContext context, String name) throws IOException;

    abstract void newDynamicDateField(ParseContext context, String name, DateFormatter dateFormatter) throws IOException;

    abstract void newDynamicBinaryField(ParseContext context, String name) throws IOException;

    /**
     * Returns the builder for a dynamically created object mapper. Note that objects are always created under properties.
     */
    final Mapper.Builder newDynamicObjectBuilder(ParseContext context, String name) {
        //dynamic:runtime maps objects under properties, exactly like dynamic:true
        Mapper.Builder templateBuilder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.OBJECT);
        if (templateBuilder == null) {
            return new ObjectMapper.Builder(name, context.indexSettings().getIndexVersionCreated()).enabled(true);
        }
        return templateBuilder;
    }

    final void newDynamicStringFieldFromTemplate(ParseContext context, String name) throws IOException {
        Mapper.Builder templateBuilder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.STRING);
        if (templateBuilder != null) {
            Concrete.INSTANCE.createDynamicField(templateBuilder, context);
        }
    }

    final Mapper getObjectMapperFromTemplate(ParseContext context, String name) {
        Mapper.Builder templateBuilder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.OBJECT);
        return templateBuilder == null ? null : templateBuilder.build(context.path());
    }

    protected static Mapper.Builder findTemplateBuilder(ParseContext context, String name, DynamicTemplate.XContentFieldType matchType) {
        return findTemplateBuilder(context, name, matchType, null);
    }

    /**
     * Creates dynamic fields based on matching dynamic templates. If there are none, according to the current dynamic setting.
     */
    private static final class TemplateOrDelegate extends DynamicFieldsBuilder {
        private final DynamicFieldsBuilder delegate;

        private TemplateOrDelegate(DynamicFieldsBuilder delegate) {
            this.delegate = delegate;
        }

        @Override
        void newDynamicStringField(ParseContext context, String name) throws IOException {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.STRING);
            if (builder == null) {
                delegate.newDynamicStringField(context, name);
            } else {
                Concrete.INSTANCE.createDynamicField(builder, context);
            }
        }

        @Override
        void newDynamicLongField(ParseContext context, String name) throws IOException {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.LONG);
            if (builder == null) {
                delegate.newDynamicLongField(context, name);
            } else {
                Concrete.INSTANCE.createDynamicField(builder, context);
            }
        }

        @Override
        void newDynamicDoubleField(ParseContext context, String name) throws IOException {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.DOUBLE);
            if (builder == null) {
                delegate.newDynamicDoubleField(context, name);
            } else {
                Concrete.INSTANCE.createDynamicField(builder, context);
            }
        }

        @Override
        final void newDynamicBooleanField(ParseContext context, String name) throws IOException {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.BOOLEAN);
            if (builder == null) {
                delegate.newDynamicBooleanField(context, name);
            } else {
                Concrete.INSTANCE.createDynamicField(builder, context);
            }
        }

        @Override
        void newDynamicDateField(ParseContext context, String name, DateFormatter dateFormatter) throws IOException {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.DATE, dateFormatter);
            if (builder == null) {
                delegate.newDynamicDateField(context, name, dateFormatter);
            } else {
                Concrete.INSTANCE.createDynamicField(builder, context);
            }
        }

        @Override
        void newDynamicBinaryField(ParseContext context, String name) throws IOException {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.BINARY);
            if (builder == null) {
                delegate.newDynamicBinaryField(context, name);
            } else {
                Concrete.INSTANCE.createDynamicField(builder, context);
            }
        }
    }

    /**
     * Creates dynamic concrete fields, in the properties section
     */
    private static final class Concrete extends DynamicFieldsBuilder {
        private static final Concrete INSTANCE = new Concrete(DocumentParser::parseObjectOrField);

        private final CheckedBiConsumer<ParseContext, Mapper, IOException> parseField;

        Concrete(CheckedBiConsumer<ParseContext, Mapper, IOException> parseField) {
            this.parseField = parseField;
        }

        private void createDynamicField(Mapper.Builder builder, ParseContext context) throws IOException {
            Mapper mapper = builder.build(context.path());
            context.addDynamicMapper(mapper);
            parseField.accept(context, mapper);
        }

        @Override
        public void newDynamicStringField(ParseContext context, String name) throws IOException {
            createDynamicField(new TextFieldMapper.Builder(name, context.indexAnalyzers()).addMultiField(
                    new KeywordFieldMapper.Builder("keyword").ignoreAbove(256)), context);
        }

        @Override
        public void newDynamicLongField(ParseContext context, String name) throws IOException {
            createDynamicField(
                new NumberFieldMapper.Builder(name, NumberFieldMapper.NumberType.LONG, context.indexSettings().getSettings()), context);
        }

        @Override
        public void newDynamicDoubleField(ParseContext context, String name) throws IOException {
            // no templates are defined, we use float by default instead of double
            // since this is much more space-efficient and should be enough most of
            // the time
            createDynamicField(new NumberFieldMapper.Builder(name,
                NumberFieldMapper.NumberType.FLOAT, context.indexSettings().getSettings()), context);
        }

        @Override
        public void newDynamicBooleanField(ParseContext context, String name) throws IOException {
            createDynamicField(new BooleanFieldMapper.Builder(name), context);
        }

        @Override
        public void newDynamicDateField(ParseContext context, String name, DateFormatter dateTimeFormatter) throws IOException {
            Settings settings = context.indexSettings().getSettings();
            boolean ignoreMalformed = FieldMapper.IGNORE_MALFORMED_SETTING.get(settings);
            createDynamicField(new DateFieldMapper.Builder(name, DateFieldMapper.Resolution.MILLISECONDS,
                dateTimeFormatter, ignoreMalformed, context.indexSettings().getIndexVersionCreated()), context);
        }

        @Override
        void newDynamicBinaryField(ParseContext context, String name) throws IOException {
            createDynamicField(new BinaryFieldMapper.Builder(name), context);
        }
    }

    /**
     * Creates dynamic runtime fields, in the runtime section
     */
    private static final class Runtime extends DynamicFieldsBuilder {
        private static final Runtime INSTANCE = new Runtime();

        @Override
        public void newDynamicStringField(ParseContext context, String name) {
            RuntimeFieldType runtimeFieldType = context.getDynamicRuntimeFieldsBuilder().newDynamicStringField(name);
            context.addDynamicRuntimeField(runtimeFieldType);
        }

        @Override
        public void newDynamicLongField(ParseContext context, String name) {
            RuntimeFieldType runtimeFieldType = context.getDynamicRuntimeFieldsBuilder().newDynamicLongField(name);
            context.addDynamicRuntimeField(runtimeFieldType);
        }

        @Override
        public void newDynamicDoubleField(ParseContext context, String name) {
            RuntimeFieldType runtimeFieldType = context.getDynamicRuntimeFieldsBuilder().newDynamicDoubleField(name);
            context.addDynamicRuntimeField(runtimeFieldType);
        }

        @Override
        public void newDynamicBooleanField(ParseContext context, String name) {
            RuntimeFieldType runtimeFieldType = context.getDynamicRuntimeFieldsBuilder().newDynamicBooleanField(name);
            context.addDynamicRuntimeField(runtimeFieldType);
        }

        @Override
        public void newDynamicDateField(ParseContext context, String name, DateFormatter dateFormatter) {
            RuntimeFieldType runtimeFieldType = context.getDynamicRuntimeFieldsBuilder().newDynamicDateField(name, dateFormatter);
            context.addDynamicRuntimeField(runtimeFieldType);
        }

        @Override
        void newDynamicBinaryField(ParseContext context, String name) throws IOException {
            //binary runtime fields are not supported
            Concrete.INSTANCE.newDynamicBinaryField(context, name);
        }
    }

    /**
     * Find a template. Returns {@code null} if no template could be found.
     * @param context        the parse context for this document
     * @param name           the current field name
     * @param matchType      the type of the field in the json document or null if unknown
     * @param dateFormatter  a date formatter to use if the type is a date, null if not a date or is using the default format
     * @return a mapper builder, or null if there is no template for such a field
     */
    static Mapper.Builder findTemplateBuilder(ParseContext context, String name, DynamicTemplate.XContentFieldType matchType,
                                              DateFormatter dateFormatter) {
        DynamicTemplate dynamicTemplate = context.root().findTemplate(context.path(), name, matchType);
        if (dynamicTemplate == null) {
            return null;
        }
        String dynamicType = matchType.defaultMappingType();
        Mapper.TypeParser.ParserContext parserContext = context.parserContext(dateFormatter);
        String mappingType = dynamicTemplate.mappingType(dynamicType);
        Mapper.TypeParser typeParser = parserContext.typeParser(mappingType);
        if (typeParser == null) {
            throw new MapperParsingException("failed to find type parsed [" + mappingType + "] for [" + name + "]");
        }
        return typeParser.parse(name, dynamicTemplate.mappingForName(name, dynamicType), parserContext);
    }
}
