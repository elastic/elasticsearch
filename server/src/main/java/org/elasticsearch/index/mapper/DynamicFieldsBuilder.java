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

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;

import java.util.Objects;

/**
 * Encapsulates the logic for dynamically creating fields based on values parsed from incoming documents.
 */
abstract class DynamicFieldsBuilder {

    /**
     * Returns the appropriate dynamic fields builder given the current dynamic setting
     */
    static DynamicFieldsBuilder forDynamic(ObjectMapper.Dynamic dynamic) {
        assert dynamic.canCreateDynamicFields();
        if (dynamic == ObjectMapper.Dynamic.TRUE) {
            return TemplateOrDelegate.TEMPLATE_OR_CONCRETE;
        }
        if (dynamic == ObjectMapper.Dynamic.RUNTIME) {
            return TemplateOrDelegate.TEMPLATE_OR_RUNTIME;
        }
        throw new IllegalStateException(dynamic + " is neither " + ObjectMapper.Dynamic.TRUE + " nor " + ObjectMapper.Dynamic.RUNTIME);
    }

    abstract Field newDynamicStringField(ParseContext context, String name);

    abstract Field newDynamicLongField(ParseContext context, String name);

    abstract Field newDynamicDoubleField(ParseContext context, String name);

    abstract Field newDynamicBooleanField(ParseContext context, String name);

    abstract Field newDynamicDateField(ParseContext context, String name, DateFormatter dateFormatter);

    abstract Field newDynamicBinaryField(ParseContext context, String name);

    private static final class TemplateOrDelegate extends DynamicFieldsBuilder {
        private static final DynamicFieldsBuilder TEMPLATE_OR_CONCRETE = new TemplateOrDelegate(Concrete.INSTANCE);
        private static final DynamicFieldsBuilder TEMPLATE_OR_RUNTIME = new TemplateOrDelegate(Runtime.INSTANCE);

        private final DynamicFieldsBuilder delegate;

        private TemplateOrDelegate(DynamicFieldsBuilder delegate) {
            this.delegate = delegate;
        }

        @Override
        Field newDynamicStringField(ParseContext context, String name) {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.STRING);
            return builder == null ? delegate.newDynamicStringField(context, name) : new Field(builder);
        }

        @Override
        Field newDynamicLongField(ParseContext context, String name) {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.LONG);
            return builder == null ? delegate.newDynamicLongField(context, name) : new Field(builder);
        }

        @Override
        Field newDynamicDoubleField(ParseContext context, String name) {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.DOUBLE);
            return builder == null ? delegate.newDynamicDoubleField(context, name) : new Field(builder);
        }

        @Override
        final Field newDynamicBooleanField(ParseContext context, String name) {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.BOOLEAN);
            return builder == null ? delegate.newDynamicBooleanField(context, name) : new Field(builder);
        }

        @Override
        Field newDynamicDateField(ParseContext context, String name, DateFormatter dateFormatter) {
            Mapper.Builder builder = findTemplateBuilderForDate(context, name, dateFormatter);
            return builder == null ? delegate.newDynamicDateField(context, name, dateFormatter) : new Field(builder);
        }

        @Override
        Field newDynamicBinaryField(ParseContext context, String name) {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.BINARY);
            return builder == null ? delegate.newDynamicBinaryField(context, name) : new Field(builder);
        }
    }

    private static final class Concrete extends DynamicFieldsBuilder {
        private static final Concrete INSTANCE = new Concrete();

        @Override
        public Field newDynamicStringField(ParseContext context, String name) {
            return new Field(new TextFieldMapper.Builder(name, () -> context.indexAnalyzers().getDefaultIndexAnalyzer())
                .addMultiField(new KeywordFieldMapper.Builder("keyword").ignoreAbove(256)));
        }

        @Override
        public Field newDynamicLongField(ParseContext context, String name) {
            return new Field(new NumberFieldMapper.Builder(name, NumberFieldMapper.NumberType.LONG, context.indexSettings().getSettings()));
        }

        @Override
        public Field newDynamicDoubleField(ParseContext context, String name) {
            // no templates are defined, we use float by default instead of double
            // since this is much more space-efficient and should be enough most of
            // the time
            return new Field(new NumberFieldMapper.Builder(name,
                NumberFieldMapper.NumberType.FLOAT, context.indexSettings().getSettings()));
        }

        @Override
        public Field newDynamicBooleanField(ParseContext context, String name) {
            return new Field(new BooleanFieldMapper.Builder(name));
        }

        @Override
        public Field newDynamicDateField(ParseContext context, String name, DateFormatter dateTimeFormatter) {
            Settings settings = context.indexSettings().getSettings();
            boolean ignoreMalformed = FieldMapper.IGNORE_MALFORMED_SETTING.get(settings);
            return new Field(new DateFieldMapper.Builder(name, DateFieldMapper.Resolution.MILLISECONDS,
                dateTimeFormatter, ignoreMalformed, Version.indexCreated(settings)));
        }

        @Override
        Field newDynamicBinaryField(ParseContext context, String name) {
            return new Field(new BinaryFieldMapper.Builder(name));
        }
    }

    private static final class Runtime extends DynamicFieldsBuilder {
        private static final Runtime INSTANCE = new Runtime();

        @Override
        public Field newDynamicStringField(ParseContext context, String name) {
            return new Field(context.getDynamicRuntimeFieldsBuilder().newDynamicStringField(name));
        }

        @Override
        public Field newDynamicLongField(ParseContext context, String name) {
            return new Field(context.getDynamicRuntimeFieldsBuilder().newDynamicLongField(name));
        }

        @Override
        public Field newDynamicDoubleField(ParseContext context, String name) {
            return new Field(context.getDynamicRuntimeFieldsBuilder().newDynamicDoubleField(name));
        }

        @Override
        public Field newDynamicBooleanField(ParseContext context, String name) {
            return new Field(context.getDynamicRuntimeFieldsBuilder().newDynamicBooleanField(name));
        }

        @Override
        public Field newDynamicDateField(ParseContext context, String name, DateFormatter dateFormatter) {
            return new Field(context.getDynamicRuntimeFieldsBuilder().newDynamicDateField(name, dateFormatter));
        }

        @Override
        Field newDynamicBinaryField(ParseContext context, String name) {
            //binary runtime fields are not supported
            return Concrete.INSTANCE.newDynamicBinaryField(context, name);
        }
    }

    static Mapper.Builder newDynamicObjectBuilder(ParseContext context, String name) {
        //dynamic:runtime maps objects under properties, exactly like dynamic:true
        Mapper.Builder templateBuilder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.OBJECT);
        if (templateBuilder == null) {
            Version version = Version.indexCreated(context.indexSettings().getSettings());
            return new ObjectMapper.Builder(name, version).enabled(true);
        }
        return templateBuilder;
    }

    static Mapper.Builder findObjectTemplateBuilder(ParseContext context, String name) {
        return findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.OBJECT);
    }

    static DynamicFieldsBuilder.Field findStringTemplateBuilder(ParseContext context, String name) {
        Mapper.Builder templateBuilder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.STRING);
        return templateBuilder == null ? null : new Field(templateBuilder);
    }

    private static Mapper.Builder findTemplateBuilder(ParseContext context, String name, DynamicTemplate.XContentFieldType matchType) {
        assert matchType != DynamicTemplate.XContentFieldType.DATE;
        return findTemplateBuilder(context, name, matchType, null);
    }

    private static Mapper.Builder findTemplateBuilderForDate(ParseContext context, String name, DateFormatter dateFormatter) {
        assert dateFormatter != null;
        return findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.DATE, dateFormatter);
    }

    /**
     * Find a template. Returns {@code null} if no template could be found.
     * @param name        the field name
     * @param matchType   the type of the field in the json document or null if unknown
     * @param dateFormatter  a date formatter to use if the type is a date, null if not a date or is using the default format
     * @return a mapper builder, or null if there is no template for such a field
     */
    private static Mapper.Builder findTemplateBuilder(ParseContext context,
                                                      String name,
                                                      DynamicTemplate.XContentFieldType matchType,
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

    static final class Field {
        private final Mapper.Builder builder;
        private final RuntimeFieldType runtimeField;

        private Field(Mapper.Builder builder) {
            this.builder = Objects.requireNonNull(builder);
            this.runtimeField = null;
        }

        private Field(RuntimeFieldType runtimeField) {
            this.builder = null;
            this.runtimeField = Objects.requireNonNull(runtimeField);
        }

        boolean isRuntimeField() {
            return runtimeField != null;
        }

        RuntimeFieldType getRuntimeField() {
            assert runtimeField != null;
            return runtimeField;
        }

        Mapper.Builder getBuilder() {
            assert builder != null;
            return builder;
        }
    }
}
