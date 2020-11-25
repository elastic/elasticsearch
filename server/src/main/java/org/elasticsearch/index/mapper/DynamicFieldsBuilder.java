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

    abstract DynamicField newDynamicStringField(ParseContext context, String name);

    abstract DynamicField newDynamicLongField(ParseContext context, String name);

    abstract DynamicField newDynamicDoubleField(ParseContext context, String name);

    abstract DynamicField newDynamicBooleanField(ParseContext context, String name);

    abstract DynamicField newDynamicDateField(ParseContext context, String name, DateFormatter dateFormatter);

    abstract DynamicField newDynamicBinaryField(ParseContext context, String name);

    /**
     * Creates dynamic fields based on matching dynamic templates. If there are none, according to the current dynamic setting.
     */
    private static final class TemplateOrDelegate extends DynamicFieldsBuilder {
        private static final DynamicFieldsBuilder TEMPLATE_OR_CONCRETE = new TemplateOrDelegate(Concrete.INSTANCE);
        private static final DynamicFieldsBuilder TEMPLATE_OR_RUNTIME = new TemplateOrDelegate(Runtime.INSTANCE);

        private final DynamicFieldsBuilder delegate;

        private TemplateOrDelegate(DynamicFieldsBuilder delegate) {
            this.delegate = delegate;
        }

        @Override
        DynamicField newDynamicStringField(ParseContext context, String name) {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.STRING);
            return builder == null ? delegate.newDynamicStringField(context, name) : new DynamicField(builder);
        }

        @Override
        DynamicField newDynamicLongField(ParseContext context, String name) {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.LONG);
            return builder == null ? delegate.newDynamicLongField(context, name) : new DynamicField(builder);
        }

        @Override
        DynamicField newDynamicDoubleField(ParseContext context, String name) {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.DOUBLE);
            return builder == null ? delegate.newDynamicDoubleField(context, name) : new DynamicField(builder);
        }

        @Override
        final DynamicField newDynamicBooleanField(ParseContext context, String name) {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.BOOLEAN);
            return builder == null ? delegate.newDynamicBooleanField(context, name) : new DynamicField(builder);
        }

        @Override
        DynamicField newDynamicDateField(ParseContext context, String name, DateFormatter dateFormatter) {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.DATE, dateFormatter);
            return builder == null ? delegate.newDynamicDateField(context, name, dateFormatter) : new DynamicField(builder);
        }

        @Override
        DynamicField newDynamicBinaryField(ParseContext context, String name) {
            Mapper.Builder builder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.BINARY);
            return builder == null ? delegate.newDynamicBinaryField(context, name) : new DynamicField(builder);
        }
    }

    /**
     * Creates dynamic concrete fields, in the properties section
     */
    private static final class Concrete extends DynamicFieldsBuilder {
        private static final Concrete INSTANCE = new Concrete();

        @Override
        public DynamicField newDynamicStringField(ParseContext context, String name) {
            return new DynamicField(new TextFieldMapper.Builder(name, () -> context.indexAnalyzers().getDefaultIndexAnalyzer())
                .addMultiField(new KeywordFieldMapper.Builder("keyword").ignoreAbove(256)));
        }

        @Override
        public DynamicField newDynamicLongField(ParseContext context, String name) {
            return new DynamicField(new NumberFieldMapper.Builder(name, NumberFieldMapper.NumberType.LONG, context.indexSettings().getSettings()));
        }

        @Override
        public DynamicField newDynamicDoubleField(ParseContext context, String name) {
            // no templates are defined, we use float by default instead of double
            // since this is much more space-efficient and should be enough most of
            // the time
            return new DynamicField(new NumberFieldMapper.Builder(name,
                NumberFieldMapper.NumberType.FLOAT, context.indexSettings().getSettings()));
        }

        @Override
        public DynamicField newDynamicBooleanField(ParseContext context, String name) {
            return new DynamicField(new BooleanFieldMapper.Builder(name));
        }

        @Override
        public DynamicField newDynamicDateField(ParseContext context, String name, DateFormatter dateTimeFormatter) {
            Settings settings = context.indexSettings().getSettings();
            boolean ignoreMalformed = FieldMapper.IGNORE_MALFORMED_SETTING.get(settings);
            return new DynamicField(new DateFieldMapper.Builder(name, DateFieldMapper.Resolution.MILLISECONDS,
                dateTimeFormatter, ignoreMalformed, Version.indexCreated(settings)));
        }

        @Override
        DynamicField newDynamicBinaryField(ParseContext context, String name) {
            return new DynamicField(new BinaryFieldMapper.Builder(name));
        }
    }

    /**
     * Creates dynamic runtime fields, in the runtime section
     */
    private static final class Runtime extends DynamicFieldsBuilder {
        private static final Runtime INSTANCE = new Runtime();

        @Override
        public DynamicField newDynamicStringField(ParseContext context, String name) {
            return new DynamicField(context.getDynamicRuntimeFieldsBuilder().newDynamicStringField(name));
        }

        @Override
        public DynamicField newDynamicLongField(ParseContext context, String name) {
            return new DynamicField(context.getDynamicRuntimeFieldsBuilder().newDynamicLongField(name));
        }

        @Override
        public DynamicField newDynamicDoubleField(ParseContext context, String name) {
            return new DynamicField(context.getDynamicRuntimeFieldsBuilder().newDynamicDoubleField(name));
        }

        @Override
        public DynamicField newDynamicBooleanField(ParseContext context, String name) {
            return new DynamicField(context.getDynamicRuntimeFieldsBuilder().newDynamicBooleanField(name));
        }

        @Override
        public DynamicField newDynamicDateField(ParseContext context, String name, DateFormatter dateFormatter) {
            return new DynamicField(context.getDynamicRuntimeFieldsBuilder().newDynamicDateField(name, dateFormatter));
        }

        @Override
        DynamicField newDynamicBinaryField(ParseContext context, String name) {
            //binary runtime fields are not supported
            return Concrete.INSTANCE.newDynamicBinaryField(context, name);
        }
    }

    /**
     * Returns the builder for a dynamically created object mapper. Note that objects are always created under properties.
     */
    static Mapper.Builder newDynamicObjectBuilder(ParseContext context, String name) {
        //dynamic:runtime maps objects under properties, exactly like dynamic:true
        Mapper.Builder templateBuilder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.OBJECT);
        if (templateBuilder == null) {
            Version version = Version.indexCreated(context.indexSettings().getSettings());
            return new ObjectMapper.Builder(name, version).enabled(true);
        }
        return templateBuilder;
    }

    static DynamicField findStringTemplateBuilder(ParseContext context, String name) {
        Mapper.Builder templateBuilder = findTemplateBuilder(context, name, DynamicTemplate.XContentFieldType.STRING);
        return templateBuilder == null ? null : new DynamicField(templateBuilder);
    }

    static Mapper.Builder findTemplateBuilder(ParseContext context, String name, DynamicTemplate.XContentFieldType matchType) {
        return findTemplateBuilder(context, name, matchType, null);
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

    /**
     * Wraps a dynamically created field, which can either be a concrete field built through a {@link Mapper.Builder} that gets
     * mapped under properties or a {@link RuntimeFieldType} that gets mapped as part of the runtime section
     */
    static final class DynamicField {
        private final Mapper.Builder builder;
        private final RuntimeFieldType runtimeField;

        private DynamicField(Mapper.Builder builder) {
            this.builder = Objects.requireNonNull(builder);
            this.runtimeField = null;
        }

        private DynamicField(RuntimeFieldType runtimeField) {
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
