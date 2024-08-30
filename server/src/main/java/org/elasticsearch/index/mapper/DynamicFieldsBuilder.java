/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.mapper.ObjectMapper.Dynamic;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.DateTimeException;
import java.util.Map;

/**
 * Encapsulates the logic for dynamically creating fields as part of document parsing.
 * Fields can be mapped under properties, as concrete fields that get indexed,
 * or as runtime fields that are evaluated at search-time and have no indexing overhead.
 * Objects get dynamically mapped only under dynamic:true.
 */
final class DynamicFieldsBuilder {
    private static final Concrete CONCRETE = new Concrete(DocumentParser::parseObjectOrField);
    static final DynamicFieldsBuilder DYNAMIC_TRUE = new DynamicFieldsBuilder(CONCRETE);
    static final DynamicFieldsBuilder DYNAMIC_RUNTIME = new DynamicFieldsBuilder(new Runtime());

    private final Strategy strategy;

    private DynamicFieldsBuilder(Strategy strategy) {
        this.strategy = strategy;
    }

    /**
     * Creates a dynamic field based on the value of the current token being parsed from an incoming document.
     * Makes decisions based on the type of the field being found, looks at matching dynamic templates and
     * delegates to the appropriate strategy which depends on the current dynamic mode.
     * The strategy defines if fields are going to be mapped as ordinary or runtime fields.
     */
    boolean createDynamicFieldFromValue(final DocumentParserContext context, String name) throws IOException {
        XContentParser.Token token = context.parser().currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            String text = context.parser().text();

            boolean parseableAsLong = false;
            try {
                Long.parseLong(text);
                parseableAsLong = true;
            } catch (NumberFormatException e) {
                // not a long number
            }

            boolean parseableAsDouble = false;
            try {
                Double.parseDouble(text);
                parseableAsDouble = true;
            } catch (NumberFormatException e) {
                // not a double number
            }

            if (parseableAsLong && context.root().numericDetection()) {
                return createDynamicField(
                    context,
                    name,
                    DynamicTemplate.XContentFieldType.LONG,
                    () -> strategy.newDynamicLongField(context, name)
                );
            } else if (parseableAsDouble && context.root().numericDetection()) {
                return createDynamicField(
                    context,
                    name,
                    DynamicTemplate.XContentFieldType.DOUBLE,
                    () -> strategy.newDynamicDoubleField(context, name)
                );
            } else if (parseableAsLong == false && parseableAsDouble == false && context.root().dateDetection()) {
                // We refuse to match pure numbers, which are too likely to be
                // false positives with date formats that include eg.
                // `epoch_millis` or `YYYY`
                for (DateFormatter dateTimeFormatter : context.root().dynamicDateTimeFormatters()) {
                    try {
                        dateTimeFormatter.parseMillis(text);
                    } catch (DateTimeException | ElasticsearchParseException | IllegalArgumentException e) {
                        // failure to parse this, continue
                        continue;
                    }
                    return createDynamicDateField(
                        context,
                        name,
                        dateTimeFormatter,
                        () -> strategy.newDynamicDateField(context, name, dateTimeFormatter)
                    );
                }
                return createDynamicField(
                    context,
                    name,
                    DynamicTemplate.XContentFieldType.STRING,
                    () -> strategy.newDynamicStringField(context, name)
                );
            } else {
                return createDynamicField(
                    context,
                    name,
                    DynamicTemplate.XContentFieldType.STRING,
                    () -> strategy.newDynamicStringField(context, name)
                );
            }
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            XContentParser.NumberType numberType = context.parser().numberType();
            if (numberType == XContentParser.NumberType.INT
                || numberType == XContentParser.NumberType.LONG
                || numberType == XContentParser.NumberType.BIG_INTEGER) {
                return createDynamicField(
                    context,
                    name,
                    DynamicTemplate.XContentFieldType.LONG,
                    () -> strategy.newDynamicLongField(context, name)
                );
            } else if (numberType == XContentParser.NumberType.FLOAT
                || numberType == XContentParser.NumberType.DOUBLE
                || numberType == XContentParser.NumberType.BIG_DECIMAL) {
                    return createDynamicField(
                        context,
                        name,
                        DynamicTemplate.XContentFieldType.DOUBLE,
                        () -> strategy.newDynamicDoubleField(context, name)
                    );
                } else {
                    throw new IllegalStateException("Unable to parse number of type [" + numberType + "]");
                }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            return createDynamicField(
                context,
                name,
                DynamicTemplate.XContentFieldType.BOOLEAN,
                () -> strategy.newDynamicBooleanField(context, name)
            );
        } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
            // runtime binary fields are not supported, hence binary objects always get created as concrete fields
            return createDynamicField(
                context,
                name,
                DynamicTemplate.XContentFieldType.BINARY,
                () -> CONCRETE.newDynamicBinaryField(context, name)
            );
        } else {
            return createDynamicStringFieldFromTemplate(context, name);
        }
    }

    /**
     * Returns a dynamically created object mapper, eventually based on a matching dynamic template.
     */
    static Mapper createDynamicObjectMapper(DocumentParserContext context, String name) {
        Mapper mapper = createObjectMapperFromTemplate(context, name);
        return mapper != null
            ? mapper
            : new ObjectMapper.Builder(name, context.parent().subobjects).enabled(ObjectMapper.Defaults.ENABLED)
                .build(context.createDynamicMapperBuilderContext());
    }

    /**
     * Returns a dynamically created object mapper, based exclusively on a matching dynamic template, null otherwise.
     */
    static Mapper createObjectMapperFromTemplate(DocumentParserContext context, String name) {
        Mapper.Builder templateBuilder = findTemplateBuilderForObject(context, name);
        return templateBuilder == null ? null : templateBuilder.build(context.createDynamicMapperBuilderContext());
    }

    /**
     * Creates a dynamic string field based on a matching dynamic template.
     * No field is created in case there is no matching dynamic template.
     */
    static boolean createDynamicStringFieldFromTemplate(DocumentParserContext context, String name) throws IOException {
        return createDynamicField(context, name, DynamicTemplate.XContentFieldType.STRING, () -> false);
    }

    private static boolean createDynamicDateField(
        DocumentParserContext context,
        String name,
        DateFormatter dateFormatter,
        CheckedSupplier<Boolean, IOException> createDynamicField
    ) throws IOException {
        return createDynamicField(context, name, DynamicTemplate.XContentFieldType.DATE, dateFormatter, createDynamicField);
    }

    private static boolean createDynamicField(
        DocumentParserContext context,
        String name,
        DynamicTemplate.XContentFieldType matchType,
        CheckedSupplier<Boolean, IOException> dynamicFieldStrategy
    ) throws IOException {
        assert matchType != DynamicTemplate.XContentFieldType.DATE;
        return createDynamicField(context, name, matchType, null, dynamicFieldStrategy);
    }

    private static boolean createDynamicField(
        DocumentParserContext context,
        String name,
        DynamicTemplate.XContentFieldType matchType,
        DateFormatter dateFormatter,
        CheckedSupplier<Boolean, IOException> dynamicFieldStrategy
    ) throws IOException {
        if (applyMatchingTemplate(context, name, matchType, dateFormatter)) {
            context.markFieldAsAppliedFromTemplate(name);
            return true;
        } else {
            return dynamicFieldStrategy.get();
        }
    }

    /**
     * Find and apply a matching dynamic template. Returns {@code true} if a template could be found, {@code false} otherwise.
     * @param context        the parse context for this document
     * @param name           the current field name
     * @param matchType      the type of the field in the json document or null if unknown
     * @param dateFormatter  a date formatter to use if the type is a date, null if not a date or is using the default format
     * @return true if a template was found and applied, false otherwise
     */
    private static boolean applyMatchingTemplate(
        DocumentParserContext context,
        String name,
        DynamicTemplate.XContentFieldType matchType,
        DateFormatter dateFormatter
    ) throws IOException {
        DynamicTemplate dynamicTemplate = context.findDynamicTemplate(name, matchType);
        if (dynamicTemplate == null) {
            return false;
        }
        String dynamicType = dynamicTemplate.isRuntimeMapping() ? matchType.defaultRuntimeMappingType() : matchType.defaultMappingType();

        String mappingType = dynamicTemplate.mappingType(dynamicType);
        Map<String, Object> mapping = dynamicTemplate.mappingForName(name, dynamicType);
        if (dynamicTemplate.isRuntimeMapping()) {
            MappingParserContext parserContext = context.dynamicTemplateParserContext(dateFormatter);
            RuntimeField.Parser parser = parserContext.runtimeFieldParser(mappingType);
            String fullName = context.path().pathAsText(name);
            if (parser == null) {
                throw new MapperParsingException("failed to find type parsed [" + mappingType + "] for [" + fullName + "]");
            }
            RuntimeField.Builder builder = parser.parse(fullName, mapping, parserContext);
            Runtime.createDynamicField(builder.createRuntimeField(parserContext), context);
        } else {
            Mapper.Builder builder = parseDynamicTemplateMapping(name, mappingType, mapping, dateFormatter, context);
            CONCRETE.createDynamicField(builder, context);
        }
        return true;
    }

    private static Mapper.Builder findTemplateBuilderForObject(DocumentParserContext context, String name) {
        DynamicTemplate.XContentFieldType matchType = DynamicTemplate.XContentFieldType.OBJECT;
        DynamicTemplate dynamicTemplate = context.findDynamicTemplate(name, matchType);
        if (dynamicTemplate == null) {
            return null;
        }
        String dynamicType = matchType.defaultMappingType();
        String mappingType = dynamicTemplate.mappingType(dynamicType);
        Map<String, Object> mapping = dynamicTemplate.mappingForName(name, dynamicType);
        return parseDynamicTemplateMapping(name, mappingType, mapping, null, context);
    }

    private static Mapper.Builder parseDynamicTemplateMapping(
        String name,
        String mappingType,
        Map<String, Object> mapping,
        DateFormatter dateFormatter,
        DocumentParserContext context
    ) {
        MappingParserContext parserContext = context.dynamicTemplateParserContext(dateFormatter);
        Mapper.TypeParser typeParser = parserContext.typeParser(mappingType);
        if (typeParser == null) {
            throw new MapperParsingException("failed to find type parsed [" + mappingType + "] for [" + name + "]");
        }
        return typeParser.parse(name, mapping, parserContext);
    }

    /**
     * Defines how leaf fields of type string, long, double, boolean and date are dynamically mapped
     */
    private interface Strategy {
        boolean newDynamicStringField(DocumentParserContext context, String name) throws IOException;

        boolean newDynamicLongField(DocumentParserContext context, String name) throws IOException;

        boolean newDynamicDoubleField(DocumentParserContext context, String name) throws IOException;

        boolean newDynamicBooleanField(DocumentParserContext context, String name) throws IOException;

        boolean newDynamicDateField(DocumentParserContext context, String name, DateFormatter dateFormatter) throws IOException;
    }

    /**
     * Dynamically creates concrete fields, as part of the properties section.
     * Use for leaf fields, when their parent object is mapped as dynamic:true
     * @see Dynamic
     */
    private static final class Concrete implements Strategy {
        private final CheckedBiConsumer<DocumentParserContext, Mapper, IOException> parseField;

        Concrete(CheckedBiConsumer<DocumentParserContext, Mapper, IOException> parseField) {
            this.parseField = parseField;
        }

        boolean createDynamicField(Mapper.Builder builder, DocumentParserContext context, MapperBuilderContext mapperBuilderContext)
            throws IOException {
            Mapper mapper = builder.build(mapperBuilderContext);
            if (context.addDynamicMapper(mapper)) {
                parseField.accept(context, mapper);
                return true;
            } else {
                return false;
            }
        }

        boolean createDynamicField(Mapper.Builder builder, DocumentParserContext context) throws IOException {
            return createDynamicField(builder, context, context.createDynamicMapperBuilderContext());
        }

        @Override
        public boolean newDynamicStringField(DocumentParserContext context, String name) throws IOException {
            MapperBuilderContext mapperBuilderContext = context.createDynamicMapperBuilderContext();
            if (mapperBuilderContext.parentObjectContainsDimensions()) {
                return createDynamicField(
                    new KeywordFieldMapper.Builder(name, context.indexSettings().getIndexVersionCreated()),
                    context,
                    mapperBuilderContext
                );
            } else {
                return createDynamicField(
                    new TextFieldMapper.Builder(
                        name,
                        context.indexAnalyzers(),
                        context.indexSettings().getMode().isSyntheticSourceEnabled()
                    ).addMultiField(
                        new KeywordFieldMapper.Builder("keyword", context.indexSettings().getIndexVersionCreated()).ignoreAbove(256)
                    ),
                    context
                );
            }
        }

        @Override
        public boolean newDynamicLongField(DocumentParserContext context, String name) throws IOException {
            return createDynamicField(
                new NumberFieldMapper.Builder(
                    name,
                    NumberFieldMapper.NumberType.LONG,
                    ScriptCompiler.NONE,
                    context.indexSettings().getSettings(),
                    context.indexSettings().getIndexVersionCreated(),
                    context.indexSettings().getMode()
                ),
                context
            );
        }

        @Override
        public boolean newDynamicDoubleField(DocumentParserContext context, String name) throws IOException {
            // no templates are defined, we use float by default instead of double
            // since this is much more space-efficient and should be enough most of
            // the time
            return createDynamicField(
                new NumberFieldMapper.Builder(
                    name,
                    NumberFieldMapper.NumberType.FLOAT,
                    ScriptCompiler.NONE,
                    context.indexSettings().getSettings(),
                    context.indexSettings().getIndexVersionCreated(),
                    context.indexSettings().getMode()
                ),
                context
            );
        }

        @Override
        public boolean newDynamicBooleanField(DocumentParserContext context, String name) throws IOException {
            Settings settings = context.indexSettings().getSettings();
            boolean ignoreMalformed = FieldMapper.IGNORE_MALFORMED_SETTING.get(settings);
            return createDynamicField(
                new BooleanFieldMapper.Builder(
                    name,
                    ScriptCompiler.NONE,
                    ignoreMalformed,
                    context.indexSettings().getIndexVersionCreated()
                ),
                context
            );
        }

        @Override
        public boolean newDynamicDateField(DocumentParserContext context, String name, DateFormatter dateTimeFormatter) throws IOException {
            Settings settings = context.indexSettings().getSettings();
            boolean ignoreMalformed = FieldMapper.IGNORE_MALFORMED_SETTING.get(settings);
            return createDynamicField(
                new DateFieldMapper.Builder(
                    name,
                    DateFieldMapper.Resolution.MILLISECONDS,
                    dateTimeFormatter,
                    ScriptCompiler.NONE,
                    ignoreMalformed,
                    context.indexSettings().getIndexVersionCreated()
                ),
                context
            );
        }

        boolean newDynamicBinaryField(DocumentParserContext context, String name) throws IOException {
            return createDynamicField(
                new BinaryFieldMapper.Builder(name, context.indexSettings().getMode().isSyntheticSourceEnabled()),
                context
            );
        }
    }

    /**
     * Dynamically creates runtime fields, in the runtime section.
     * Used for sub-fields of objects that are mapped as dynamic:runtime.
     * @see Dynamic
     */
    private static final class Runtime implements Strategy {
        static boolean createDynamicField(RuntimeField runtimeField, DocumentParserContext context) {
            return context.addDynamicRuntimeField(runtimeField);
        }

        @Override
        public boolean newDynamicStringField(DocumentParserContext context, String name) {
            String fullName = context.path().pathAsText(name);
            return createDynamicField(KeywordScriptFieldType.sourceOnly(fullName), context);
        }

        @Override
        public boolean newDynamicLongField(DocumentParserContext context, String name) {
            String fullName = context.path().pathAsText(name);
            return createDynamicField(LongScriptFieldType.sourceOnly(fullName), context);
        }

        @Override
        public boolean newDynamicDoubleField(DocumentParserContext context, String name) {
            String fullName = context.path().pathAsText(name);
            return createDynamicField(DoubleScriptFieldType.sourceOnly(fullName), context);
        }

        @Override
        public boolean newDynamicBooleanField(DocumentParserContext context, String name) {
            String fullName = context.path().pathAsText(name);
            return createDynamicField(BooleanScriptFieldType.sourceOnly(fullName), context);
        }

        @Override
        public boolean newDynamicDateField(DocumentParserContext context, String name, DateFormatter dateFormatter) {
            String fullName = context.path().pathAsText(name);
            MappingParserContext parserContext = context.dynamicTemplateParserContext(dateFormatter);

            return createDynamicField(
                DateScriptFieldType.sourceOnly(fullName, dateFormatter, parserContext.indexVersionCreated()),
                context
            );
        }
    }
}
