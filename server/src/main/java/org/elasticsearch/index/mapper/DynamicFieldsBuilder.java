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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.mapper.ObjectMapper.Dynamic;
import org.elasticsearch.script.ScriptCompiler;

import java.io.IOException;
import java.time.format.DateTimeParseException;
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
    void createDynamicFieldFromValue(final DocumentParserContext context,
                                           XContentParser.Token token,
                                           String name) throws IOException {
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
                createDynamicField(context, name, DynamicTemplate.XContentFieldType.LONG,
                    () -> strategy.newDynamicLongField(context, name));
            } else if (parseableAsDouble && context.root().numericDetection()) {
                createDynamicField(context, name, DynamicTemplate.XContentFieldType.DOUBLE,
                    () -> strategy.newDynamicDoubleField(context, name));
            } else if (parseableAsLong == false && parseableAsDouble == false && context.root().dateDetection()) {
                // We refuse to match pure numbers, which are too likely to be
                // false positives with date formats that include eg.
                // `epoch_millis` or `YYYY`
                for (DateFormatter dateTimeFormatter : context.root().dynamicDateTimeFormatters()) {
                    try {
                        dateTimeFormatter.parse(text);
                    } catch (ElasticsearchParseException | DateTimeParseException | IllegalArgumentException e) {
                        // failure to parse this, continue
                        continue;
                    }
                    createDynamicDateField(context, name, dateTimeFormatter,
                        () -> strategy.newDynamicDateField(context, name, dateTimeFormatter));
                    return;
                }
                createDynamicField(context, name, DynamicTemplate.XContentFieldType.STRING,
                    () -> strategy.newDynamicStringField(context, name));
            } else {
                createDynamicField(context, name, DynamicTemplate.XContentFieldType.STRING,
                    () -> strategy.newDynamicStringField(context, name));
            }
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            XContentParser.NumberType numberType = context.parser().numberType();
            if (numberType == XContentParser.NumberType.INT
                || numberType == XContentParser.NumberType.LONG
                || numberType == XContentParser.NumberType.BIG_INTEGER) {
                createDynamicField(context, name, DynamicTemplate.XContentFieldType.LONG,
                    () -> strategy.newDynamicLongField(context, name));
            } else if (numberType == XContentParser.NumberType.FLOAT
                || numberType == XContentParser.NumberType.DOUBLE
                || numberType == XContentParser.NumberType.BIG_DECIMAL) {
                createDynamicField(context, name, DynamicTemplate.XContentFieldType.DOUBLE,
                    () -> strategy.newDynamicDoubleField(context, name));
            } else {
                throw new IllegalStateException("Unable to parse number of type [" + numberType + "]");
            }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            createDynamicField(context, name, DynamicTemplate.XContentFieldType.BOOLEAN,
                () -> strategy.newDynamicBooleanField(context, name));
        } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
            //runtime binary fields are not supported, hence binary objects always get created as concrete fields
            createDynamicField(context, name, DynamicTemplate.XContentFieldType.BINARY,
                () -> CONCRETE.newDynamicBinaryField(context, name));
        } else {
            createDynamicStringFieldFromTemplate(context, name);
        }
    }

    /**
     * Returns a dynamically created object mapper, eventually based on a matching dynamic template.
     */
    Mapper createDynamicObjectMapper(DocumentParserContext context, String name) {
        Mapper mapper = createObjectMapperFromTemplate(context, name);
        return mapper != null ? mapper : new ObjectMapper.Builder(name).enabled(true).build(context.path());
    }

    /**
     * Returns a dynamically created object mapper, based exclusively on a matching dynamic template, null otherwise.
     */
    Mapper createObjectMapperFromTemplate(DocumentParserContext context, String name) {
        Mapper.Builder templateBuilder = findTemplateBuilderForObject(context, name);
        return templateBuilder == null ? null : templateBuilder.build(context.path());
    }

    /**
     * Creates a dynamic string field based on a matching dynamic template.
     * No field is created in case there is no matching dynamic template.
     */
    void createDynamicStringFieldFromTemplate(DocumentParserContext context, String name) throws IOException {
        createDynamicField(context, name, DynamicTemplate.XContentFieldType.STRING, () -> {});
    }

    private static void createDynamicDateField(DocumentParserContext context,
                                               String name,
                                               DateFormatter dateFormatter,
                                               CheckedRunnable<IOException> createDynamicField) throws IOException {
        createDynamicField(context, name, DynamicTemplate.XContentFieldType.DATE, dateFormatter, createDynamicField);
    }

    private static void createDynamicField(DocumentParserContext context,
                                           String name,
                                           DynamicTemplate.XContentFieldType matchType,
                                           CheckedRunnable<IOException> dynamicFieldStrategy) throws IOException {
        assert matchType != DynamicTemplate.XContentFieldType.DATE;
        createDynamicField(context, name, matchType, null, dynamicFieldStrategy);
    }

    private static void createDynamicField(DocumentParserContext context,
                                           String name,
                                           DynamicTemplate.XContentFieldType matchType,
                                           DateFormatter dateFormatter,
                                           CheckedRunnable<IOException> dynamicFieldStrategy) throws IOException {
        if (applyMatchingTemplate(context, name, matchType, dateFormatter) == false) {
            dynamicFieldStrategy.run();
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
    private static boolean applyMatchingTemplate(DocumentParserContext context,
                                                 String name,
                                                 DynamicTemplate.XContentFieldType matchType,
                                                 DateFormatter dateFormatter) throws IOException {
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
            RuntimeField runtimeField = parser.parse(fullName, mapping, parserContext);
            Runtime.createDynamicField(runtimeField, context);
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

    private static Mapper.Builder parseDynamicTemplateMapping(String name,
                                               String mappingType,
                                               Map<String, Object> mapping,
                                               DateFormatter dateFormatter,
                                               DocumentParserContext context) {
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
        void newDynamicStringField(DocumentParserContext context, String name) throws IOException;
        void newDynamicLongField(DocumentParserContext context, String name) throws IOException;
        void newDynamicDoubleField(DocumentParserContext context, String name) throws IOException;
        void newDynamicBooleanField(DocumentParserContext context, String name) throws IOException;
        void newDynamicDateField(DocumentParserContext context, String name, DateFormatter dateFormatter) throws IOException;
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

        void createDynamicField(Mapper.Builder builder, DocumentParserContext context) throws IOException {
            Mapper mapper = builder.build(context.path());
            context.addDynamicMapper(mapper);
            parseField.accept(context, mapper);
        }

        @Override
        public void newDynamicStringField(DocumentParserContext context, String name) throws IOException {
            createDynamicField(new TextFieldMapper.Builder(name, context.indexAnalyzers()).addMultiField(
                    new KeywordFieldMapper.Builder("keyword").ignoreAbove(256)), context);
        }

        @Override
        public void newDynamicLongField(DocumentParserContext context, String name) throws IOException {
            createDynamicField(
                new NumberFieldMapper.Builder(
                    name,
                    NumberFieldMapper.NumberType.LONG,
                    ScriptCompiler.NONE,
                    context.indexSettings().getSettings()
                ), context);
        }

        @Override
        public void newDynamicDoubleField(DocumentParserContext context, String name) throws IOException {
            // no templates are defined, we use float by default instead of double
            // since this is much more space-efficient and should be enough most of
            // the time
            createDynamicField(new NumberFieldMapper.Builder(
                name,
                NumberFieldMapper.NumberType.FLOAT,
                ScriptCompiler.NONE,
                context.indexSettings().getSettings()), context);
        }

        @Override
        public void newDynamicBooleanField(DocumentParserContext context, String name) throws IOException {
            createDynamicField(new BooleanFieldMapper.Builder(name, ScriptCompiler.NONE), context);
        }

        @Override
        public void newDynamicDateField(DocumentParserContext context, String name, DateFormatter dateTimeFormatter) throws IOException {
            Settings settings = context.indexSettings().getSettings();
            boolean ignoreMalformed = FieldMapper.IGNORE_MALFORMED_SETTING.get(settings);
            createDynamicField(new DateFieldMapper.Builder(name, DateFieldMapper.Resolution.MILLISECONDS,
                dateTimeFormatter, ScriptCompiler.NONE, ignoreMalformed, context.indexSettings().getIndexVersionCreated()), context);
        }

        void newDynamicBinaryField(DocumentParserContext context, String name) throws IOException {
            createDynamicField(new BinaryFieldMapper.Builder(name), context);
        }
    }

    /**
     * Dynamically creates runtime fields, in the runtime section.
     * Used for sub-fields of objects that are mapped as dynamic:runtime.
     * @see Dynamic
     */
    private static final class Runtime implements Strategy {
        static void createDynamicField(RuntimeField runtimeField, DocumentParserContext context) {
            context.addDynamicRuntimeField(runtimeField);
        }

        @Override
        public void newDynamicStringField(DocumentParserContext context, String name) {
            String fullName = context.path().pathAsText(name);
            createDynamicField(KeywordScriptFieldType.sourceOnly(fullName), context);
        }

        @Override
        public void newDynamicLongField(DocumentParserContext context, String name) {
            String fullName = context.path().pathAsText(name);
            createDynamicField(LongScriptFieldType.sourceOnly(fullName), context);
        }

        @Override
        public void newDynamicDoubleField(DocumentParserContext context, String name) {
            String fullName = context.path().pathAsText(name);
            createDynamicField(DoubleScriptFieldType.sourceOnly(fullName), context);
        }

        @Override
        public void newDynamicBooleanField(DocumentParserContext context, String name) {
            String fullName = context.path().pathAsText(name);
            createDynamicField(BooleanScriptFieldType.sourceOnly(fullName), context);
        }

        @Override
        public void newDynamicDateField(DocumentParserContext context, String name, DateFormatter dateFormatter) {
            String fullName = context.path().pathAsText(name);
            createDynamicField(DateScriptFieldType.sourceOnly(fullName, dateFormatter), context);
        }
    }
}
