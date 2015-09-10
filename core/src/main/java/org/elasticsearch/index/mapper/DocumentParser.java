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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.core.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper.StringFieldType;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.object.ArrayValueMapperParser;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A parser for documents, given mappings from a DocumentMapper */
class DocumentParser implements Closeable {

    private CloseableThreadLocal<ParseContext.InternalParseContext> cache = new CloseableThreadLocal<ParseContext.InternalParseContext>() {
        @Override
        protected ParseContext.InternalParseContext initialValue() {
            return new ParseContext.InternalParseContext(indexSettings, docMapperParser, docMapper, new ContentPath(0));
        }
    };

    private final Settings indexSettings;
    private final DocumentMapperParser docMapperParser;
    private final DocumentMapper docMapper;
    private final ReleasableLock parseLock;

    public DocumentParser(Settings indexSettings, DocumentMapperParser docMapperParser, DocumentMapper docMapper, ReleasableLock parseLock) {
        this.indexSettings = indexSettings;
        this.docMapperParser = docMapperParser;
        this.docMapper = docMapper;
        this.parseLock = parseLock;
    }

    public ParsedDocument parseDocument(SourceToParse source) throws MapperParsingException {
        try (ReleasableLock lock = parseLock.acquire()){
            return innerParseDocument(source);
        }
    }

    private ParsedDocument innerParseDocument(SourceToParse source) throws MapperParsingException {
        ParseContext.InternalParseContext context = cache.get();

        final Mapping mapping = docMapper.mapping();
        if (source.type() != null && !source.type().equals(docMapper.type())) {
            throw new MapperParsingException("Type mismatch, provide type [" + source.type() + "] but mapper is of type [" + docMapper.type() + "]");
        }
        source.type(docMapper.type());

        XContentParser parser = source.parser();
        try {
            if (parser == null) {
                parser = XContentHelper.createParser(source.source());
            }
            if (mapping.sourceTransforms.length > 0) {
                parser = transform(mapping, parser);
            }
            context.reset(parser, new ParseContext.Document(), source);

            // will result in START_OBJECT
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new MapperParsingException("Malformed content, must start with an object");
            }

            boolean emptyDoc = false;
            if (mapping.root.isEnabled()) {
                token = parser.nextToken();
                if (token == XContentParser.Token.END_OBJECT) {
                    // empty doc, we can handle it...
                    emptyDoc = true;
                } else if (token != XContentParser.Token.FIELD_NAME) {
                    throw new MapperParsingException("Malformed content, after first object, either the type field or the actual properties should exist");
                }
            }

            for (MetadataFieldMapper metadataMapper : mapping.metadataMappers) {
                metadataMapper.preParse(context);
            }

            if (mapping.root.isEnabled() == false) {
                // entire type is disabled
                parser.skipChildren();
            } else if (emptyDoc == false) {
                Mapper update = parseObject(context, mapping.root);
                if (update != null) {
                    context.addDynamicMappingsUpdate(update);
                }
            }

            for (MetadataFieldMapper metadataMapper : mapping.metadataMappers) {
                metadataMapper.postParse(context);
            }

            // try to parse the next token, this should be null if the object is ended properly
            // but will throw a JSON exception if the extra tokens is not valid JSON (this will be handled by the catch)
            if (Version.indexCreated(indexSettings).onOrAfter(Version.V_2_0_0_beta1)
                && source.parser() == null && parser != null) {
                // only check for end of tokens if we created the parser here
                token = parser.nextToken();
                if (token != null) {
                    throw new IllegalArgumentException("Malformed content, found extra data after parsing: " + token);
                }
            }

        } catch (Throwable e) {
            // if its already a mapper parsing exception, no need to wrap it...
            if (e instanceof MapperParsingException) {
                throw (MapperParsingException) e;
            }

            // Throw a more meaningful message if the document is empty.
            if (source.source() != null && source.source().length() == 0) {
                throw new MapperParsingException("failed to parse, document is empty");
            }

            throw new MapperParsingException("failed to parse", e);
        } finally {
            // only close the parser when its not provided externally
            if (source.parser() == null && parser != null) {
                parser.close();
            }
        }
        // reverse the order of docs for nested docs support, parent should be last
        if (context.docs().size() > 1) {
            Collections.reverse(context.docs());
        }
        // apply doc boost
        if (context.docBoost() != 1.0f) {
            Set<String> encounteredFields = new HashSet<>();
            for (ParseContext.Document doc : context.docs()) {
                encounteredFields.clear();
                for (IndexableField field : doc) {
                    if (field.fieldType().indexOptions() != IndexOptions.NONE && !field.fieldType().omitNorms()) {
                        if (!encounteredFields.contains(field.name())) {
                            ((Field) field).setBoost(context.docBoost() * field.boost());
                            encounteredFields.add(field.name());
                        }
                    }
                }
            }
        }

        Mapper rootDynamicUpdate = context.dynamicMappingsUpdate();
        Mapping update = null;
        if (rootDynamicUpdate != null) {
            update = mapping.mappingUpdate(rootDynamicUpdate);
        }

        ParsedDocument doc = new ParsedDocument(context.uid(), context.version(), context.id(), context.type(), source.routing(), source.timestamp(), source.ttl(), context.docs(),
            context.source(), update).parent(source.parent());
        // reset the context to free up memory
        context.reset(null, null, null);
        return doc;
    }

    static ObjectMapper parseObject(ParseContext context, ObjectMapper mapper) throws IOException {
        if (mapper.isEnabled() == false) {
            context.parser().skipChildren();
            return null;
        }
        XContentParser parser = context.parser();

        String currentFieldName = parser.currentName();
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            // the object is null ("obj1" : null), simply bail
            return null;
        }

        if (token.isValue()) {
            throw new MapperParsingException("object mapping for [" + mapper.name() + "] tried to parse field [" + currentFieldName + "] as object, but found a concrete value");
        }

        ObjectMapper.Nested nested = mapper.nested();
        if (nested.isNested()) {
            context = context.createNestedContext(mapper.fullPath());
            ParseContext.Document nestedDoc = context.doc();
            ParseContext.Document parentDoc = nestedDoc.getParent();
            // pre add the uid field if possible (id was already provided)
            IndexableField uidField = parentDoc.getField(UidFieldMapper.NAME);
            if (uidField != null) {
                // we don't need to add it as a full uid field in nested docs, since we don't need versioning
                // we also rely on this for UidField#loadVersion

                // this is a deeply nested field
                nestedDoc.add(new Field(UidFieldMapper.NAME, uidField.stringValue(), UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
            }
            // the type of the nested doc starts with __, so we can identify that its a nested one in filters
            // note, we don't prefix it with the type of the doc since it allows us to execute a nested query
            // across types (for example, with similar nested objects)
            nestedDoc.add(new Field(TypeFieldMapper.NAME, mapper.nestedTypePathAsString(), TypeFieldMapper.Defaults.FIELD_TYPE));
        }

        ContentPath.Type origPathType = context.path().pathType();
        context.path().pathType(mapper.pathType());

        // if we are at the end of the previous object, advance
        if (token == XContentParser.Token.END_OBJECT) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            // if we are just starting an OBJECT, advance, this is the object we are parsing, we need the name first
            token = parser.nextToken();
        }

        ObjectMapper update = null;
        while (token != XContentParser.Token.END_OBJECT) {
            ObjectMapper newUpdate = null;
            if (token == XContentParser.Token.START_OBJECT) {
                newUpdate = parseObject(context, mapper, currentFieldName);
            } else if (token == XContentParser.Token.START_ARRAY) {
                newUpdate = parseArray(context, mapper, currentFieldName);
            } else if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NULL) {
                parseNullValue(context, mapper, currentFieldName);
            } else if (token == null) {
                throw new MapperParsingException("object mapping for [" + mapper.name() + "] tried to parse field [" + currentFieldName + "] as object, but got EOF, has a concrete value been provided to it?");
            } else if (token.isValue()) {
                newUpdate = parseValue(context, mapper, currentFieldName, token);
            }
            token = parser.nextToken();
            if (newUpdate != null) {
                if (update == null) {
                    update = newUpdate;
                } else {
                    MapperUtils.merge(update, newUpdate);
                }
            }
        }
        // restore the enable path flag
        context.path().pathType(origPathType);
        if (nested.isNested()) {
            ParseContext.Document nestedDoc = context.doc();
            ParseContext.Document parentDoc = nestedDoc.getParent();
            if (nested.isIncludeInParent()) {
                for (IndexableField field : nestedDoc.getFields()) {
                    if (field.name().equals(UidFieldMapper.NAME) || field.name().equals(TypeFieldMapper.NAME)) {
                        continue;
                    } else {
                        parentDoc.add(field);
                    }
                }
            }
            if (nested.isIncludeInRoot()) {
                ParseContext.Document rootDoc = context.rootDoc();
                // don't add it twice, if its included in parent, and we are handling the master doc...
                if (!nested.isIncludeInParent() || parentDoc != rootDoc) {
                    for (IndexableField field : nestedDoc.getFields()) {
                        if (field.name().equals(UidFieldMapper.NAME) || field.name().equals(TypeFieldMapper.NAME)) {
                            continue;
                        } else {
                            rootDoc.add(field);
                        }
                    }
                }
            }
        }
        return update;
    }

    private static Mapper parseObjectOrField(ParseContext context, Mapper mapper) throws IOException {
        if (mapper instanceof ObjectMapper) {
            return parseObject(context, (ObjectMapper) mapper);
        } else {
            FieldMapper fieldMapper = (FieldMapper)mapper;
            Mapper update = fieldMapper.parse(context);
            if (fieldMapper.copyTo() != null) {
                parseCopyFields(context, fieldMapper, fieldMapper.copyTo().copyToFields());
            }
            return update;
        }
    }

    private static ObjectMapper parseObject(final ParseContext context, ObjectMapper mapper, String currentFieldName) throws IOException {
        if (currentFieldName == null) {
            throw new MapperParsingException("object mapping [" + mapper.name() + "] trying to serialize an object with no field associated with it, current value [" + context.parser().textOrNull() + "]");
        }
        context.path().add(currentFieldName);

        ObjectMapper update = null;
        Mapper objectMapper = mapper.getMapper(currentFieldName);
        if (objectMapper != null) {
            final Mapper subUpdate = parseObjectOrField(context, objectMapper);
            if (subUpdate != null) {
                // propagate mapping update
                update = mapper.mappingUpdate(subUpdate);
            }
        } else {
            ObjectMapper.Dynamic dynamic = mapper.dynamic();
            if (dynamic == null) {
                dynamic = dynamicOrDefault(context.root().dynamic());
            }
            if (dynamic == ObjectMapper.Dynamic.STRICT) {
                throw new StrictDynamicMappingException(mapper.fullPath(), currentFieldName);
            } else if (dynamic == ObjectMapper.Dynamic.TRUE) {
                // remove the current field name from path, since template search and the object builder add it as well...
                context.path().remove();
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "object");
                if (builder == null) {
                    builder = MapperBuilders.object(currentFieldName).enabled(true).pathType(mapper.pathType());
                    // if this is a non root object, then explicitly set the dynamic behavior if set
                    if (!(mapper instanceof RootObjectMapper) && mapper.dynamic() != ObjectMapper.Defaults.DYNAMIC) {
                        ((ObjectMapper.Builder) builder).dynamic(mapper.dynamic());
                    }
                }
                Mapper.BuilderContext builderContext = new Mapper.BuilderContext(context.indexSettings(), context.path());
                objectMapper = builder.build(builderContext);
                context.path().add(currentFieldName);
                update = mapper.mappingUpdate(parseAndMergeUpdate(objectMapper, context));
            } else {
                // not dynamic, read everything up to end object
                context.parser().skipChildren();
            }
        }

        context.path().remove();
        return update;
    }

    private static ObjectMapper parseArray(ParseContext context, ObjectMapper parentMapper, String lastFieldName) throws IOException {
        String arrayFieldName = lastFieldName;
        Mapper mapper = parentMapper.getMapper(lastFieldName);
        if (mapper != null) {
            // There is a concrete mapper for this field already. Need to check if the mapper
            // expects an array, if so we pass the context straight to the mapper and if not
            // we serialize the array components
            if (mapper instanceof ArrayValueMapperParser) {
                final Mapper subUpdate = parseObjectOrField(context, mapper);
                if (subUpdate != null) {
                    // propagate the mapping update
                    return parentMapper.mappingUpdate(subUpdate);
                } else {
                    return null;
                }
            } else {
                return parseNonDynamicArray(context, parentMapper, lastFieldName, arrayFieldName);
            }
        } else {

            ObjectMapper.Dynamic dynamic = parentMapper.dynamic();
            if (dynamic == null) {
                dynamic = dynamicOrDefault(context.root().dynamic());
            }
            if (dynamic == ObjectMapper.Dynamic.STRICT) {
                throw new StrictDynamicMappingException(parentMapper.fullPath(), arrayFieldName);
            } else if (dynamic == ObjectMapper.Dynamic.TRUE) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, arrayFieldName, "object");
                if (builder == null) {
                    return parseNonDynamicArray(context, parentMapper, lastFieldName, arrayFieldName);
                }
                Mapper.BuilderContext builderContext = new Mapper.BuilderContext(context.indexSettings(), context.path());
                mapper = builder.build(builderContext);
                if (mapper != null && mapper instanceof ArrayValueMapperParser) {
                    context.path().add(arrayFieldName);
                    mapper = parseAndMergeUpdate(mapper, context);
                    return parentMapper.mappingUpdate(mapper);
                } else {
                    return parseNonDynamicArray(context, parentMapper, lastFieldName, arrayFieldName);
                }
            } else {
                return parseNonDynamicArray(context, parentMapper, lastFieldName, arrayFieldName);
            }
        }
    }

    private static ObjectMapper parseNonDynamicArray(ParseContext context, ObjectMapper mapper, String lastFieldName, String arrayFieldName) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.START_OBJECT) {
                return parseObject(context, mapper, lastFieldName);
            } else if (token == XContentParser.Token.START_ARRAY) {
                return parseArray(context, mapper, lastFieldName);
            } else if (token == XContentParser.Token.FIELD_NAME) {
                lastFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NULL) {
                parseNullValue(context, mapper, lastFieldName);
            } else if (token == null) {
                throw new MapperParsingException("object mapping for [" + mapper.name() + "] with array for [" + arrayFieldName + "] tried to parse as array, but got EOF, is there a mismatch in types for the same field?");
            } else {
                return parseValue(context, mapper, lastFieldName, token);
            }
        }
        return null;
    }

    private static ObjectMapper parseValue(final ParseContext context, ObjectMapper parentMapper, String currentFieldName, XContentParser.Token token) throws IOException {
        if (currentFieldName == null) {
            throw new MapperParsingException("object mapping [" + parentMapper.name() + "] trying to serialize a value with no field associated with it, current value [" + context.parser().textOrNull() + "]");
        }
        Mapper mapper = parentMapper.getMapper(currentFieldName);
        if (mapper != null) {
            Mapper subUpdate = parseObjectOrField(context, mapper);
            if (subUpdate == null) {
                return null;
            }
            return parentMapper.mappingUpdate(subUpdate);
        } else {
            return parseDynamicValue(context, parentMapper, currentFieldName, token);
        }
    }

    private static void parseNullValue(ParseContext context, ObjectMapper parentMapper, String lastFieldName) throws IOException {
        // we can only handle null values if we have mappings for them
        Mapper mapper = parentMapper.getMapper(lastFieldName);
        if (mapper != null) {
            // TODO: passing null to an object seems bogus?
            parseObjectOrField(context, mapper);
        } else if (parentMapper.dynamic() == ObjectMapper.Dynamic.STRICT) {
            throw new StrictDynamicMappingException(parentMapper.fullPath(), lastFieldName);
        }
    }

    private static Mapper.Builder<?,?> createBuilderFromFieldType(final ParseContext context, MappedFieldType fieldType, String currentFieldName) {
        Mapper.Builder builder = null;
        if (fieldType instanceof StringFieldType) {
            builder = context.root().findTemplateBuilder(context, currentFieldName, "string");
            if (builder == null) {
                builder = MapperBuilders.stringField(currentFieldName);
            }
        } else if (fieldType instanceof DateFieldType) {
            builder = context.root().findTemplateBuilder(context, currentFieldName, "date");
            if (builder == null) {
                builder = MapperBuilders.dateField(currentFieldName);
            }
        } else if (fieldType.numericType() != null) {
            switch (fieldType.numericType()) {
            case LONG:
                builder = context.root().findTemplateBuilder(context, currentFieldName, "long");
                if (builder == null) {
                    builder = MapperBuilders.longField(currentFieldName);
                }
                break;
            case DOUBLE:
                builder = context.root().findTemplateBuilder(context, currentFieldName, "double");
                if (builder == null) {
                    builder = MapperBuilders.doubleField(currentFieldName);
                }
                break;
            case INT:
                builder = context.root().findTemplateBuilder(context, currentFieldName, "integer");
                if (builder == null) {
                    builder = MapperBuilders.integerField(currentFieldName);
                }
                break;
            case FLOAT:
                builder = context.root().findTemplateBuilder(context, currentFieldName, "float");
                if (builder == null) {
                    builder = MapperBuilders.floatField(currentFieldName);
                }
                break;
            default:
                throw new AssertionError("Unexpected numeric type " + fieldType.numericType());
            }
        }
        return builder;
    }

    private static Mapper.Builder<?,?> createBuilderFromDynamicValue(final ParseContext context, XContentParser.Token token, String currentFieldName) throws IOException {
        if (token == XContentParser.Token.VALUE_STRING) {
            // do a quick test to see if its fits a dynamic template, if so, use it.
            // we need to do it here so we can handle things like attachment templates, where calling
            // text (to see if its a date) causes the binary value to be cleared
            {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "string", null);
                if (builder != null) {
                    return builder;
                }
            }

            if (context.root().dateDetection()) {
                String text = context.parser().text();
                // a safe check since "1" gets parsed as well
                if (Strings.countOccurrencesOf(text, ":") > 1 || Strings.countOccurrencesOf(text, "-") > 1 || Strings.countOccurrencesOf(text, "/") > 1) {
                    for (FormatDateTimeFormatter dateTimeFormatter : context.root().dynamicDateTimeFormatters()) {
                        try {
                            dateTimeFormatter.parser().parseMillis(text);
                            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "date");
                            if (builder == null) {
                                builder = MapperBuilders.dateField(currentFieldName).dateTimeFormatter(dateTimeFormatter);
                            }
                            return builder;
                        } catch (Exception e) {
                            // failure to parse this, continue
                        }
                    }
                }
            }
            if (context.root().numericDetection()) {
                String text = context.parser().text();
                try {
                    Long.parseLong(text);
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "long");
                    if (builder == null) {
                        builder = MapperBuilders.longField(currentFieldName);
                    }
                    return builder;
                } catch (NumberFormatException e) {
                    // not a long number
                }
                try {
                    Double.parseDouble(text);
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "double");
                    if (builder == null) {
                        builder = MapperBuilders.doubleField(currentFieldName);
                    }
                    return builder;
                } catch (NumberFormatException e) {
                    // not a long number
                }
            }
            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "string");
            if (builder == null) {
                builder = MapperBuilders.stringField(currentFieldName);
            }
            return builder;
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            XContentParser.NumberType numberType = context.parser().numberType();
            if (numberType == XContentParser.NumberType.INT) {
                if (context.parser().estimatedNumberType()) {
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "long");
                    if (builder == null) {
                        builder = MapperBuilders.longField(currentFieldName);
                    }
                    return builder;
                } else {
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "integer");
                    if (builder == null) {
                        builder = MapperBuilders.integerField(currentFieldName);
                    }
                    return builder;
                }
            } else if (numberType == XContentParser.NumberType.LONG) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "long");
                if (builder == null) {
                    builder = MapperBuilders.longField(currentFieldName);
                }
                return builder;
            } else if (numberType == XContentParser.NumberType.FLOAT) {
                if (context.parser().estimatedNumberType()) {
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "double");
                    if (builder == null) {
                        builder = MapperBuilders.doubleField(currentFieldName);
                    }
                    return builder;
                } else {
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "float");
                    if (builder == null) {
                        builder = MapperBuilders.floatField(currentFieldName);
                    }
                    return builder;
                }
            } else if (numberType == XContentParser.NumberType.DOUBLE) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "double");
                if (builder == null) {
                    builder = MapperBuilders.doubleField(currentFieldName);
                }
                return builder;
            }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "boolean");
            if (builder == null) {
                builder = MapperBuilders.booleanField(currentFieldName);
            }
            return builder;
        } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "binary");
            if (builder == null) {
                builder = MapperBuilders.binaryField(currentFieldName);
            }
            return builder;
        } else {
            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, null);
            if (builder != null) {
                return builder;
            }
        }
        // TODO how do we identify dynamically that its a binary value?
        throw new IllegalStateException("Can't handle serializing a dynamic type with content token [" + token + "] and field name [" + currentFieldName + "]");
    }

    private static ObjectMapper parseDynamicValue(final ParseContext context, ObjectMapper parentMapper, String currentFieldName, XContentParser.Token token) throws IOException {
        ObjectMapper.Dynamic dynamic = parentMapper.dynamic();
        if (dynamic == null) {
            dynamic = dynamicOrDefault(context.root().dynamic());
        }
        if (dynamic == ObjectMapper.Dynamic.STRICT) {
            throw new StrictDynamicMappingException(parentMapper.fullPath(), currentFieldName);
        }
        if (dynamic == ObjectMapper.Dynamic.FALSE) {
            return null;
        }
        final Mapper.BuilderContext builderContext = new Mapper.BuilderContext(context.indexSettings(), context.path());
        final MappedFieldType existingFieldType = context.mapperService().fullName(context.path().fullPathAsText(currentFieldName));
        Mapper.Builder builder = null;
        if (existingFieldType != null) {
            // create a builder of the same type
            builder = createBuilderFromFieldType(context, existingFieldType, currentFieldName);
            if (builder != null) {
                // best-effort to not introduce a conflict
                if (builder instanceof StringFieldMapper.Builder) {
                    StringFieldMapper.Builder stringBuilder = (StringFieldMapper.Builder) builder;
                    stringBuilder.fieldDataSettings(existingFieldType.fieldDataType().getSettings());
                    stringBuilder.store(existingFieldType.stored());
                    stringBuilder.indexOptions(existingFieldType.indexOptions());
                    stringBuilder.tokenized(existingFieldType.tokenized());
                    stringBuilder.omitNorms(existingFieldType.omitNorms());
                    stringBuilder.docValues(existingFieldType.hasDocValues());
                    stringBuilder.indexAnalyzer(existingFieldType.indexAnalyzer());
                    stringBuilder.searchAnalyzer(existingFieldType.searchAnalyzer());
                } else if (builder instanceof NumberFieldMapper.Builder) {
                    NumberFieldMapper.Builder<?,?> numberBuilder = (NumberFieldMapper.Builder<?, ?>) builder;
                    numberBuilder.fieldDataSettings(existingFieldType.fieldDataType().getSettings());
                    numberBuilder.store(existingFieldType.stored());
                    numberBuilder.indexOptions(existingFieldType.indexOptions());
                    numberBuilder.tokenized(existingFieldType.tokenized());
                    numberBuilder.omitNorms(existingFieldType.omitNorms());
                    numberBuilder.docValues(existingFieldType.hasDocValues());
                    numberBuilder.precisionStep(existingFieldType.numericPrecisionStep());
                }
            }
        }
        if (builder == null) {
            builder = createBuilderFromDynamicValue(context, token, currentFieldName);
        }
        Mapper mapper = builder.build(builderContext);

        mapper = parseAndMergeUpdate(mapper, context);

        ObjectMapper update = null;
        if (mapper != null) {
            update = parentMapper.mappingUpdate(mapper);
        }
        return update;
    }

    /** Creates instances of the fields that the current field should be copied to */
    private static void parseCopyFields(ParseContext context, FieldMapper fieldMapper, List<String> copyToFields) throws IOException {
        if (!context.isWithinCopyTo() && copyToFields.isEmpty() == false) {
            context = context.createCopyToContext();
            for (String field : copyToFields) {
                // In case of a hierarchy of nested documents, we need to figure out
                // which document the field should go to
                ParseContext.Document targetDoc = null;
                for (ParseContext.Document doc = context.doc(); doc != null; doc = doc.getParent()) {
                    if (field.startsWith(doc.getPrefix())) {
                        targetDoc = doc;
                        break;
                    }
                }
                assert targetDoc != null;
                final ParseContext copyToContext;
                if (targetDoc == context.doc()) {
                    copyToContext = context;
                } else {
                    copyToContext = context.switchDoc(targetDoc);
                }
                parseCopy(field, copyToContext);
            }
        }
    }

    /** Creates an copy of the current field with given field name and boost */
    private static void parseCopy(String field, ParseContext context) throws IOException {
        FieldMapper fieldMapper = context.docMapper().mappers().getMapper(field);
        if (fieldMapper != null) {
            fieldMapper.parse(context);
        } else {
            // The path of the dest field might be completely different from the current one so we need to reset it
            context = context.overridePath(new ContentPath(0));

            ObjectMapper mapper = context.root();
            String objectPath = "";
            String fieldPath = field;
            int posDot = field.lastIndexOf('.');
            if (posDot > 0) {
                objectPath = field.substring(0, posDot);
                context.path().add(objectPath);
                mapper = context.docMapper().objectMappers().get(objectPath);
                fieldPath = field.substring(posDot + 1);
            }
            if (mapper == null) {
                //TODO: Create an object dynamically?
                throw new MapperParsingException("attempt to copy value to non-existing object [" + field + "]");
            }
            ObjectMapper update = parseDynamicValue(context, mapper, fieldPath, context.parser().currentToken());
            assert update != null; // we are parsing a dynamic value so we necessarily created a new mapping

            // propagate the update to the root
            while (objectPath.length() > 0) {
                String parentPath = "";
                ObjectMapper parent = context.root();
                posDot = objectPath.lastIndexOf('.');
                if (posDot > 0) {
                    parentPath = objectPath.substring(0, posDot);
                    parent = context.docMapper().objectMappers().get(parentPath);
                }
                if (parent == null) {
                    throw new IllegalStateException("[" + objectPath + "] has no parent for path [" + parentPath + "]");
                }
                update = parent.mappingUpdate(update);
                objectPath = parentPath;
            }
            context.addDynamicMappingsUpdate(update);
        }
    }

    /**
     * Parse the given {@code context} with the given {@code mapper} and apply
     * the potential mapping update in-place. This method is useful when
     * composing mapping updates.
     */
    private static <M extends Mapper> M parseAndMergeUpdate(M mapper, ParseContext context) throws IOException {
        final Mapper update = parseObjectOrField(context, mapper);
        if (update != null) {
            MapperUtils.merge(mapper, update);
        }
        return mapper;
    }

    private static XContentParser transform(Mapping mapping, XContentParser parser) throws IOException {
        Map<String, Object> transformed;
        try (XContentParser autoCloses = parser) {
            transformed = transformSourceAsMap(mapping, parser.mapOrdered());
        }
        XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType()).value(transformed);
        return parser.contentType().xContent().createParser(builder.bytes());
    }

    private static ObjectMapper.Dynamic dynamicOrDefault(ObjectMapper.Dynamic dynamic) {
        return dynamic == null ? ObjectMapper.Dynamic.TRUE : dynamic;
    }

    static Map<String, Object> transformSourceAsMap(Mapping mapping, Map<String, Object> sourceAsMap) {
        if (mapping.sourceTransforms.length == 0) {
            return sourceAsMap;
        }
        for (Mapping.SourceTransform transform : mapping.sourceTransforms) {
            sourceAsMap = transform.transformSourceAsMap(sourceAsMap);
        }
        return sourceAsMap;
    }

    @Override
    public void close() {
        cache.close();
    }
}
