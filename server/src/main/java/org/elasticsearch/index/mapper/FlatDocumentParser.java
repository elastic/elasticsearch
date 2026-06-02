/*
* Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
* or more contributor license agreements. Licensed under the "Elastic License
* 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
* Public License v 1"; you may not use this file except in compliance with, at
* your election, the "Elastic License 2.0", the "GNU Affero General Public
* License v3.0 only", or the "Server Side Public License, v 1".
*/

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MIN_DIMS_FOR_DYNAMIC_FLOAT_MAPPING;

/**
 * A specialized parser for documents indexed into a strict-columnar index (subobjects disabled, no nested fields,
 * dynamic mode RUNTIME not supported, and FALSE dropping data).
 *
 * It is a fork of {@link DocumentParser} with branches that can never apply to such a
 * mapping removed: nested-document handling, synthetic-source-keep / ignored-source capture, FALLBACK synthetic
 * source, dot-expansion, and ObjectMapper recursion from the root.
 * <p>
 * This parser is intentionally lossy under synthetic source: fields that cannot reconstruct their source from doc
 * values (FALLBACK fields such as geo_shape, completion, or fields with doc_values disabled) will silently lose
 * their contribution to {@code _source}. This trade-off is acceptable for strict columnar mode:
 * <ul>
 *     <li>Field can't be configured to disable doc values.</li>
 *     <li>Fields that don't have synthetic source support don't need to be supported in columnar mode, or ta least not initially</li>
 *     <li>Copy to has very little usage, but is planned to be supported</li>
 * </ul>
 * <p>
 */
final class FlatDocumentParser implements DocumentParser {

    static final IndexVersion DYNAMICALLY_MAP_DENSE_VECTORS_INDEX_VERSION = IndexVersions.FIRST_DETACHED_INDEX_VERSION;

    private final XContentParserConfiguration parserConfiguration;
    private final MappingParserContext mappingParserContext;

    FlatDocumentParser(XContentParserConfiguration parserConfiguration, MappingParserContext mappingParserContext) {
        this.mappingParserContext = mappingParserContext;
        this.parserConfiguration = parserConfiguration;
    }

    /**
     * Parse a document
     *
     * @param source        the document to parse
     * @param mappingLookup the mappings information needed to parse the document
     * @return the parsed document
     * @throws DocumentParsingException whenever there's a problem parsing the document
     */
    public ParsedDocument parseDocument(SourceToParse source, MappingLookup mappingLookup) throws DocumentParsingException {
        SourceToParse.Source sourceObject = source.source();
        if (sourceObject.isEmpty()) {
            throw new DocumentParsingException(new XContentLocation(0, 0), "failed to parse, document is empty");
        }
        final RootDocumentParserContext context;

        try (
            // the context needs to be closed after parsing to make sure the actual parser is properly closed;
            // closing won't impact other state of the context
            RootDocumentParserContext ctx = new RootDocumentParserContext(
                mappingLookup,
                mappingParserContext,
                source,
                sourceObject.parser(parserConfiguration)
            )
        ) {
            context = ctx;
            validateStart(context.parser());
            MetadataFieldMapper[] metadataFieldsMappers = mappingLookup.getMapping().getSortedMetadataMappers();
            internalParseDocument(metadataFieldsMappers, context);
            validateEnd(context.parser());
        } catch (XContentParseException e) {
            throw new DocumentParsingException(e.getLocation(), e.getMessage(), e);
        } catch (IOException e) {
            // IOException from jackson, we don't have any useful location information here
            throw new DocumentParsingException(XContentLocation.UNKNOWN, "Error parsing document", e);
        }
        assert context.path().pathAsText("").isEmpty() : "found leftover path elements: " + context.path().pathAsText("");

        CompressedXContent dynamicUpdate = DefaultDocumentParser.createDynamicUpdate(context);

        return new ParsedDocument(
            context.version(),
            context.seqID(),
            context.id(),
            context.routing(),
            context.reorderParentAndGetDocs(),
            context.sourceToParse().source(),
            dynamicUpdate,
            source.getMeteringParserDecorator().meteredDocumentSize()
        ) {
            @Override
            public String documentDescription() {
                IdFieldMapper idMapper = (IdFieldMapper) mappingLookup.getMapping().getMetadataMapperByName(IdFieldMapper.NAME);
                return idMapper.documentDescription(this);
            }
        };
    }

    private static void skipChildren(DocumentParserContext context) throws IOException {
        boolean withinLeafObject = context.path().isWithinLeafObject();
        // treat everything as leaf while skipping children, this allows parser decorators
        // to implement custom skip logic for children with DotExpandingXContentParser
        context.path().setWithinLeafObject(true);
        context.parser().skipChildren();
        context.path().setWithinLeafObject(withinLeafObject);
    }

    private void internalParseDocument(MetadataFieldMapper[] metadataFieldsMappers, DocumentParserContext context) {
        try {
            final boolean emptyDoc = isEmptyDoc(context.root(), context.parser());

            for (MetadataFieldMapper metadataMapper : metadataFieldsMappers) {
                metadataMapper.preParse(context);
            }

            if (context.root().isEnabled() == false) {
                // entire type is disabled — skip, no ignored-source capture in the flat parser
                skipChildren(context);
            } else if (emptyDoc == false) {
                parseObjectOrNested(context);
            }

            // Index-time scripts are not supported: mappings with script mappers are ineligible for this parser.
            assert context.mappingLookup().indexTimeScriptMappers().isEmpty();

            context.processArrayOffsets(context);
            for (MetadataFieldMapper metadataMapper : metadataFieldsMappers) {
                metadataMapper.postParse(context);
            }
        } catch (Exception e) {
            throw wrapInDocumentParsingException(context, e);
        }
    }

    private static void validateStart(XContentParser parser) throws IOException {
        // will result in START_OBJECT
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throwNoStartOnObject();
        }
    }

    private static void throwNoStartOnObject() {
        throw new DocumentParsingException(XContentLocation.UNKNOWN, "Malformed content, must start with an object");
    }

    private static void validateEnd(XContentParser parser) throws IOException {
        XContentParser.Token token;// only check for end of tokens if we created the parser here
        // try to parse the next token, this should be null if the object is ended properly
        // but will throw a JSON exception if the extra tokens is not valid JSON (this will be handled by the catch)
        token = parser.nextToken();
        if (token != null) {
            throwNotAtEnd(token);
        }
    }

    private static void throwNotAtEnd(XContentParser.Token token) {
        throw new IllegalArgumentException("Malformed content, found extra data after parsing: " + token);
    }

    private static boolean isEmptyDoc(RootObjectMapper root, XContentParser parser) throws IOException {
        if (root.isEnabled()) {
            final XContentParser.Token token = parser.nextToken();
            switch (token) {
                case END_OBJECT:
                    // empty doc, we can handle it...
                    return true;
                case FIELD_NAME:
                    return false;
                default:
                    throwOnMalformedContent(parser);
            }
        }
        return false;
    }

    private static void throwOnMalformedContent(XContentParser parser) {
        throw new DocumentParsingException(
            parser.getTokenLocation(),
            "Malformed content, after first object, either the type field or the actual properties should exist"
        );
    }

    private static DocumentParsingException wrapInDocumentParsingException(DocumentParserContext context, Exception e) {
        // if its already a document parsing exception, no need to wrap it...
        if (e instanceof DocumentParsingException) {
            return (DocumentParsingException) e;
        }
        return new DocumentParsingException(context.parser().getTokenLocation(), "failed to parse: " + e.getMessage(), e);
    }

    /**
     * Parses the current object context. Nested objects and source-keep capture are not supported by this parser;
     * the mapping is expected to have {@link ObjectMapper.Subobjects#DISABLED} on the root and no nested fields.
     */
    private static void parseObjectOrNested(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        String currentFieldName = parser.currentName();
        if (context.parent().isEnabled() == false) {
            // entire type is disabled — skip without capturing ignored source
            skipChildren(context);
            return;
        }
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            // the object is null ("obj1" : null), simply bail
            return;
        }
        if (token.isValue()) {
            throwOnConcreteValue(context.parent(), currentFieldName, context);
        }
        // if we are at the end of the previous object, advance
        if (token == XContentParser.Token.END_OBJECT) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            // if we are just starting an OBJECT, advance, this is the object we are parsing, we need the name first
            parser.nextToken();
        }
        innerParseObject(context);
    }

    private static void throwOnConcreteValue(ObjectMapper mapper, String currentFieldName, DocumentParserContext context) {
        throw new DocumentParsingException(
            context.parser().getTokenLocation(),
            "object mapping for ["
                + mapper.fullPath()
                + "] tried to parse field ["
                + currentFieldName
                + "] as object, but found a concrete value"
        );
    }

    private static void innerParseObject(DocumentParserContext context) throws IOException {

        final XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = null;
        assert token == XContentParser.Token.FIELD_NAME || token == XContentParser.Token.END_OBJECT;
        while (token != XContentParser.Token.END_OBJECT) {
            if (token == null) {
                throwEOF(context.parent(), context);
            }
            switch (token) {
                case FIELD_NAME:
                    currentFieldName = parser.currentName();
                    if (currentFieldName.isEmpty()) {
                        throw new IllegalArgumentException("Field name cannot be an empty string");
                    }
                    if (currentFieldName.isBlank()) {
                        throwFieldNameBlank(context, currentFieldName);
                    }
                    break;
                case START_OBJECT:
                    parseObject(context, currentFieldName);
                    break;
                case START_ARRAY:
                    parseArray(context, currentFieldName);
                    break;
                case VALUE_NULL:
                    parseNullValue(context, currentFieldName);
                    break;
                default:
                    if (token.isValue()) {
                        parseValue(context, currentFieldName);
                    }
                    break;
            }
            token = parser.nextToken();
        }
    }

    private static void throwFieldNameBlank(DocumentParserContext context, String currentFieldName) {
        throw new DocumentParsingException(
            context.parser().getTokenLocation(),
            "Field name cannot contain only whitespace: [" + context.path().pathAsText(currentFieldName) + "]"
        );
    }

    private static void throwEOF(ObjectMapper mapper, DocumentParserContext context) throws IOException {
        throw new DocumentParsingException(
            context.parser().getTokenLocation(),
            "object mapping for ["
                + mapper.fullPath()
                + "] tried to parse field ["
                + context.parser().currentName()
                + "] as object, but got EOF, has a concrete value been provided to it?"
        );
    }

    /**
     * Parses a field or object mapper entry. With subobjects disabled, the mapping tree contains no child
     * {@link ObjectMapper}s (they are flattened at build time), so the ObjectMapper recursion branch is
     * a safety net for unusual cases (e.g., metadata mappers) rather than the common path.
     */
    private static void parseObjectOrField(DocumentParserContext context, Mapper mapper) throws IOException {
        if (mapper instanceof ObjectMapper objectMapper) {
            parseObjectOrNested(context.createChildContext(objectMapper));
        } else if (mapper instanceof FieldMapper fieldMapper) {
            if (shouldFlattenObject(context, fieldMapper)) {
                // we pass the mapper's simpleName as parentName to the new DocumentParserContext
                String currentFieldName = fieldMapper.leafName();
                context.path().remove();
                parseObjectOrNested(context.createFlattenContext(currentFieldName));
                context.path().add(currentFieldName);
            } else {
                // No FALLBACK/source-keep capture — the flat parser intentionally drops that _source data.
                fieldMapper.parse(context);
            }
            if (context.isWithinCopyTo() == false) {
                List<String> copyToFields = fieldMapper.copyTo().copyToFields();
                if (copyToFields.isEmpty() == false) {
                    XContentParser.Token currentToken = context.parser().currentToken();
                    if (currentToken.isValue() == false && currentToken != XContentParser.Token.VALUE_NULL) {
                        // sanity check, we currently support copy-to only for value-type field, not objects
                        throwOnCopyToOnObject(mapper, copyToFields, context);
                    }
                    parseCopyFields(context, copyToFields);
                }
            }
        } else if (mapper instanceof FieldAliasMapper) {
            throwOnCopyToOnFieldAlias(context, mapper);
        } else {
            throwOnUnrecognizedMapperType(mapper);
        }
    }

    private static boolean shouldFlattenObject(DocumentParserContext context, FieldMapper fieldMapper) {
        return context.parser().currentToken() == XContentParser.Token.START_OBJECT
            && context.parent().subobjects() != ObjectMapper.Subobjects.ENABLED
            && fieldMapper.supportsParsingObject() == false;
    }

    private static void throwOnUnrecognizedMapperType(Mapper mapper) {
        throw new IllegalStateException(
            "The provided mapper [" + mapper.fullPath() + "] has an unrecognized type [" + mapper.getClass().getSimpleName() + "]."
        );
    }

    private static void throwOnCopyToOnFieldAlias(DocumentParserContext context, Mapper mapper) {
        throw new DocumentParsingException(
            context.parser().getTokenLocation(),
            "Cannot " + (context.isWithinCopyTo() ? "copy" : "write") + " to a field alias [" + mapper.fullPath() + "]."
        );
    }

    private static void throwOnCopyToOnObject(Mapper mapper, List<String> copyToFields, DocumentParserContext context) {
        throw new DocumentParsingException(
            context.parser().getTokenLocation(),
            "Cannot copy field ["
                + mapper.fullPath()
                + "] to fields "
                + copyToFields
                + ". Copy-to currently only works for value-type fields, not objects."
        );
    }

    private static void parseObject(final DocumentParserContext context, String currentFieldName) throws IOException {
        assert currentFieldName != null;
        var prev = context.getImmediateXContentParent();
        context.setImmediateXContentParent(context.parser().currentToken());
        Mapper objectMapper = context.getMapper(currentFieldName);
        if (objectMapper != null) {
            doParseObject(context, currentFieldName, objectMapper);
        } else {
            parseObjectDynamic(context, currentFieldName);
        }
        context.setImmediateXContentParent(prev);
    }

    private static void doParseObject(DocumentParserContext context, String currentFieldName, Mapper objectMapper) throws IOException {
        context.path().add(currentFieldName);
        boolean withinLeafObject = context.path().isWithinLeafObject();
        if (objectMapper instanceof ObjectMapper objMapper && objMapper.subobjects() != ObjectMapper.Subobjects.ENABLED) {
            context.path().setWithinLeafObject(true);
        }
        parseObjectOrField(context, objectMapper);
        context.path().setWithinLeafObject(withinLeafObject);
        context.path().remove();
    }

    /**
     * Handles a dynamically encountered object when there is no existing mapper for the field name. With
     * subobjects disabled, any object is flattened by prepending its name to child field names; nested
     * objects are forbidden. dynamic:runtime is not supported (ineligible mappings are routed to
     * {@link DocumentParser}).
     */
    private static void parseObjectDynamic(DocumentParserContext context, String currentFieldName) throws IOException {
        ensureNotStrict(context, currentFieldName);
        // With subobjects disabled, intermediate objects must be flattened rather than skipped.
        if (context.dynamic() == ObjectMapper.Dynamic.FALSE) {
            failIfMatchesRoutingPath(context, currentFieldName);
            skipChildren(context);
            return;
        }
        Mapper.Builder dynamicObjectBuilder = DynamicFieldsBuilder.createDynamicObjectMapperBuilder(context, currentFieldName);
        if (context.parent().subobjects() == ObjectMapper.Subobjects.DISABLED) {
            if (dynamicObjectBuilder instanceof NestedObjectMapper.Builder) {
                throw new DocumentParsingException(
                    context.parser().getTokenLocation(),
                    "Tried to add nested object ["
                        + dynamicObjectBuilder.leafName()
                        + "] to object ["
                        + context.parent().fullPath()
                        + "] which does not support subobjects"
                );
            }
            if (dynamicObjectBuilder instanceof ObjectMapper.Builder) {
                // subobjects disallowed → flatten children by prepending currentFieldName
                parseObjectOrNested(context.createFlattenContext(currentFieldName));
                return;
            }
        }
        Mapper dynamicObjectMapper = context.getDynamicMapper(dynamicObjectBuilder);
        if (dynamicObjectMapper == null) {
            failIfMatchesRoutingPath(context, currentFieldName);
            skipChildren(context);
            return;
        }
        doParseObject(context, currentFieldName, dynamicObjectMapper);
    }

    private static void parseArray(DocumentParserContext context, String lastFieldName) throws IOException {
        // Record previous immediate parent, so that it can be reset after array has been parsed.
        // This is for recording array offset with synthetic source. Only if the immediate parent is an array,
        // then the offsets can be accounted accurately.
        var prev = context.getImmediateXContentParent();
        context.setImmediateXContentParent(context.parser().currentToken());

        Mapper mapper = getLeafMapper(context, lastFieldName);
        if (mapper != null) {
            // There is a concrete mapper for this field already. Need to check if the mapper
            // expects an array, if so we pass the context straight to the mapper and if not
            // we serialize the array components
            if (parsesArrayValue(mapper)) {
                parseObjectOrField(context, mapper);
            } else {
                parseArrayElements(context, mapper, lastFieldName, lastFieldName);
            }
        } else {
            parseArrayDynamic(context, lastFieldName);
        }
        // Reset previous immediate parent
        context.setImmediateXContentParent(prev);
    }

    private static void parseArrayDynamic(DocumentParserContext context, String currentFieldName) throws IOException {
        ensureNotStrict(context, currentFieldName);
        if (context.dynamic() == ObjectMapper.Dynamic.FALSE) {
            // No ignored-source capture in the flat parser — just skip.
            skipChildren(context);
            return;
        }
        Mapper.Builder builderFromTemplate = DynamicFieldsBuilder.createObjectMapperBuilderFromTemplate(context, currentFieldName);
        if (builderFromTemplate == null) {
            if (context.indexSettings().isIgnoreDynamicFieldsBeyondLimit()
                && context.mappingLookup().exceedsLimit(context.indexSettings().getMappingTotalFieldsLimit(), 1)) {
                // Record in _ignored; no ignored-source capture needed.
                context.addIgnoredField(context.path().pathAsText(currentFieldName));
                return;
            }

            if (context.indexSettings().isIgnoreDynamicFieldNamesBeyondLimit()
                && currentFieldName.length() > context.indexSettings().getMappingFieldNameLengthLimit()) {
                // Record in _ignored; no ignored-source capture needed.
                context.addIgnoredField(context.path().pathAsText(currentFieldName));
                return;
            }

            parseArrayElements(context, null, currentFieldName, currentFieldName);
        } else {
            MapperBuilderContext builderContext = context.createDynamicMapperBuilderContext();
            Mapper objectMapperFromTemplate = builderFromTemplate.build(builderContext);
            if (parsesArrayValue(objectMapperFromTemplate)) {
                Mapper mapper = context.getDynamicMapper(builderFromTemplate, objectMapperFromTemplate, builderContext);
                if (mapper == null) {
                    skipChildren(context);
                    return;
                }
                context.path().add(currentFieldName);
                parseObjectOrField(context, mapper);
                context.path().remove();
            } else {
                parseArrayElements(context, objectMapperFromTemplate, currentFieldName, currentFieldName);
            }
        }
    }

    private static boolean parsesArrayValue(Mapper mapper) {
        return mapper instanceof FieldMapper && ((FieldMapper) mapper).parsesArrayValue();
    }

    /**
     * Iterates over array elements. Synthetic-source-keep and FALLBACK ignored-source capture are not performed;
     * the flat parser intentionally drops that {@code _source} data.
     */
    private static void parseArrayElements(
        DocumentParserContext context,
        @Nullable Mapper mapper,
        final String lastFieldName,
        String arrayFieldName
    ) throws IOException {

        XContentParser parser = context.parser();
        XContentParser.Token token;
        int valueElements = 0;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.START_OBJECT) {
                context.incrementAndCheckObjectArrayElementLimit();
                parseObject(context, lastFieldName);
            } else if (token == XContentParser.Token.START_ARRAY) {
                parseArray(context, lastFieldName);
            } else if (token == XContentParser.Token.VALUE_NULL) {
                parseNullValue(context, lastFieldName);
            } else if (token == null) {
                throwEOFOnParseArray(arrayFieldName, context);
            } else {
                assert token.isValue();
                parseValue(context, lastFieldName);
                valueElements++;
            }
        }
        postProcessDynamicArrayMapping(context, lastFieldName, valueElements);
    }

    /**
     * Arrays that have been classified as floats and meet specific criteria are re-mapped to dense_vector.
     */
    private static void postProcessDynamicArrayMapping(DocumentParserContext context, String fieldName, int arraySize) {
        // cheap/free early return checks
        if (arraySize < MIN_DIMS_FOR_DYNAMIC_FLOAT_MAPPING
            || arraySize > MAX_DIMS_COUNT
            || context.indexSettings().getIndexVersionCreated().before(DYNAMICALLY_MAP_DENSE_VECTORS_INDEX_VERSION)) {
            return;
        }

        final MapperBuilderContext builderContext = context.createDynamicMapperBuilderContext();
        final String fullFieldName = builderContext.buildFullName(fieldName);
        final List<Mapper.Builder> builders = context.getDynamicMappers(fullFieldName);
        // non-cheap/free early return checks
        if (builders == null
            || context.isFieldAppliedFromTemplate(fullFieldName)
            || context.isCopyToDestinationField(fullFieldName)
            || builders.stream()
                .anyMatch(
                    b -> b instanceof NumberFieldMapper.Builder == false
                        || ((NumberFieldMapper.Builder) b).type() != NumberFieldMapper.NumberType.FLOAT
                )) {
            return;
        }

        DenseVectorFieldMapper.Builder builder = new DenseVectorFieldMapper.Builder(
            fieldName,
            context.indexSettings().getIndexVersionCreated(),
            context.indexSettings().getMode(),
            IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.get(context.indexSettings().getSettings()),
            IndexSettings.DENSE_VECTOR_EXPERIMENTAL_FEATURES_SETTING.get(context.indexSettings().getSettings()),
            context.getVectorFormatProviders(),
            context.indexSettings().isIndexDisabledByDefault()
        );
        builder.dimensions(arraySize);
        context.updateDynamicMappers(fullFieldName, List.of(builder));
    }

    private static void throwEOFOnParseArray(String arrayFieldName, DocumentParserContext context) {
        throw new DocumentParsingException(
            context.parser().getTokenLocation(),
            "object mapping for ["
                + context.parent().fullPath()
                + "] with array for ["
                + arrayFieldName
                + "] tried to parse as array, but got EOF, is there a mismatch in types for the same field?"
        );
    }

    private static void parseValue(final DocumentParserContext context, String currentFieldName) throws IOException {
        if (currentFieldName == null) {
            throwOnNoFieldName(context);
        }
        Mapper mapper = getLeafMapper(context, currentFieldName);
        if (mapper != null) {
            parseObjectOrField(context, mapper);
        } else {
            parseDynamicValue(context, currentFieldName);
        }
    }

    private static void throwOnNoFieldName(DocumentParserContext context) throws IOException {
        throw new DocumentParsingException(
            context.parser().getTokenLocation(),
            "object mapping ["
                + context.parent().fullPath()
                + "] trying to serialize a value with"
                + " no field associated with it, current value ["
                + context.parser().textOrNull()
                + "]"
        );
    }

    private static void parseNullValue(DocumentParserContext context, String lastFieldName) throws IOException {
        // we can only handle null values if we have mappings for them
        Mapper mapper = getLeafMapper(context, lastFieldName);
        if (mapper != null) {
            // TODO: passing null to an object seems bogus?
            parseObjectOrField(context, mapper);
        } else {
            ensureNotStrict(context, lastFieldName);
        }
    }

    private static void parseDynamicValue(DocumentParserContext context, String currentFieldName) throws IOException {
        ensureNotStrict(context, currentFieldName);
        if (context.dynamic() == ObjectMapper.Dynamic.FALSE) {
            failIfMatchesRoutingPath(context, currentFieldName);
            // No ignored-source capture in the flat parser — unmapped values under dynamic:false are dropped.
            return;
        }
        if (context.dynamic().getDynamicFieldsBuilder().createDynamicFieldFromValue(context, currentFieldName) == false) {
            failIfMatchesRoutingPath(context, currentFieldName);
        }
    }

    private static void ensureNotStrict(DocumentParserContext context, String currentFieldName) {
        if (context.dynamic() == ObjectMapper.Dynamic.STRICT) {
            throw new StrictDynamicMappingException(context.parser().getTokenLocation(), context.parent().fullPath(), currentFieldName);
        }
    }

    private static void failIfMatchesRoutingPath(DocumentParserContext context, String currentFieldName) {
        if (context.indexSettings().getIndexMetadata().getRoutingPaths().isEmpty()) {
            return;
        }
        String path = context.parent().fullPath().isEmpty() ? currentFieldName : context.parent().fullPath() + "." + currentFieldName;
        if (Regex.simpleMatch(context.indexSettings().getIndexMetadata().getRoutingPaths(), path)) {
            throw new DocumentParsingException(
                context.parser().getTokenLocation(),
                "All fields matching [routing_path] must be mapped but [" + path + "] was declared as [dynamic: false]"
            );
        }
    }

    /**
     * Creates instances of the fields that the current field should be copied to
     */
    private static void parseCopyFields(DocumentParserContext context, List<String> copyToFields) throws IOException {
        for (String field : copyToFields) {
            if (context.mappingLookup().inferenceFields().get(field) != null) {
                // ignore copy_to that targets inference fields, values are already extracted in the coordinating node to perform inference.
                continue;
            }
            // In case of a hierarchy of nested documents, we need to figure out
            // which document the field should go to
            LuceneDocument targetDoc = null;
            for (LuceneDocument doc = context.doc(); doc != null; doc = doc.getParent()) {
                if (field.startsWith(doc.getPrefix())) {
                    targetDoc = doc;
                    break;
                }
            }
            assert targetDoc != null;
            final DocumentParserContext copyToContext = context.createCopyToContext(field, targetDoc);
            innerParseObject(copyToContext);
            context.markFieldAsCopyTo(field);
        }
    }

    // looks up a child mapper; runtime fields are not supported (ineligible mappings are routed to DocumentParser)
    private static Mapper getLeafMapper(final DocumentParserContext context, String fieldName) {
        return context.getMapper(fieldName);
    }
}
