/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MIN_DIMS_FOR_DYNAMIC_FLOAT_MAPPING;

/**
 * A parser for documents
 */
public final class DocumentParser {

    public static final IndexVersion DYNAMICALLY_MAP_DENSE_VECTORS_INDEX_VERSION = IndexVersions.FIRST_DETACHED_INDEX_VERSION;

    private final XContentParserConfiguration parserConfiguration;
    private final MappingParserContext mappingParserContext;

    DocumentParser(XContentParserConfiguration parserConfiguration, MappingParserContext mappingParserContext) {
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
        if (source.source() != null && source.source().length() == 0) {
            throw new DocumentParsingException(new XContentLocation(0, 0), "failed to parse, document is empty");
        }
        final RootDocumentParserContext context;
        final XContentType xContentType = source.getXContentType();

        XContentMeteringParserDecorator meteringParserDecorator = source.getDocumentSizeObserver();
        try (
            XContentParser parser = meteringParserDecorator.decorate(
                XContentHelper.createParser(parserConfiguration, source.source(), xContentType)
            )
        ) {
            context = new RootDocumentParserContext(mappingLookup, mappingParserContext, source, parser);
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
        assert context.path.pathAsText("").isEmpty() : "found leftover path elements: " + context.path.pathAsText("");

        Mapping dynamicUpdate = createDynamicUpdate(context);

        return new ParsedDocument(
            context.version(),
            context.seqID(),
            context.id(),
            context.routing(),
            context.reorderParentAndGetDocs(),
            context.sourceToParse().source(),
            context.sourceToParse().getXContentType(),
            dynamicUpdate,
            meteringParserDecorator.meteredDocumentSize()
        ) {
            @Override
            public String documentDescription() {
                IdFieldMapper idMapper = (IdFieldMapper) mappingLookup.getMapping().getMetadataMapperByName(IdFieldMapper.NAME);
                return idMapper.documentDescription(this);
            }
        };
    }

    private void internalParseDocument(MetadataFieldMapper[] metadataFieldsMappers, DocumentParserContext context) {
        try {
            final boolean emptyDoc = isEmptyDoc(context.root(), context.parser());

            for (MetadataFieldMapper metadataMapper : metadataFieldsMappers) {
                metadataMapper.preParse(context);
            }

            if (context.root().isEnabled() == false) {
                // entire type is disabled
                if (context.canAddIgnoredField()) {
                    context.addIgnoredField(
                        new IgnoredSourceFieldMapper.NameValue(
                            MapperService.SINGLE_MAPPING_NAME,
                            0,
                            context.encodeFlattenedToken(),
                            context.doc()
                        )
                    );
                } else {
                    context.parser().skipChildren();
                }
            } else if (emptyDoc == false) {
                parseObjectOrNested(context);
            }

            executeIndexTimeScripts(context);

            // Record additional entries for {@link IgnoredSourceFieldMapper} before calling #postParse, so that they get stored.
            addIgnoredSourceMissingValues(context);

            for (MetadataFieldMapper metadataMapper : metadataFieldsMappers) {
                metadataMapper.postParse(context);
            }
        } catch (Exception e) {
            throw wrapInDocumentParsingException(context, e);
        }
    }

    private void addIgnoredSourceMissingValues(DocumentParserContext context) throws IOException {
        Collection<IgnoredSourceFieldMapper.NameValue> ignoredFieldsMissingValues = context.getIgnoredFieldsMissingValues();
        if (ignoredFieldsMissingValues.isEmpty()) {
            return;
        }

        // Clean up any conflicting ignored values, to avoid double-printing them as array elements in synthetic source.
        Map<String, IgnoredSourceFieldMapper.NameValue> fields = new HashMap<>(ignoredFieldsMissingValues.size());
        for (var field : ignoredFieldsMissingValues) {
            fields.put(field.name(), field);
        }
        context.deduplicateIgnoredFieldValues(fields.keySet());

        assert context.mappingLookup().isSourceSynthetic();
        try (
            XContentParser parser = XContentHelper.createParser(
                parserConfiguration,
                context.sourceToParse().source(),
                context.sourceToParse().getXContentType()
            )
        ) {
            DocumentParserContext newContext = new RootDocumentParserContext(
                context.mappingLookup(),
                mappingParserContext,
                context.sourceToParse(),
                parser
            );
            var nameValues = parseDocForMissingValues(newContext, fields);
            for (var nameValue : nameValues) {
                context.addIgnoredField(nameValue);
            }
        }
    }

    /**
     * Simplified parsing version for retrieving the source of a given set of fields.
     */
    private static List<IgnoredSourceFieldMapper.NameValue> parseDocForMissingValues(
        DocumentParserContext context,
        Map<String, IgnoredSourceFieldMapper.NameValue> fields
    ) throws IOException {
        // Generate all possible parent names for the given fields.
        // This is used to skip processing objects that can't generate missing values.
        Set<String> parentNames = getPossibleParentNames(fields.keySet());
        List<IgnoredSourceFieldMapper.NameValue> result = new ArrayList<>();

        XContentParser parser = context.parser();
        XContentParser.Token currentToken = parser.nextToken();
        List<String> path = new ArrayList<>();
        String fieldName = null;
        while (currentToken != null) {
            while (currentToken != XContentParser.Token.FIELD_NAME) {
                if (fieldName != null
                    && (currentToken == XContentParser.Token.START_OBJECT || currentToken == XContentParser.Token.START_ARRAY)) {
                    if (parentNames.contains(getCurrentPath(path, fieldName)) == false) {
                        // No missing value under this parsing subtree, skip it.
                        parser.skipChildren();
                    } else {
                        path.add(fieldName);
                    }
                    fieldName = null;
                } else if (currentToken == XContentParser.Token.END_OBJECT || currentToken == XContentParser.Token.END_ARRAY) {
                    if (currentToken == XContentParser.Token.END_OBJECT && path.isEmpty() == false) {
                        path.removeLast();
                    }
                    fieldName = null;
                }
                currentToken = parser.nextToken();
                if (currentToken == null) {
                    return result;
                }
            }
            fieldName = parser.currentName();
            String fullName = getCurrentPath(path, fieldName);
            var leaf = fields.get(fullName);  // There may be multiple matches for array elements, don't use #remove.
            if (leaf != null) {
                parser.nextToken();  // Advance the parser to the value to be read.
                result.add(leaf.cloneWithValue(context.encodeFlattenedToken()));
                parser.nextToken();  // Skip the token ending the value.
                fieldName = null;
            }
            currentToken = parser.nextToken();
        }
        return result;
    }

    private static String getCurrentPath(List<String> path, String fieldName) {
        assert fieldName != null;
        return path.isEmpty() ? fieldName : String.join(".", path) + "." + fieldName;
    }

    /**
     * Generates all possible parent object names for the given full names.
     * For instance, for input ['path.to.foo', 'another.path.to.bar'], it returns:
     * [ 'path', 'path.to', 'another', 'another.path', 'another.path.to' ]
     */
    private static Set<String> getPossibleParentNames(Set<String> fullPaths) {
        if (fullPaths.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> paths = new HashSet<>();
        for (String fullPath : fullPaths) {
            String[] split = fullPath.split("\\.");
            if (split.length < 2) {
                continue;
            }
            StringBuilder builder = new StringBuilder(split[0]);
            paths.add(builder.toString());
            for (int i = 1; i < split.length - 1; i++) {
                builder.append(".");
                builder.append(split[i]);
                paths.add(builder.toString());
            }
        }
        return paths;
    }

    private static void executeIndexTimeScripts(DocumentParserContext context) {
        List<FieldMapper> indexTimeScriptMappers = context.mappingLookup().indexTimeScriptMappers();
        if (indexTimeScriptMappers.isEmpty()) {
            return;
        }
        SearchLookup searchLookup = new SearchLookup(
            context.mappingLookup().indexTimeLookup()::get,
            (ft, lookup, fto) -> ft.fielddataBuilder(
                new FieldDataContext(
                    context.indexSettings().getIndex().getName(),
                    context.indexSettings(),
                    lookup,
                    context.mappingLookup()::sourcePaths,
                    fto
                )
            ).build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService()),
            (ctx, doc) -> Source.fromBytes(context.sourceToParse().source())
        );
        // field scripts can be called both by the loop at the end of this method and via
        // the document reader, so to ensure that we don't run them multiple times we
        // guard them with an 'executed' boolean
        Map<String, Consumer<LeafReaderContext>> fieldScripts = new HashMap<>();
        indexTimeScriptMappers.forEach(mapper -> fieldScripts.put(mapper.fullPath(), new Consumer<>() {
            boolean executed = false;

            @Override
            public void accept(LeafReaderContext leafReaderContext) {
                if (executed == false) {
                    mapper.executeScript(searchLookup, leafReaderContext, 0, context);
                    executed = true;
                }
            }
        }));

        // call the index script on all field mappers configured with one
        DocumentLeafReader reader = new DocumentLeafReader(context.rootDoc(), fieldScripts);
        for (Consumer<LeafReaderContext> script : fieldScripts.values()) {
            script.accept(reader.getContext());
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

    private static Mapper.SourceKeepMode getSourceKeepMode(DocumentParserContext context, Optional<Mapper.SourceKeepMode> mapperMode) {
        return mapperMode.orElseGet(context::sourceKeepModeFromIndexSettings);
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

    static Mapping createDynamicUpdate(DocumentParserContext context) {
        if (context.hasDynamicMappersOrRuntimeFields() == false) {
            return null;
        }
        RootObjectMapper.Builder rootBuilder = context.updateRoot();
        context.getDynamicMappers().forEach(mapper -> rootBuilder.addDynamic(mapper.fullPath(), null, mapper, context));

        for (RuntimeField runtimeField : context.getDynamicRuntimeFields()) {
            rootBuilder.addRuntimeField(runtimeField);
        }
        RootObjectMapper root = rootBuilder.build(MapperBuilderContext.root(context.mappingLookup().isSourceSynthetic(), false));
        return context.mappingLookup().getMapping().mappingUpdate(root);
    }

    static void parseObjectOrNested(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        String currentFieldName = parser.currentName();
        if (context.parent().isEnabled() == false) {
            // entire type is disabled
            if (context.canAddIgnoredField()) {
                context.addIgnoredField(
                    new IgnoredSourceFieldMapper.NameValue(
                        context.parent().fullPath(),
                        context.parent().fullPath().lastIndexOf(context.parent().leafName()),
                        context.encodeFlattenedToken(),
                        context.doc()
                    )
                );
            } else {
                parser.skipChildren();
            }
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

        if (context.canAddIgnoredField() && getSourceKeepMode(context, context.parent().sourceKeepMode()) == Mapper.SourceKeepMode.ALL) {
            context = context.addIgnoredFieldFromContext(
                new IgnoredSourceFieldMapper.NameValue(
                    context.parent().fullPath(),
                    context.parent().fullPath().lastIndexOf(context.parent().leafName()),
                    null,
                    context.doc()
                )
            );
            token = context.parser().currentToken();
            parser = context.parser();
        }

        if (context.parent().isNested()) {
            // Handle a nested object that doesn't contain an array. Arrays are handled in #parseNonDynamicArray.
            context = context.createNestedContext((NestedObjectMapper) context.parent());
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
        // restore the enable path flag
        if (context.parent().isNested()) {
            copyNestedFields(context, (NestedObjectMapper) context.parent());
        }
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

    private static void copyNestedFields(DocumentParserContext context, NestedObjectMapper nested) {
        if (context.isWithinCopyTo()) {
            // Only process the nested document after we've finished parsing the actual
            // doc; we can't copy_to outside of the current nested context, so if we are
            // in a copy_to context then we're adding data within the doc and we haven't
            // finished parsing yet.
            return;
        }
        LuceneDocument nestedDoc = context.doc();
        LuceneDocument parentDoc = nestedDoc.getParent();
        IndexVersion indexVersion = context.indexSettings().getIndexVersionCreated();
        if (nested.isIncludeInParent()) {
            addFields(indexVersion, nestedDoc, parentDoc);
        }
        if (nested.isIncludeInRoot()) {
            LuceneDocument rootDoc = context.rootDoc();
            // don't add it twice, if its included in parent, and we are handling the master doc...
            if (nested.isIncludeInParent() == false || parentDoc != rootDoc) {
                addFields(indexVersion, nestedDoc, rootDoc);
            }
        }
    }

    private static void addFields(IndexVersion indexCreatedVersion, LuceneDocument nestedDoc, LuceneDocument rootDoc) {
        String nestedPathFieldName = NestedPathFieldMapper.name(indexCreatedVersion);
        for (IndexableField field : nestedDoc.getFields()) {
            if (field.name().equals(nestedPathFieldName) == false && field.name().equals(IdFieldMapper.NAME) == false) {
                rootDoc.add(field);
            }
        }
    }

    static void parseObjectOrField(DocumentParserContext context, Mapper mapper) throws IOException {
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
                if (context.canAddIgnoredField()
                    && (fieldMapper.syntheticSourceMode() == FieldMapper.SyntheticSourceMode.FALLBACK
                        || getSourceKeepMode(context, fieldMapper.sourceKeepMode()) == Mapper.SourceKeepMode.ALL
                        || (context.isWithinCopyTo() == false && context.isCopyToDestinationField(mapper.fullPath())))) {
                    context = context.addIgnoredFieldFromContext(
                        IgnoredSourceFieldMapper.NameValue.fromContext(context, fieldMapper.fullPath(), null)
                    );
                    fieldMapper.parse(context);
                } else {
                    fieldMapper.parse(context);
                }
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
        Mapper objectMapper = context.getMapper(currentFieldName);
        if (objectMapper != null) {
            doParseObject(context, currentFieldName, objectMapper);
        } else {
            parseObjectDynamic(context, currentFieldName);
        }
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

    private static void parseObjectDynamic(DocumentParserContext context, String currentFieldName) throws IOException {
        ensureNotStrict(context, currentFieldName);
        if (context.dynamic() == ObjectMapper.Dynamic.FALSE) {
            failIfMatchesRoutingPath(context, currentFieldName);
            if (context.canAddIgnoredField()) {
                context.addIgnoredField(
                    IgnoredSourceFieldMapper.NameValue.fromContext(
                        context,
                        context.path().pathAsText(currentFieldName),
                        context.encodeFlattenedToken()
                    )
                );
            } else {
                // not dynamic, read everything up to end object
                context.parser().skipChildren();
            }
        } else {
            Mapper dynamicObjectMapper;
            if (context.dynamic() == ObjectMapper.Dynamic.RUNTIME) {
                // with dynamic:runtime all leaf fields will be runtime fields unless explicitly mapped,
                // hence we don't dynamically create empty objects under properties, but rather carry around an artificial object mapper
                dynamicObjectMapper = new NoOpObjectMapper(currentFieldName, context.path().pathAsText(currentFieldName));
                if (context.canAddIgnoredField()) {
                    context = context.addIgnoredFieldFromContext(
                        IgnoredSourceFieldMapper.NameValue.fromContext(context, context.path().pathAsText(currentFieldName), null)
                    );
                }
            } else {
                dynamicObjectMapper = DynamicFieldsBuilder.createDynamicObjectMapper(context, currentFieldName);
            }
            if (context.parent().subobjects() == ObjectMapper.Subobjects.DISABLED) {
                if (dynamicObjectMapper instanceof NestedObjectMapper) {
                    throw new DocumentParsingException(
                        context.parser().getTokenLocation(),
                        "Tried to add nested object ["
                            + dynamicObjectMapper.leafName()
                            + "] to object ["
                            + context.parent().fullPath()
                            + "] which does not support subobjects"
                    );
                }
                if (dynamicObjectMapper instanceof ObjectMapper) {
                    // We have an ObjectMapper but subobjects are disallowed
                    // therefore we create a new DocumentParserContext that
                    // prepends currentFieldName to any immediate children.
                    parseObjectOrNested(context.createFlattenContext(currentFieldName));
                    return;
                }

            }
            if (context.dynamic() != ObjectMapper.Dynamic.RUNTIME) {
                if (context.addDynamicMapper(dynamicObjectMapper) == false) {
                    failIfMatchesRoutingPath(context, currentFieldName);
                    context.parser().skipChildren();
                    return;
                }
            }
            if (dynamicObjectMapper instanceof NestedObjectMapper && context.isWithinCopyTo()) {
                throwOnCreateDynamicNestedViaCopyTo(dynamicObjectMapper, context);
            }
            doParseObject(context, currentFieldName, dynamicObjectMapper);
        }
    }

    private static void throwOnCreateDynamicNestedViaCopyTo(Mapper dynamicObjectMapper, DocumentParserContext context) {
        throw new DocumentParsingException(
            context.parser().getTokenLocation(),
            "It is forbidden to create dynamic nested objects ([" + dynamicObjectMapper.fullPath() + "]) through `copy_to`"
        );
    }

    private static void parseArray(DocumentParserContext context, String lastFieldName) throws IOException {
        Mapper mapper = getLeafMapper(context, lastFieldName);
        if (mapper != null) {
            // There is a concrete mapper for this field already. Need to check if the mapper
            // expects an array, if so we pass the context straight to the mapper and if not
            // we serialize the array components
            if (parsesArrayValue(mapper)) {
                parseObjectOrField(context, mapper);
            } else {
                parseNonDynamicArray(context, mapper, lastFieldName, lastFieldName);
            }
        } else {
            parseArrayDynamic(context, lastFieldName);
        }
    }

    private static void parseArrayDynamic(DocumentParserContext context, String currentFieldName) throws IOException {
        ensureNotStrict(context, currentFieldName);
        if (context.dynamic() == ObjectMapper.Dynamic.FALSE) {
            if (context.canAddIgnoredField()) {
                context.addIgnoredField(
                    IgnoredSourceFieldMapper.NameValue.fromContext(
                        context,
                        context.path().pathAsText(currentFieldName),
                        context.encodeFlattenedToken()
                    )
                );
            } else {
                context.parser().skipChildren();
            }
            return;
        }
        Mapper objectMapperFromTemplate = DynamicFieldsBuilder.createObjectMapperFromTemplate(context, currentFieldName);
        if (objectMapperFromTemplate == null) {
            if (context.indexSettings().isIgnoreDynamicFieldsBeyondLimit()
                && context.mappingLookup().exceedsLimit(context.indexSettings().getMappingTotalFieldsLimit(), 1)) {
                if (context.canAddIgnoredField()) {
                    try {
                        context.addIgnoredField(
                            IgnoredSourceFieldMapper.NameValue.fromContext(
                                context,
                                context.path().pathAsText(currentFieldName),
                                context.encodeFlattenedToken()
                            )
                        );
                    } catch (IOException e) {
                        throw new IllegalArgumentException("failed to parse field [" + currentFieldName + " ]", e);
                    }
                }
                context.addIgnoredField(currentFieldName);
                return;
            }
            parseNonDynamicArray(context, objectMapperFromTemplate, currentFieldName, currentFieldName);
        } else {
            if (parsesArrayValue(objectMapperFromTemplate)) {
                if (context.addDynamicMapper(objectMapperFromTemplate) == false) {
                    context.parser().skipChildren();
                    return;
                }
                context.path().add(currentFieldName);
                parseObjectOrField(context, objectMapperFromTemplate);
                context.path().remove();
            } else {
                parseNonDynamicArray(context, objectMapperFromTemplate, currentFieldName, currentFieldName);
            }
        }
    }

    private static boolean parsesArrayValue(Mapper mapper) {
        return mapper instanceof FieldMapper && ((FieldMapper) mapper).parsesArrayValue();
    }

    private static void parseNonDynamicArray(
        DocumentParserContext context,
        @Nullable Mapper mapper,
        final String lastFieldName,
        String arrayFieldName
    ) throws IOException {
        String fullPath = context.path().pathAsText(arrayFieldName);

        // Check if we need to record the array source. This only applies to synthetic source.
        boolean canRemoveSingleLeafElement = false;
        if (context.canAddIgnoredField()) {
            Mapper.SourceKeepMode mode = Mapper.SourceKeepMode.NONE;
            boolean objectWithFallbackSyntheticSource = false;
            if (mapper instanceof ObjectMapper objectMapper) {
                mode = getSourceKeepMode(context, objectMapper.sourceKeepMode());
                objectWithFallbackSyntheticSource = (mode == Mapper.SourceKeepMode.ALL
                    || (mode == Mapper.SourceKeepMode.ARRAYS && objectMapper instanceof NestedObjectMapper == false));
            }
            boolean fieldWithFallbackSyntheticSource = false;
            boolean fieldWithStoredArraySource = false;
            if (mapper instanceof FieldMapper fieldMapper) {
                mode = getSourceKeepMode(context, fieldMapper.sourceKeepMode());
                fieldWithFallbackSyntheticSource = fieldMapper.syntheticSourceMode() == FieldMapper.SyntheticSourceMode.FALLBACK;
                fieldWithStoredArraySource = mode != Mapper.SourceKeepMode.NONE;
            }
            boolean copyToFieldHasValuesInDocument = context.isWithinCopyTo() == false && context.isCopyToDestinationField(fullPath);

            canRemoveSingleLeafElement = mapper instanceof FieldMapper
                && mode == Mapper.SourceKeepMode.ARRAYS
                && fieldWithFallbackSyntheticSource == false
                && copyToFieldHasValuesInDocument == false;

            if (objectWithFallbackSyntheticSource
                || fieldWithFallbackSyntheticSource
                || fieldWithStoredArraySource
                || copyToFieldHasValuesInDocument) {
                context = context.addIgnoredFieldFromContext(IgnoredSourceFieldMapper.NameValue.fromContext(context, fullPath, null));
            } else if (mapper instanceof ObjectMapper objectMapper && (objectMapper.isEnabled() == false)) {
                // No need to call #addIgnoredFieldFromContext as both singleton and array instances of this object
                // get tracked through ignored source.
                context.addIgnoredField(IgnoredSourceFieldMapper.NameValue.fromContext(context, fullPath, context.encodeFlattenedToken()));
                return;
            }
        }

        // In synthetic source, if any array element requires storing its source as-is, it takes precedence over
        // elements from regular source loading that are then skipped from the synthesized array source.
        // To prevent this, we track that parsing sub-context is within array scope.
        context = context.maybeCloneForArray(mapper);

        XContentParser parser = context.parser();
        XContentParser.Token token;
        int elements = 0;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.START_OBJECT) {
                elements = Integer.MAX_VALUE;
                parseObject(context, lastFieldName);
            } else if (token == XContentParser.Token.START_ARRAY) {
                elements = Integer.MAX_VALUE;
                parseArray(context, lastFieldName);
            } else if (token == XContentParser.Token.VALUE_NULL) {
                elements++;
                parseNullValue(context, lastFieldName);
            } else if (token == null) {
                throwEOFOnParseArray(arrayFieldName, context);
            } else {
                assert token.isValue();
                elements++;
                parseValue(context, lastFieldName);
            }
        }
        if (elements <= 1 && canRemoveSingleLeafElement) {
            context.removeLastIgnoredField(fullPath);
        }
        postProcessDynamicArrayMapping(context, lastFieldName);
    }

    /**
     * Arrays that have been classified as floats and meet specific criteria are re-mapped to dense_vector.
     */
    private static void postProcessDynamicArrayMapping(DocumentParserContext context, String fieldName) {
        if (context.indexSettings().getIndexVersionCreated().onOrAfter(DYNAMICALLY_MAP_DENSE_VECTORS_INDEX_VERSION)) {
            final MapperBuilderContext builderContext = context.createDynamicMapperBuilderContext();
            final String fullFieldName = builderContext.buildFullName(fieldName);
            final List<Mapper> mappers = context.getDynamicMappers(fullFieldName);
            if (mappers == null
                || context.isFieldAppliedFromTemplate(fullFieldName)
                || context.isCopyToDestinationField(fullFieldName)
                || mappers.size() < MIN_DIMS_FOR_DYNAMIC_FLOAT_MAPPING
                || mappers.size() > MAX_DIMS_COUNT
                // Anything that is NOT a number or anything that IS a number but not mapped to `float` should NOT be mapped to dense_vector
                || mappers.stream()
                    .anyMatch(
                        m -> m instanceof NumberFieldMapper == false || ((NumberFieldMapper) m).type() != NumberFieldMapper.NumberType.FLOAT
                    )) {
                return;
            }

            DenseVectorFieldMapper.Builder builder = new DenseVectorFieldMapper.Builder(
                fieldName,
                context.indexSettings().getIndexVersionCreated()
            );
            DenseVectorFieldMapper denseVectorFieldMapper = builder.build(builderContext);
            context.updateDynamicMappers(fullFieldName, List.of(denseVectorFieldMapper));
        }
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
            if (context.canAddIgnoredField()) {
                context.addIgnoredField(
                    IgnoredSourceFieldMapper.NameValue.fromContext(
                        context,
                        context.path().pathAsText(currentFieldName),
                        context.encodeFlattenedToken()
                    )
                );
            }
            return;
        }
        if (context.dynamic() == ObjectMapper.Dynamic.RUNTIME && context.canAddIgnoredField()) {
            context.addIgnoredField(
                IgnoredSourceFieldMapper.NameValue.fromContext(
                    context,
                    context.path().pathAsText(currentFieldName),
                    context.encodeFlattenedToken()
                )
            );
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

    // looks up a child mapper
    // if no mapper is found, checks to see if a runtime field with the specified
    // field name exists and if so returns a no-op mapper to prevent indexing
    private static Mapper getLeafMapper(final DocumentParserContext context, String fieldName) {
        Mapper mapper = context.getMapper(fieldName);
        if (mapper != null) {
            return mapper;
        }
        // concrete fields take precedence over runtime fields when parsing documents
        // if a leaf field is not mapped, and is defined as a runtime field, then we
        // don't create a dynamic mapping for it and don't index it.
        String fieldPath = context.path().pathAsText(fieldName);
        MappedFieldType fieldType = context.mappingLookup().getFieldType(fieldPath);
        if (fieldType != null) {
            // we haven't found a mapper with this name above, which means if a field type is found it is for sure a runtime field.
            assert fieldType.hasDocValues() == false && fieldType.isAggregatable() && fieldType.isSearchable();
            return noopFieldMapper(fieldPath);
        }
        return null;
    }

    private static FieldMapper noopFieldMapper(String path) {
        return new FieldMapper("no-op", new MappedFieldType("no-op", false, false, false, TextSearchInfo.NONE, Collections.emptyMap()) {
            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String typeName() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Query termQuery(Object value, SearchExecutionContext context) {
                throw new UnsupportedOperationException();
            }
        }, FieldMapper.BuilderParams.empty()) {

            @Override
            protected void parseCreateField(DocumentParserContext context) {
                // Run-time fields are mapped to this mapper, so it needs to handle storing values for use in synthetic source.
                // #parseValue calls this method once the run-time field is created.
                if (context.dynamic() == ObjectMapper.Dynamic.RUNTIME && context.canAddIgnoredField()) {
                    try {
                        context.addIgnoredField(
                            IgnoredSourceFieldMapper.NameValue.fromContext(context, path, context.encodeFlattenedToken())
                        );
                    } catch (IOException e) {
                        throw new IllegalArgumentException(
                            "failed to parse run-time field under [" + context.path().pathAsText("") + " ]",
                            e
                        );
                    }
                }
            }

            @Override
            public String fullPath() {
                return path;
            }

            @Override
            public String typeName() {
                throw new UnsupportedOperationException();
            }

            @Override
            public MappedFieldType fieldType() {
                throw new UnsupportedOperationException();
            }

            @Override
            public MultiFields multiFields() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterator<Mapper> iterator() {
                throw new UnsupportedOperationException();
            }

            @Override
            protected void doValidate(MappingLookup mappers) {
                throw new UnsupportedOperationException();
            }

            @Override
            protected void checkIncomingMergeType(FieldMapper mergeWith) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Builder getMergeBuilder() {
                throw new UnsupportedOperationException();
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) {
                throw new UnsupportedOperationException();
            }

            @Override
            protected String contentType() {
                throw new UnsupportedOperationException();
            }

            @Override
            protected SyntheticSourceSupport syntheticSourceSupport() {
                // Opt out of fallback synthetic source implementation
                // since there is custom logic in #parseCreateField().
                return new SyntheticSourceSupport.Native(SourceLoader.SyntheticFieldLoader.NOTHING);
            }
        };
    }

    private static class NoOpObjectMapper extends ObjectMapper {
        NoOpObjectMapper(String name, String fullPath) {
            super(name, fullPath, Explicit.IMPLICIT_TRUE, Optional.empty(), Optional.empty(), Dynamic.RUNTIME, Collections.emptyMap());
        }

        @Override
        public ObjectMapper merge(Mapper mergeWith, MapperMergeContext mapperMergeContext) {
            return this;
        }
    }

    /**
     * Internal version of {@link DocumentParserContext} that is aware of implementation details like nested documents
     * and how they are stored in the lucene index.
     */
    private static class RootDocumentParserContext extends DocumentParserContext {
        private final ContentPath path = new ContentPath();
        private final XContentParser parser;
        private final LuceneDocument document;
        private final List<LuceneDocument> documents = new ArrayList<>();
        private final long maxAllowedNumNestedDocs;
        private long numNestedDocs;
        private boolean docsReversed = false;

        RootDocumentParserContext(
            MappingLookup mappingLookup,
            MappingParserContext mappingParserContext,
            SourceToParse source,
            XContentParser parser
        ) throws IOException {
            super(
                mappingLookup,
                mappingParserContext,
                source,
                mappingLookup.getMapping().getRoot(),
                ObjectMapper.Dynamic.getRootDynamic(mappingLookup)
            );
            if (mappingLookup.getMapping().getRoot().subobjects() == ObjectMapper.Subobjects.ENABLED) {
                this.parser = DotExpandingXContentParser.expandDots(parser, this.path);
            } else {
                this.parser = parser;
            }
            this.document = new LuceneDocument();
            this.documents.add(document);
            this.maxAllowedNumNestedDocs = indexSettings().getMappingNestedDocsLimit();
            this.numNestedDocs = 0L;
        }

        @Override
        public Mapper getMapper(String name) {
            Mapper mapper = getMetadataMapper(name);
            if (mapper != null) {
                return mapper;
            }
            return super.getMapper(name);
        }

        @Override
        public ContentPath path() {
            return this.path;
        }

        @Override
        public XContentParser parser() {
            return this.parser;
        }

        @Override
        public LuceneDocument rootDoc() {
            return documents.get(0);
        }

        @Override
        public LuceneDocument doc() {
            return this.document;
        }

        @Override
        protected void addDoc(LuceneDocument doc) {
            numNestedDocs++;
            if (numNestedDocs > maxAllowedNumNestedDocs) {
                throw new DocumentParsingException(
                    parser.getTokenLocation(),
                    "The number of nested documents has exceeded the allowed limit of ["
                        + maxAllowedNumNestedDocs
                        + "]."
                        + " This limit can be set by changing the ["
                        + MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.getKey()
                        + "] index level setting."
                );
            }
            this.documents.add(doc);
        }

        @Override
        public Iterable<LuceneDocument> nonRootDocuments() {
            if (docsReversed) {
                throw new IllegalStateException("documents are already reversed");
            }
            return documents.subList(1, documents.size());
        }

        /**
         * Returns a copy of the provided {@link List} where parent documents appear
         * after their children.
         */
        private List<LuceneDocument> reorderParentAndGetDocs() {
            if (documents.size() > 1 && docsReversed == false) {
                docsReversed = true;
                // We preserve the order of the children while ensuring that parents appear after them.
                List<LuceneDocument> newDocs = new ArrayList<>(documents.size());
                LinkedList<LuceneDocument> parents = new LinkedList<>();
                for (LuceneDocument doc : documents) {
                    while (parents.peek() != doc.getParent()) {
                        newDocs.add(parents.poll());
                    }
                    parents.add(0, doc);
                }
                newDocs.addAll(parents);
                documents.clear();
                documents.addAll(newDocs);
            }
            return documents;
        }
    }
}
