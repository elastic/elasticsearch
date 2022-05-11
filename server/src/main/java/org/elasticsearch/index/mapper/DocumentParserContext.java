/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Context used when parsing incoming documents. Holds everything that is needed to parse a document as well as
 * the lucene data structures and mappings to be dynamically created as the outcome of parsing a document.
 */
public abstract class DocumentParserContext {
    /**
     * Wraps a given context while allowing to override some of its behaviour by re-implementing some of the non final methods
     */
    private static class Wrapper extends DocumentParserContext {
        private final DocumentParserContext in;

        private Wrapper(DocumentParserContext in) {
            super(in);
            this.in = in;
        }

        @Override
        public Iterable<LuceneDocument> nonRootDocuments() {
            return in.nonRootDocuments();
        }

        @Override
        public boolean isWithinCopyTo() {
            return in.isWithinCopyTo();
        }

        @Override
        public ContentPath path() {
            return in.path();
        }

        @Override
        public XContentParser parser() {
            return in.parser();
        }

        @Override
        public LuceneDocument rootDoc() {
            return in.rootDoc();
        }

        @Override
        public LuceneDocument doc() {
            return in.doc();
        }

        @Override
        protected void addDoc(LuceneDocument doc) {
            in.addDoc(doc);
        }
    }

    private final IndexSettings indexSettings;
    private final IndexAnalyzers indexAnalyzers;
    private final MappingLookup mappingLookup;
    private final Function<DateFormatter, MappingParserContext> parserContextFunction;
    private final SourceToParse sourceToParse;
    private final Set<String> ignoredFields;
    private final List<Mapper> dynamicMappers;
    private final Set<String> newFieldsSeen;
    private final Map<String, ObjectMapper> dynamicObjectMappers;
    private final List<RuntimeField> dynamicRuntimeFields;
    private final DocumentDimensions dimensions;
    private String id;
    private Field version;
    private SeqNoFieldMapper.SequenceIDFields seqID;

    private DocumentParserContext(DocumentParserContext in) {
        this.mappingLookup = in.mappingLookup;
        this.indexSettings = in.indexSettings;
        this.indexAnalyzers = in.indexAnalyzers;
        this.parserContextFunction = in.parserContextFunction;
        this.sourceToParse = in.sourceToParse;
        this.ignoredFields = in.ignoredFields;
        this.dynamicMappers = in.dynamicMappers;
        this.newFieldsSeen = in.newFieldsSeen;
        this.dynamicObjectMappers = in.dynamicObjectMappers;
        this.dynamicRuntimeFields = in.dynamicRuntimeFields;
        this.id = in.id;
        this.version = in.version;
        this.seqID = in.seqID;
        this.dimensions = in.dimensions;
    }

    protected DocumentParserContext(
        MappingLookup mappingLookup,
        IndexSettings indexSettings,
        IndexAnalyzers indexAnalyzers,
        Function<DateFormatter, MappingParserContext> parserContextFunction,
        SourceToParse source
    ) {
        this.mappingLookup = mappingLookup;
        this.indexSettings = indexSettings;
        this.indexAnalyzers = indexAnalyzers;
        this.parserContextFunction = parserContextFunction;
        this.sourceToParse = source;
        this.ignoredFields = new HashSet<>();
        this.dynamicMappers = new ArrayList<>();
        this.newFieldsSeen = new HashSet<>();
        this.dynamicObjectMappers = new HashMap<>();
        this.dynamicRuntimeFields = new ArrayList<>();
        this.dimensions = indexSettings.getMode().buildDocumentDimensions();
    }

    public final IndexSettings indexSettings() {
        return indexSettings;
    }

    public final IndexAnalyzers indexAnalyzers() {
        return indexAnalyzers;
    }

    public final RootObjectMapper root() {
        return this.mappingLookup.getMapping().getRoot();
    }

    public final MappingLookup mappingLookup() {
        return mappingLookup;
    }

    public final MetadataFieldMapper getMetadataMapper(String mapperName) {
        return mappingLookup.getMapping().getMetadataMapperByName(mapperName);
    }

    public final MappingParserContext dynamicTemplateParserContext(DateFormatter dateFormatter) {
        return parserContextFunction.apply(dateFormatter);
    }

    public final SourceToParse sourceToParse() {
        return this.sourceToParse;
    }

    /**
     * Add the given {@code field} to the set of ignored fields.
     */
    public final void addIgnoredField(String field) {
        ignoredFields.add(field);
    }

    /**
     * Return the collection of fields that have been ignored so far.
     */
    public final Collection<String> getIgnoredFields() {
        return Collections.unmodifiableCollection(ignoredFields);
    }

    /**
     * Add the given {@code field} to the _field_names field
     *
     * Use this if an exists query run against the field cannot use docvalues
     * or norms.
     */
    public final void addToFieldNames(String field) {
        FieldNamesFieldMapper fieldNamesFieldMapper = (FieldNamesFieldMapper) getMetadataMapper(FieldNamesFieldMapper.NAME);
        if (fieldNamesFieldMapper != null) {
            fieldNamesFieldMapper.addFieldNames(this, field);
        }
    }

    public final Field version() {
        return this.version;
    }

    public final void version(Field version) {
        this.version = version;
    }

    public final String id() {
        if (id == null) {
            assert false : "id field mapper has not set the id";
            throw new IllegalStateException("id field mapper has not set the id");
        }
        return id;
    }

    public final void id(String id) {
        this.id = id;
    }

    public final SeqNoFieldMapper.SequenceIDFields seqID() {
        return this.seqID;
    }

    public final void seqID(SeqNoFieldMapper.SequenceIDFields seqID) {
        this.seqID = seqID;
    }

    /**
     * Description on the document being parsed used in error messages. Not
     * called unless there is an error.
     */
    public final String documentDescription() {
        IdFieldMapper idMapper = (IdFieldMapper) getMetadataMapper(IdFieldMapper.NAME);
        return idMapper.documentDescription(this);
    }

    /**
     * Add a new mapper dynamically created while parsing.
     */
    public final void addDynamicMapper(Mapper mapper) {
        // eagerly check field name limit here to avoid OOM errors
        // only check fields that are not already mapped or tracked in order to avoid hitting field limit too early via double-counting
        // note that existing fields can also receive dynamic mapping updates (e.g. constant_keyword to fix the value)
        if (mappingLookup.getMapper(mapper.name()) == null
            && mappingLookup.objectMappers().containsKey(mapper.name()) == false
            && newFieldsSeen.add(mapper.name())) {
            mappingLookup.checkFieldLimit(indexSettings().getMappingTotalFieldsLimit(), newFieldsSeen.size());
        }
        if (mapper instanceof ObjectMapper) {
            dynamicObjectMappers.put(mapper.name(), (ObjectMapper) mapper);
        }
        dynamicMappers.add(mapper);
    }

    /**
     * Get dynamic mappers created while parsing.
     */
    public final List<Mapper> getDynamicMappers() {
        return dynamicMappers;
    }

    public final ObjectMapper getDynamicObjectMapper(String name) {
        return dynamicObjectMappers.get(name);
    }

    /**
     * Add a new runtime field dynamically created while parsing.
     */
    public final void addDynamicRuntimeField(RuntimeField runtimeField) {
        dynamicRuntimeFields.add(runtimeField);
    }

    /**
     * Get dynamic runtime fields created while parsing.
     */
    public final List<RuntimeField> getDynamicRuntimeFields() {
        return Collections.unmodifiableList(dynamicRuntimeFields);
    }

    /**
     * Returns an Iterable over all non-root documents. If there are no non-root documents
     * the iterable will return an empty iterator.
     */
    public abstract Iterable<LuceneDocument> nonRootDocuments();

    /**
     * @return a RootObjectMapper.Builder to be used to construct a dynamic mapping update
     */
    public final RootObjectMapper.Builder updateRoot() {
        return mappingLookup.getMapping().getRoot().newBuilder(indexSettings.getIndexVersionCreated());
    }

    public boolean isWithinCopyTo() {
        return false;
    }

    /**
     * Return a new context that will be used within a nested document.
     */
    public final DocumentParserContext createNestedContext(String fullPath) {
        if (isWithinCopyTo()) {
            // nested context will already have been set up for copy_to fields
            return this;
        }
        final LuceneDocument doc = new LuceneDocument(fullPath, doc());
        addDoc(doc);
        return switchDoc(doc);
    }

    /**
     * Return a new context that has the provided document as the current document.
     */
    public final DocumentParserContext switchDoc(final LuceneDocument document) {
        return new Wrapper(this) {
            @Override
            public LuceneDocument doc() {
                return document;
            }
        };
    }

    /**
     * Return a context for copy_to directives
     * @param copyToField   the name of the field to copy to
     * @param doc           the document to target
     */
    public final DocumentParserContext createCopyToContext(String copyToField, LuceneDocument doc) throws IOException {
        ContentPath path = new ContentPath(0);
        XContentParser parser = DotExpandingXContentParser.expandDots(new CopyToParser(copyToField, parser()));
        return new Wrapper(this) {
            @Override
            public ContentPath path() {
                return path;
            }

            @Override
            public XContentParser parser() {
                return parser;
            }

            @Override
            public boolean isWithinCopyTo() {
                return true;
            }

            @Override
            public LuceneDocument doc() {
                return doc;
            }
        };
    }

    /**
     *  @deprecated we are actively deprecating and removing the ability to pass
     *              complex objects to multifields, so try and avoid using this method
     * Replace the XContentParser used by this context
     * @param parser    the replacement parser
     * @return  a new context with a replaced parser
     */
    @Deprecated
    public final DocumentParserContext switchParser(XContentParser parser) {
        return new Wrapper(this) {
            @Override
            public XContentParser parser() {
                return parser;
            }
        };
    }

    /**
     * The collection of dimensions for this document.
     */
    public DocumentDimensions getDimensions() {
        return dimensions;
    }

    public abstract ContentPath path();

    public abstract XContentParser parser();

    public abstract LuceneDocument rootDoc();

    public abstract LuceneDocument doc();

    protected abstract void addDoc(LuceneDocument doc);

    /**
     * Find a dynamic mapping template for the given field and its matching type
     *
     * @param fieldName the name of the field
     * @param matchType the expecting matchType of the field
     * @return the matching template; otherwise returns null
     * @throws MapperParsingException if the given field has a dynamic template name specified, but no template matches that name.
     */
    public final DynamicTemplate findDynamicTemplate(String fieldName, DynamicTemplate.XContentFieldType matchType) {
        final String pathAsString = path().pathAsText(fieldName);
        final String matchTemplateName = sourceToParse().dynamicTemplates().get(pathAsString);
        for (DynamicTemplate template : root().dynamicTemplates()) {
            if (template.match(matchTemplateName, pathAsString, fieldName, matchType)) {
                return template;
            }
        }
        if (matchTemplateName != null) {
            throw new MapperParsingException(
                "Can't find dynamic template for dynamic template name [" + matchTemplateName + "] of field [" + pathAsString + "]"
            );
        }
        return null;
    }

    // XContentParser that wraps an existing parser positioned on a value,
    // and a field name, and returns a stream that looks like { 'field' : 'value' }
    private static class CopyToParser extends FilterXContentParserWrapper {

        enum State {
            FIELD,
            VALUE
        }

        private State state = State.FIELD;
        private final String field;

        CopyToParser(String fieldName, XContentParser in) {
            super(in);
            this.field = fieldName;
            assert in.currentToken().isValue() || in.currentToken() == Token.VALUE_NULL;
        }

        @Override
        public Token nextToken() throws IOException {
            if (state == State.FIELD) {
                state = State.VALUE;
                return delegate().currentToken();
            }
            return Token.END_OBJECT;
        }

        @Override
        public Token currentToken() {
            if (state == State.FIELD) {
                return Token.FIELD_NAME;
            }
            return delegate().currentToken();
        }

        @Override
        public String currentName() throws IOException {
            return field;
        }
    }
}
