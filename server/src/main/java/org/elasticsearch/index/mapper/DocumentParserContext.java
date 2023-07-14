/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
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

        private Wrapper(ObjectMapper parent, DocumentParserContext in) {
            super(parent, parent.dynamic == null ? in.dynamic : parent.dynamic, in);
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

    private final MappingLookup mappingLookup;
    private final MappingParserContext mappingParserContext;
    private final SourceToParse sourceToParse;
    private final Set<String> ignoredFields;
    private final List<Mapper> dynamicMappers;
    private final Set<String> newFieldsSeen;
    private final Map<String, ObjectMapper> dynamicObjectMappers;
    private final List<RuntimeField> dynamicRuntimeFields;
    private final DocumentDimensions dimensions;
    private final ObjectMapper parent;
    private final ObjectMapper.Dynamic dynamic;
    private String id;
    private Field version;
    private SeqNoFieldMapper.SequenceIDFields seqID;

    private DocumentParserContext(
        MappingLookup mappingLookup,
        MappingParserContext mappingParserContext,
        SourceToParse sourceToParse,
        Set<String> ignoreFields,
        List<Mapper> dynamicMappers,
        Set<String> newFieldsSeen,
        Map<String, ObjectMapper> dynamicObjectMappers,
        List<RuntimeField> dynamicRuntimeFields,
        String id,
        Field version,
        SeqNoFieldMapper.SequenceIDFields seqID,
        DocumentDimensions dimensions,
        ObjectMapper parent,
        ObjectMapper.Dynamic dynamic
    ) {
        this.mappingLookup = mappingLookup;
        this.mappingParserContext = mappingParserContext;
        this.sourceToParse = sourceToParse;
        this.ignoredFields = ignoreFields;
        this.dynamicMappers = dynamicMappers;
        this.newFieldsSeen = newFieldsSeen;
        this.dynamicObjectMappers = dynamicObjectMappers;
        this.dynamicRuntimeFields = dynamicRuntimeFields;
        this.id = id;
        this.version = version;
        this.seqID = seqID;
        this.dimensions = dimensions;
        this.parent = parent;
        this.dynamic = dynamic;
    }

    private DocumentParserContext(ObjectMapper parent, ObjectMapper.Dynamic dynamic, DocumentParserContext in) {
        this(
            in.mappingLookup,
            in.mappingParserContext,
            in.sourceToParse,
            in.ignoredFields,
            in.dynamicMappers,
            in.newFieldsSeen,
            in.dynamicObjectMappers,
            in.dynamicRuntimeFields,
            in.id,
            in.version,
            in.seqID,
            in.dimensions,
            parent,
            dynamic
        );
    }

    protected DocumentParserContext(
        MappingLookup mappingLookup,
        MappingParserContext mappingParserContext,
        SourceToParse source,
        ObjectMapper parent,
        ObjectMapper.Dynamic dynamic
    ) {
        this(
            mappingLookup,
            mappingParserContext,
            source,
            new HashSet<>(),
            new ArrayList<>(),
            new HashSet<>(),
            new HashMap<>(),
            new ArrayList<>(),
            null,
            null,
            null,
            DocumentDimensions.fromIndexSettings(mappingParserContext.getIndexSettings()),
            parent,
            dynamic
        );
    }

    public final IndexSettings indexSettings() {
        return mappingParserContext.getIndexSettings();
    }

    public final IndexAnalyzers indexAnalyzers() {
        return mappingParserContext.getIndexAnalyzers();
    }

    public final RootObjectMapper root() {
        return this.mappingLookup.getMapping().getRoot();
    }

    public final ObjectMapper parent() {
        return parent;
    }

    public final MappingLookup mappingLookup() {
        return mappingLookup;
    }

    public final MetadataFieldMapper getMetadataMapper(String mapperName) {
        return mappingLookup.getMapping().getMetadataMapperByName(mapperName);
    }

    public final MappingParserContext dynamicTemplateParserContext(DateFormatter dateFormatter) {
        return mappingParserContext.createDynamicTemplateContext(dateFormatter);
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

    public Mapper getMapper(String name) {
        return parent.getMapper(name);
    }

    public ObjectMapper.Dynamic dynamic() {
        return dynamic;
    }

    /**
     * Add a new mapper dynamically created while parsing.
     */
    public final void addDynamicMapper(Mapper mapper) {
        // eagerly check object depth limit here to avoid stack overflow errors
        if (mapper instanceof ObjectMapper) {
            MappingLookup.checkObjectDepthLimit(indexSettings().getMappingDepthLimit(), mapper.name());
        }
        // eagerly check field name limit here to avoid OOM errors
        // only check fields that are not already mapped or tracked in order to avoid hitting field limit too early via double-counting
        // note that existing fields can also receive dynamic mapping updates (e.g. constant_keyword to fix the value)
        if (mappingLookup.getMapper(mapper.name()) == null
            && mappingLookup.objectMappers().containsKey(mapper.name()) == false
            && newFieldsSeen.add(mapper.name())) {
            mappingLookup.checkFieldLimit(indexSettings().getMappingTotalFieldsLimit(), newFieldsSeen.size());
        }
        if (mapper instanceof ObjectMapper objectMapper) {
            dynamicObjectMappers.put(objectMapper.name(), objectMapper);
            // dynamic object mappers may have been obtained from applying a dynamic template, in which case their definition may contain
            // sub-fields as well as sub-objects that need to be added to the mappings
            for (Mapper submapper : objectMapper.mappers.values()) {
                // we could potentially skip the step of adding these to the dynamic mappers, because their parent is already added to
                // that list, and what is important is that all of the intermediate objects are added to the dynamic object mappers so that
                // they can be looked up once sub-fields need to be added to them. For simplicity, we treat these like any other object
                addDynamicMapper(submapper);
            }
        }
        // TODO we may want to stop adding object mappers to the dynamic mappers list: most times they will be mapped when parsing their
        // sub-fields (see ObjectMapper.Builder#addDynamic), which causes extra work as the two variants of the same object field
        // will be merged together when creating the final dynamic update. The only cases where object fields need extra treatment are
        // dynamically mapped objects when the incoming document defines no sub-fields in them:
        // 1) by default, they would be empty containers in the mappings, is it then important to map them?
        // 2) they can be the result of applying a dynamic template which may define sub-fields or set dynamic, enabled or subobjects.
        dynamicMappers.add(mapper);
    }

    /**
     * Get dynamic mappers created as a result of parsing an incoming document. Responsible for exposing all the newly created
     * fields that need to be merged into the existing mappings. Used to create the required mapping update at the end of document parsing.
     * Consists of a flat set of {@link Mapper}s that will need to be added to their respective parent {@link ObjectMapper}s in order
     * to become part of the resulting dynamic mapping update.
     */
    public final List<Mapper> getDynamicMappers() {
        return dynamicMappers;
    }

    /**
     * Get a dynamic object mapper by name. Allows consumers to lookup objects that have been dynamically added as a result
     * of parsing an incoming document. Used to find the parent object for new fields that are being dynamically mapped whose parent is
     * also not mapped yet. Such new fields will need to be dynamically added to their parent according to its dynamic behaviour.
     * Holds a flat set of object mappers, meaning that an object field named <code>foo.bar</code> can be looked up directly with its
     * dotted name.
     */
    final ObjectMapper getDynamicObjectMapper(String name) {
        return dynamicObjectMappers.get(name);
    }

    /**
     * Add a new runtime field dynamically created while parsing.
     * We use the same set for both new indexed and new runtime fields,
     * because for dynamic mappings, a new field can be either mapped
     * as runtime or indexed, but never both.
     */
    final void addDynamicRuntimeField(RuntimeField runtimeField) {
        if (newFieldsSeen.add(runtimeField.name())) {
            mappingLookup.checkFieldLimit(indexSettings().getMappingTotalFieldsLimit(), newFieldsSeen.size());
        }
        dynamicRuntimeFields.add(runtimeField);
    }

    /**
     * Get dynamic runtime fields created while parsing. Holds a flat set of {@link RuntimeField}s.
     * Runtime fields get dynamically mapped when {@link org.elasticsearch.index.mapper.ObjectMapper.Dynamic#RUNTIME} is used,
     * or when dynamic templates specify a <code>runtime</code> section.
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
        return mappingLookup.getMapping().getRoot().newBuilder(mappingParserContext.getIndexSettings().getIndexVersionCreated());
    }

    public boolean isWithinCopyTo() {
        return false;
    }

    public final DocumentParserContext createChildContext(ObjectMapper parent) {
        return new Wrapper(parent, this);
    }

    /**
     * Return a new context that will be used within a nested document.
     */
    public final DocumentParserContext createNestedContext(NestedObjectMapper nestedMapper) {
        if (isWithinCopyTo()) {
            // nested context will already have been set up for copy_to fields
            return this;
        }
        final LuceneDocument doc = new LuceneDocument(nestedMapper.fullPath(), doc());
        // We need to add the uid or id to this nested Lucene document too,
        // If we do not do this then when a document gets deleted only the root Lucene document gets deleted and
        // not the nested Lucene documents! Besides the fact that we would have zombie Lucene documents, the ordering of
        // documents inside the Lucene index (document blocks) will be incorrect, as nested documents of different root
        // documents are then aligned with other root documents. This will lead to the nested query, sorting, aggregations
        // and inner hits to fail or yield incorrect results.
        IndexableField idField = doc.getParent().getField(IdFieldMapper.NAME);
        if (idField != null) {
            // We just need to store the id as indexed field, so that IndexWriter#deleteDocuments(term) can then
            // delete it when the root document is deleted too.
            // NOTE: we don't support nested fields in tsdb so it's safe to assume the standard id mapper.
            doc.add(new StringField(IdFieldMapper.NAME, idField.binaryValue(), Field.Store.NO));
        } else {
            throw new IllegalStateException("The root document of a nested document should have an _id field");
        }
        doc.add(NestedPathFieldMapper.field(indexSettings().getIndexVersionCreated(), nestedMapper.nestedTypePath()));
        addDoc(doc);
        return switchDoc(doc);
    }

    /**
     * Return a new context that has the provided document as the current document.
     */
    public final DocumentParserContext switchDoc(final LuceneDocument document) {
        return new Wrapper(this.parent, this) {
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
        ContentPath path = new ContentPath();
        XContentParser parser = DotExpandingXContentParser.expandDots(new CopyToParser(copyToField, parser()), path);
        return new Wrapper(root(), this) {
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
        return new Wrapper(this.parent, this) {
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

    /**
     * Creates a context to build dynamic mappers
     */
    public final MapperBuilderContext createDynamicMapperBuilderContext() {
        String path = ""; // default path for flattened object
        if (parent().subobjects()) {
            path = path().pathAsText(""); // obj with hierarchy, therefore retrieve path
            if (path.endsWith(".")) {
                path = path.substring(0, path.length() - 1);
            }
        }
        return new MapperBuilderContext(path, mappingLookup().isSourceSynthetic());
    }

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
     * @throws DocumentParsingException if the given field has a dynamic template name specified, but no template matches that name.
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
            throw new DocumentParsingException(
                parser().getTokenLocation(),
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
