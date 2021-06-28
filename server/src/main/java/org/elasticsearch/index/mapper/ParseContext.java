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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public abstract class ParseContext {

    private static class FilterParseContext extends ParseContext {

        private final ParseContext in;

        private FilterParseContext(ParseContext in) {
            this.in = in;
        }

        @Override
        public ObjectMapper getObjectMapper(String name) {
            return in.getObjectMapper(name);
        }

        @Override
        public Iterable<LuceneDocument> nonRootDocuments() {
            return in.nonRootDocuments();
        }

        @Override
        public MappingParserContext dynamicTemplateParserContext(DateFormatter dateFormatter) {
            return in.dynamicTemplateParserContext(dateFormatter);
        }

        @Override
        public boolean isWithinCopyTo() {
            return in.isWithinCopyTo();
        }

        @Override
        public boolean isWithinMultiFields() {
            return in.isWithinMultiFields();
        }

        @Override
        public IndexSettings indexSettings() {
            return in.indexSettings();
        }

        @Override
        public SourceToParse sourceToParse() {
            return in.sourceToParse();
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

        @Override
        public RootObjectMapper root() {
            return in.root();
        }

        @Override
        public MappingLookup mappingLookup() {
            return in.mappingLookup();
        }

        @Override
        public MetadataFieldMapper getMetadataMapper(String mapperName) {
            return in.getMetadataMapper(mapperName);
        }

        @Override
        public IndexAnalyzers indexAnalyzers() {
            return in.indexAnalyzers();
        }

        @Override
        public Field version() {
            return in.version();
        }

        @Override
        public void version(Field version) {
            in.version(version);
        }

        @Override
        public SeqNoFieldMapper.SequenceIDFields seqID() {
            return in.seqID();
        }

        @Override
        public void seqID(SeqNoFieldMapper.SequenceIDFields seqID) {
            in.seqID(seqID);
        }

        @Override
        public void addDynamicMapper(Mapper update) {
            in.addDynamicMapper(update);
        }

        @Override
        public List<Mapper> getDynamicMappers() {
            return in.getDynamicMappers();
        }

        @Override
        public void addDynamicRuntimeField(RuntimeField runtimeField) {
            in.addDynamicRuntimeField(runtimeField);
        }

        @Override
        public List<RuntimeField> getDynamicRuntimeFields() {
            return in.getDynamicRuntimeFields();
        }

        @Override
        public void addIgnoredField(String field) {
            in.addIgnoredField(field);
        }

        @Override
        public Collection<String> getIgnoredFields() {
            return in.getIgnoredFields();
        }

        @Override
        public void addToFieldNames(String field) {
            in.addToFieldNames(field);
        }

        @Override
        public Collection<String> getFieldNames() {
            return in.getFieldNames();
        }
    }

    public static class InternalParseContext extends ParseContext {
        private final MappingLookup mappingLookup;
        private final IndexSettings indexSettings;
        private final IndexAnalyzers indexAnalyzers;
        private final Function<DateFormatter, MappingParserContext> parserContextFunction;
        private final ContentPath path = new ContentPath(0);
        private final XContentParser parser;
        private final LuceneDocument document;
        private final List<LuceneDocument> documents = new ArrayList<>();
        private final SourceToParse sourceToParse;
        private final long maxAllowedNumNestedDocs;
        private final List<Mapper> dynamicMappers = new ArrayList<>();
        private final Set<String> newFieldsSeen = new HashSet<>();
        private final Map<String, ObjectMapper> dynamicObjectMappers = new HashMap<>();
        private final List<RuntimeField> dynamicRuntimeFields = new ArrayList<>();
        private final Set<String> ignoredFields = new HashSet<>();
        private final Set<String> fieldNameFields = new HashSet<>();
        private Field version;
        private SeqNoFieldMapper.SequenceIDFields seqID;
        private long numNestedDocs;
        private boolean docsReversed = false;

        public InternalParseContext(MappingLookup mappingLookup,
                                    IndexSettings indexSettings,
                                    IndexAnalyzers indexAnalyzers,
                                    Function<DateFormatter, MappingParserContext> parserContext,
                                    SourceToParse source,
                                    XContentParser parser) {
            this.mappingLookup = mappingLookup;
            this.indexSettings = indexSettings;
            this.indexAnalyzers = indexAnalyzers;
            this.parserContextFunction = parserContext;
            this.parser = parser;
            this.document = new LuceneDocument();
            this.documents.add(document);
            this.version = null;
            this.sourceToParse = source;
            this.maxAllowedNumNestedDocs = indexSettings().getMappingNestedDocsLimit();
            this.numNestedDocs = 0L;
        }

        @Override
        public MappingParserContext dynamicTemplateParserContext(DateFormatter dateFormatter) {
            return parserContextFunction.apply(dateFormatter);
        }

        @Override
        public IndexSettings indexSettings() {
            return this.indexSettings;
        }

        @Override
        public SourceToParse sourceToParse() {
            return this.sourceToParse;
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

        List<LuceneDocument> docs() {
            return this.documents;
        }

        @Override
        public LuceneDocument doc() {
            return this.document;
        }

        @Override
        protected void addDoc(LuceneDocument doc) {
            numNestedDocs ++;
            if (numNestedDocs > maxAllowedNumNestedDocs) {
                throw new MapperParsingException(
                    "The number of nested documents has exceeded the allowed limit of [" + maxAllowedNumNestedDocs + "]."
                        + " This limit can be set by changing the [" + MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.getKey()
                        + "] index level setting.");
            }
            this.documents.add(doc);
        }

        @Override
        public RootObjectMapper root() {
            return this.mappingLookup.getMapping().getRoot();
        }

        @Override
        public MappingLookup mappingLookup() {
            return mappingLookup;
        }

        @Override
        public MetadataFieldMapper getMetadataMapper(String mapperName) {
            return mappingLookup.getMapping().getMetadataMapperByName(mapperName);
        }

        @Override
        public IndexAnalyzers indexAnalyzers() {
            return this.indexAnalyzers;
        }

        @Override
        public Field version() {
            return this.version;
        }

        @Override
        public void version(Field version) {
            this.version = version;
        }

        @Override
        public SeqNoFieldMapper.SequenceIDFields seqID() {
            return this.seqID;
        }

        @Override
        public void seqID(SeqNoFieldMapper.SequenceIDFields seqID) {
            this.seqID = seqID;
        }

        @Override
        public void addDynamicMapper(Mapper mapper) {
            // eagerly check field name limit here to avoid OOM errors
            // only check fields that are not already mapped or tracked in order to avoid hitting field limit too early via double-counting
            // note that existing fields can also receive dynamic mapping updates (e.g. constant_keyword to fix the value)
            if (mappingLookup.getMapper(mapper.name()) == null &&
                mappingLookup.objectMappers().containsKey(mapper.name()) == false &&
                newFieldsSeen.add(mapper.name())) {
                mappingLookup.checkFieldLimit(indexSettings.getMappingTotalFieldsLimit(), newFieldsSeen.size());
            }
            if (mapper instanceof ObjectMapper) {
                dynamicObjectMappers.put(mapper.name(), (ObjectMapper)mapper);
            }
            dynamicMappers.add(mapper);
        }

        @Override
        public List<Mapper> getDynamicMappers() {
            return dynamicMappers;
        }

        @Override
        public ObjectMapper getObjectMapper(String name) {
            return dynamicObjectMappers.get(name);
        }

        @Override
        public void addDynamicRuntimeField(RuntimeField runtimeField) {
            dynamicRuntimeFields.add(runtimeField);
        }

        @Override
        public List<RuntimeField> getDynamicRuntimeFields() {
            return Collections.unmodifiableList(dynamicRuntimeFields);
        }

        @Override
        public Iterable<LuceneDocument> nonRootDocuments() {
            if (docsReversed) {
                throw new IllegalStateException("documents are already reversed");
            }
            return documents.subList(1, documents.size());
        }

        void postParse() {
            if (documents.size() > 1) {
                docsReversed = true;
                // We preserve the order of the children while ensuring that parents appear after them.
                List<LuceneDocument> newDocs = reorderParent(documents);
                documents.clear();
                documents.addAll(newDocs);
            }
        }

        /**
         * Returns a copy of the provided {@link List} where parent documents appear
         * after their children.
         */
        private List<LuceneDocument> reorderParent(List<LuceneDocument> docs) {
            List<LuceneDocument> newDocs = new ArrayList<>(docs.size());
            LinkedList<LuceneDocument> parents = new LinkedList<>();
            for (LuceneDocument doc : docs) {
                while (parents.peek() != doc.getParent()){
                    newDocs.add(parents.poll());
                }
                parents.add(0, doc);
            }
            newDocs.addAll(parents);
            return newDocs;
        }

        @Override
        public void addIgnoredField(String field) {
            ignoredFields.add(field);
        }

        @Override
        public Collection<String> getIgnoredFields() {
            return Collections.unmodifiableCollection(ignoredFields);
        }

        @Override
        public void addToFieldNames(String field) {
            fieldNameFields.add(field);
        }

        @Override
        public Collection<String> getFieldNames() {
            return Collections.unmodifiableCollection(fieldNameFields);
        }
    }

    /**
     * Returns an Iterable over all non-root documents. If there are no non-root documents
     * the iterable will return an empty iterator.
     */
    public abstract Iterable<LuceneDocument> nonRootDocuments();


    /**
     * Add the given {@code field} to the set of ignored fields.
     */
    public abstract void addIgnoredField(String field);

    /**
     * Return the collection of fields that have been ignored so far.
     */
    public abstract Collection<String> getIgnoredFields();

    /**
     * Add the given {@code field} to the _field_names field
     *
     * Use this if an exists query run against the field cannot use docvalues
     * or norms.
     */
    public abstract void addToFieldNames(String field);

    /**
     * Return the collection of fields to be added to the _field_names field
     */
    public abstract Collection<String> getFieldNames();

    public abstract MappingParserContext dynamicTemplateParserContext(DateFormatter dateFormatter);

    /**
     * Return a new context that will be within a copy-to operation.
     */
    public final ParseContext createCopyToContext() {
        return new FilterParseContext(this) {
            @Override
            public boolean isWithinCopyTo() {
                return true;
            }
        };
    }

    public boolean isWithinCopyTo() {
        return false;
    }

    /**
     * Return a new context that will be within multi-fields.
     */
    public final ParseContext createMultiFieldContext() {
        return new FilterParseContext(this) {
            @Override
            public boolean isWithinMultiFields() {
                return true;
            }
        };
    }

    /**
     * Return a new context that will be used within a nested document.
     */
    public final ParseContext createNestedContext(String fullPath) {
        final LuceneDocument doc = new LuceneDocument(fullPath, doc());
        addDoc(doc);
        return switchDoc(doc);
    }

    /**
     * Return a new context that has the provided document as the current document.
     */
    public final ParseContext switchDoc(final LuceneDocument document) {
        return new FilterParseContext(this) {
            @Override
            public LuceneDocument doc() {
                return document;
            }
        };
    }

    /**
     * Return a new context that will have the provided path.
     */
    public final ParseContext overridePath(final ContentPath path) {
        return new FilterParseContext(this) {
            @Override
            public ContentPath path() {
                return path;
            }
        };
    }

    /**
     * @deprecated we are actively deprecating and removing the ability to pass
     *             complex objects to multifields, so try and avoid using this method
     */
    @Deprecated
    public final ParseContext switchParser(XContentParser parser) {
        return new FilterParseContext(this) {
            @Override
            public XContentParser parser() {
                return parser;
            }
        };
    }

    public boolean isWithinMultiFields() {
        return false;
    }

    public abstract IndexSettings indexSettings();

    public abstract SourceToParse sourceToParse();

    public abstract ContentPath path();

    public abstract XContentParser parser();

    public abstract LuceneDocument rootDoc();

    public abstract LuceneDocument doc();

    protected abstract void addDoc(LuceneDocument doc);

    public abstract RootObjectMapper root();

    public abstract MappingLookup mappingLookup();

    public abstract MetadataFieldMapper getMetadataMapper(String mapperName);

    public abstract IndexAnalyzers indexAnalyzers();

    public abstract Field version();

    public abstract void version(Field version);

    public abstract SeqNoFieldMapper.SequenceIDFields seqID();

    public abstract void seqID(SeqNoFieldMapper.SequenceIDFields seqID);

    /**
     * Add a new mapper dynamically created while parsing.
     */
    public abstract void addDynamicMapper(Mapper update);

    public abstract ObjectMapper getObjectMapper(String name);

    /**
     * Get dynamic mappers created while parsing.
     */
    public abstract List<Mapper> getDynamicMappers();

    /**
     * Add a new runtime field dynamically created while parsing.
     */
    public abstract void addDynamicRuntimeField(RuntimeField runtimeField);

    /**
     * Get dynamic runtime fields created while parsing.
     */
    public abstract List<RuntimeField> getDynamicRuntimeFields();

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
                "Can't find dynamic template for dynamic template name [" + matchTemplateName + "] of field [" + pathAsString + "]");
        }
        return null;
    }
}
