/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public abstract class ParseContext {

    /** Fork of {@link org.apache.lucene.document.Document} with additional functionality. */
    public static class Document implements Iterable<IndexableField> {

        private final Document parent;
        private final String path;
        private final String prefix;
        private final List<IndexableField> fields;
        private Map<Object, IndexableField> keyedFields;

        private Document(String path, Document parent) {
            fields = new ArrayList<>();
            this.path = path;
            this.prefix = path.isEmpty() ? "" : path + ".";
            this.parent = parent;
        }

        public Document() {
            this("", null);
        }

        /**
         * Return the path associated with this document.
         */
        public String getPath() {
            return path;
        }

        /**
         * Return a prefix that all fields in this document should have.
         */
        public String getPrefix() {
            return prefix;
        }

        /**
         * Return the parent document, or null if this is the root document.
         */
        public Document getParent() {
            return parent;
        }

        @Override
        public Iterator<IndexableField> iterator() {
            return fields.iterator();
        }

        public List<IndexableField> getFields() {
            return fields;
        }

        public void addAll(List<? extends IndexableField> fields) {
            this.fields.addAll(fields);
        }

        public void add(IndexableField field) {
            // either a meta fields or starts with the prefix
            assert field.name().startsWith("_") || field.name().startsWith(prefix) : field.name() + " " + prefix;
            fields.add(field);
        }

        /** Add fields so that they can later be fetched using {@link #getByKey(Object)}. */
        public void addWithKey(Object key, IndexableField field) {
            if (keyedFields == null) {
                keyedFields = new HashMap<>();
            } else if (keyedFields.containsKey(key)) {
                throw new IllegalStateException("Only one field can be stored per key");
            }
            keyedFields.put(key, field);
            add(field);
        }

        /** Get back fields that have been previously added with {@link #addWithKey(Object, IndexableField)}. */
        public IndexableField getByKey(Object key) {
            return keyedFields == null ? null : keyedFields.get(key);
        }

        public IndexableField[] getFields(String name) {
            List<IndexableField> f = new ArrayList<>();
            for (IndexableField field : fields) {
                if (field.name().equals(name)) {
                    f.add(field);
                }
            }
            return f.toArray(new IndexableField[f.size()]);
        }

        public IndexableField getField(String name) {
            for (IndexableField field : fields) {
                if (field.name().equals(name)) {
                    return field;
                }
            }
            return null;
        }

        public String get(String name) {
            for (IndexableField f : fields) {
                if (f.name().equals(name) && f.stringValue() != null) {
                    return f.stringValue();
                }
            }
            return null;
        }

        public BytesRef getBinaryValue(String name) {
            for (IndexableField f : fields) {
                if (f.name().equals(name) && f.binaryValue() != null) {
                    return f.binaryValue();
                }
            }
            return null;
        }

    }

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
        public Iterable<Document> nonRootDocuments() {
            return in.nonRootDocuments();
        }

        @Override
        public Mapper.TypeParser.ParserContext parserContext(DateFormatter dateFormatter) {
            return in.parserContext(dateFormatter);
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
        public Document rootDoc() {
            return in.rootDoc();
        }

        @Override
        public Document doc() {
            return in.doc();
        }

        @Override
        protected void addDoc(Document doc) {
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
        public boolean externalValueSet() {
            return in.externalValueSet();
        }

        @Override
        public Object externalValue() {
            return in.externalValue();
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
        public void addDynamicRuntimeField(RuntimeFieldType runtimeField) {
            in.addDynamicRuntimeField(runtimeField);
        }

        @Override
        public List<RuntimeFieldType> getDynamicRuntimeFields() {
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
    }

    public static class InternalParseContext extends ParseContext {
        private final MappingLookup mappingLookup;
        private final Function<DateFormatter, Mapper.TypeParser.ParserContext> parserContextFunction;
        private final ContentPath path = new ContentPath(0);
        private final XContentParser parser;
        private final Document document;
        private final List<Document> documents = new ArrayList<>();
        private final SourceToParse sourceToParse;
        private final long maxAllowedNumNestedDocs;
        private final List<Mapper> dynamicMappers = new ArrayList<>();
        private final Map<String, ObjectMapper> dynamicObjectMappers = new HashMap<>();
        private final List<RuntimeFieldType> dynamicRuntimeFields = new ArrayList<>();
        private final Set<String> ignoredFields = new HashSet<>();
        private Field version;
        private SeqNoFieldMapper.SequenceIDFields seqID;
        private long numNestedDocs;
        private boolean docsReversed = false;

        public InternalParseContext(MappingLookup mappingLookup,
                                    Function<DateFormatter, Mapper.TypeParser.ParserContext> parserContextFunction,
                                    SourceToParse source,
                                    XContentParser parser) {
            this.mappingLookup = mappingLookup;
            this.parserContextFunction = parserContextFunction;
            this.parser = parser;
            this.document = new Document();
            this.documents.add(document);
            this.version = null;
            this.sourceToParse = source;
            this.maxAllowedNumNestedDocs = indexSettings().getMappingNestedDocsLimit();
            this.numNestedDocs = 0L;
        }

        @Override
        public Mapper.TypeParser.ParserContext parserContext(DateFormatter dateFormatter) {
            return parserContextFunction.apply(dateFormatter);
        }

        @Override
        public IndexSettings indexSettings() {
            return this.mappingLookup.getIndexSettings();
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
        public Document rootDoc() {
            return documents.get(0);
        }

        List<Document> docs() {
            return this.documents;
        }

        @Override
        public Document doc() {
            return this.document;
        }

        @Override
        protected void addDoc(Document doc) {
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
            return this.mappingLookup.getMapping().root();
        }

        @Override
        public MappingLookup mappingLookup() {
            return mappingLookup;
        }

        @Override
        public MetadataFieldMapper getMetadataMapper(String mapperName) {
            return mappingLookup.getMapping().getMetadataMapper(mapperName);
        }

        @Override
        public IndexAnalyzers indexAnalyzers() {
            return mappingLookup.getIndexAnalyzers();
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
        public void addDynamicRuntimeField(RuntimeFieldType runtimeField) {
            dynamicRuntimeFields.add(runtimeField);
        }

        @Override
        public List<RuntimeFieldType> getDynamicRuntimeFields() {
            return Collections.unmodifiableList(dynamicRuntimeFields);
        }

        @Override
        public Iterable<Document> nonRootDocuments() {
            if (docsReversed) {
                throw new IllegalStateException("documents are already reversed");
            }
            return documents.subList(1, documents.size());
        }

        void postParse() {
            if (documents.size() > 1) {
                docsReversed = true;
                // We preserve the order of the children while ensuring that parents appear after them.
                List<Document> newDocs = reorderParent(documents);
                documents.clear();
                documents.addAll(newDocs);
            }
        }

        /**
         * Returns a copy of the provided {@link List} where parent documents appear
         * after their children.
         */
        private List<Document> reorderParent(List<Document> docs) {
            List<Document> newDocs = new ArrayList<>(docs.size());
            LinkedList<Document> parents = new LinkedList<>();
            for (Document doc : docs) {
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
    }

    /**
     * Returns an Iterable over all non-root documents. If there are no non-root documents
     * the iterable will return an empty iterator.
     */
    public abstract Iterable<Document> nonRootDocuments();


    /**
     * Add the given {@code field} to the set of ignored fields.
     */
    public abstract void addIgnoredField(String field);

    /**
     * Return the collection of fields that have been ignored so far.
     */
    public abstract Collection<String> getIgnoredFields();

    public abstract Mapper.TypeParser.ParserContext parserContext(DateFormatter dateFormatter);

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
        final Document doc = new Document(fullPath, doc());
        addDoc(doc);
        return switchDoc(doc);
    }

    /**
     * Return a new context that has the provided document as the current document.
     */
    public final ParseContext switchDoc(final Document document) {
        return new FilterParseContext(this) {
            @Override
            public Document doc() {
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

    public boolean isWithinMultiFields() {
        return false;
    }

    public abstract IndexSettings indexSettings();

    public abstract SourceToParse sourceToParse();

    public abstract ContentPath path();

    public abstract XContentParser parser();

    public abstract Document rootDoc();

    public abstract Document doc();

    protected abstract void addDoc(Document doc);

    public abstract RootObjectMapper root();

    public abstract MappingLookup mappingLookup();

    public abstract MetadataFieldMapper getMetadataMapper(String mapperName);

    public abstract IndexAnalyzers indexAnalyzers();

    public abstract Field version();

    public abstract void version(Field version);

    public abstract SeqNoFieldMapper.SequenceIDFields seqID();

    public abstract void seqID(SeqNoFieldMapper.SequenceIDFields seqID);

    /**
     * Return a new context that will have the external value set.
     */
    public final ParseContext createExternalValueContext(final Object externalValue) {
        return new FilterParseContext(this) {
            @Override
            public boolean externalValueSet() {
                return true;
            }
            @Override
            public Object externalValue() {
                return externalValue;
            }
        };
    }

    public boolean externalValueSet() {
        return false;
    }

    public Object externalValue() {
        throw new IllegalStateException("External value is not set");
    }

    /**
     * Try to parse an externalValue if any
     * @param clazz Expected class for external value
     * @return null if no external value has been set or the value
     */
    public final <T> T parseExternalValue(Class<T> clazz) {
        if (externalValueSet() == false || externalValue() == null) {
            return null;
        }

        if (clazz.isInstance(externalValue()) == false) {
            throw new IllegalArgumentException("illegal external value class ["
                    + externalValue().getClass().getName() + "]. Should be " + clazz.getName());
        }
        return clazz.cast(externalValue());
    }

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
    public abstract void addDynamicRuntimeField(RuntimeFieldType runtimeField);

    /**
     * Get dynamic runtime fields created while parsing.
     */
    public abstract List<RuntimeFieldType> getDynamicRuntimeFields();
}
