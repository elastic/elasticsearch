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

import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.carrotsearch.hppc.ObjectObjectMap;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.all.AllEntries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.object.RootObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class ParseContext {

    /** Fork of {@link org.apache.lucene.document.Document} with additional functionality. */
    public static class Document implements Iterable<IndexableField> {

        private final Document parent;
        private final String path;
        private final String prefix;
        private final List<IndexableField> fields;
        private ObjectObjectMap<Object, IndexableField> keyedFields;

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

        public void add(IndexableField field) {
            // either a meta fields or starts with the prefix
            assert field.name().startsWith("_") || field.name().startsWith(prefix) : field.name() + " " + prefix;
            fields.add(field);
        }

        /** Add fields so that they can later be fetched using {@link #getByKey(Object)}. */
        public void addWithKey(Object key, IndexableField field) {
            if (keyedFields == null) {
                keyedFields = new ObjectObjectHashMap<>();
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

        /**
         * Returns an array of values of the field specified as the method parameter.
         * This method returns an empty array when there are no
         * matching fields.  It never returns null.
         * For {@link org.apache.lucene.document.IntField}, {@link org.apache.lucene.document.LongField}, {@link
         * org.apache.lucene.document.FloatField} and {@link org.apache.lucene.document.DoubleField} it returns the string value of the number.
         * If you want the actual numeric field instances back, use {@link #getFields}.
         * @param name the name of the field
         * @return a <code>String[]</code> of field values
         */
        public final String[] getValues(String name) {
            List<String> result = new ArrayList<>();
            for (IndexableField field : fields) {
                if (field.name().equals(name) && field.stringValue() != null) {
                    result.add(field.stringValue());
                }
            }
            return result.toArray(new String[result.size()]);
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
        public boolean flyweight() {
            return in.flyweight();
        }

        @Override
        public DocumentMapperParser docMapperParser() {
            return in.docMapperParser();
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
        public String index() {
            return in.index();
        }

        @Override
        public Settings indexSettings() {
            return in.indexSettings();
        }

        @Override
        public String type() {
            return in.type();
        }

        @Override
        public SourceToParse sourceToParse() {
            return in.sourceToParse();
        }

        @Override
        public BytesReference source() {
            return in.source();
        }

        @Override
        public void source(BytesReference source) {
            in.source(source);
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
        public List<Document> docs() {
            return in.docs();
        }

        @Override
        public Document doc() {
            return in.doc();
        }

        @Override
        public void addDoc(Document doc) {
            in.addDoc(doc);
        }

        @Override
        public RootObjectMapper root() {
            return in.root();
        }

        @Override
        public DocumentMapper docMapper() {
            return in.docMapper();
        }

        @Override
        public AnalysisService analysisService() {
            return in.analysisService();
        }

        @Override
        public MapperService mapperService() {
            return in.mapperService();
        }

        @Override
        public String id() {
            return in.id();
        }

        @Override
        public void ignoredValue(String indexName, String value) {
            in.ignoredValue(indexName, value);
        }

        @Override
        public String ignoredValue(String indexName) {
            return in.ignoredValue(indexName);
        }

        @Override
        public void id(String id) {
            in.id(id);
        }

        @Override
        public Field uid() {
            return in.uid();
        }

        @Override
        public void uid(Field uid) {
            in.uid(uid);
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
        public AllEntries allEntries() {
            return in.allEntries();
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
        public float docBoost() {
            return in.docBoost();
        }

        @Override
        public void docBoost(float docBoost) {
            in.docBoost(docBoost);
        }

        @Override
        public StringBuilder stringBuilder() {
            return in.stringBuilder();
        }

        @Override
        public void addDynamicMappingsUpdate(Mapper update) {
            in.addDynamicMappingsUpdate(update);
        }

        @Override
        public Mapper dynamicMappingsUpdate() {
            return in.dynamicMappingsUpdate();
        }
    }

    public static class InternalParseContext extends ParseContext {

        private final DocumentMapper docMapper;

        private final DocumentMapperParser docMapperParser;

        private final ContentPath path;

        private XContentParser parser;

        private Document document;

        private List<Document> documents = new ArrayList<>();

        @Nullable
        private final Settings indexSettings;

        private SourceToParse sourceToParse;
        private BytesReference source;

        private String id;

        private Field uid, version;

        private StringBuilder stringBuilder = new StringBuilder();

        private Map<String, String> ignoredValues = new HashMap<>();

        private AllEntries allEntries = new AllEntries();

        private float docBoost = 1.0f;

        private Mapper dynamicMappingsUpdate = null;

        public InternalParseContext(@Nullable Settings indexSettings, DocumentMapperParser docMapperParser, DocumentMapper docMapper, ContentPath path) {
            this.indexSettings = indexSettings;
            this.docMapper = docMapper;
            this.docMapperParser = docMapperParser;
            this.path = path;
        }

        public void reset(XContentParser parser, Document document, SourceToParse source) {
            this.parser = parser;
            this.document = document;
            if (document != null) {
                this.documents = new ArrayList<>();
                this.documents.add(document);
            } else {
                this.documents = null;
            }
            this.uid = null;
            this.version = null;
            this.id = null;
            this.sourceToParse = source;
            this.source = source == null ? null : sourceToParse.source();
            this.path.reset();
            this.allEntries = new AllEntries();
            this.ignoredValues.clear();
            this.docBoost = 1.0f;
            this.dynamicMappingsUpdate = null;
        }

        @Override
        public boolean flyweight() {
            return sourceToParse.flyweight();
        }

        @Override
        public DocumentMapperParser docMapperParser() {
            return this.docMapperParser;
        }

        @Override
        public String index() {
            return sourceToParse.index();
        }

        @Override
        @Nullable
        public Settings indexSettings() {
            return this.indexSettings;
        }

        @Override
        public String type() {
            return sourceToParse.type();
        }

        @Override
        public SourceToParse sourceToParse() {
            return this.sourceToParse;
        }

        @Override
        public BytesReference source() {
            return source;
        }

        // only should be used by SourceFieldMapper to update with a compressed source
        @Override
        public void source(BytesReference source) {
            this.source = source;
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

        @Override
        public List<Document> docs() {
            return this.documents;
        }

        @Override
        public Document doc() {
            return this.document;
        }

        @Override
        public void addDoc(Document doc) {
            this.documents.add(doc);
        }

        @Override
        public RootObjectMapper root() {
            return docMapper.root();
        }

        @Override
        public DocumentMapper docMapper() {
            return this.docMapper;
        }

        @Override
        public AnalysisService analysisService() {
            return docMapperParser.analysisService;
        }

        @Override
        public MapperService mapperService() {
            return docMapperParser.mapperService;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public void ignoredValue(String indexName, String value) {
            ignoredValues.put(indexName, value);
        }

        @Override
        public String ignoredValue(String indexName) {
            return ignoredValues.get(indexName);
        }

        /**
         * Really, just the id mapper should set this.
         */
        @Override
        public void id(String id) {
            this.id = id;
        }

        @Override
        public Field uid() {
            return this.uid;
        }

        /**
         * Really, just the uid mapper should set this.
         */
        @Override
        public void uid(Field uid) {
            this.uid = uid;
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
        public AllEntries allEntries() {
            return this.allEntries;
        }

        @Override
        public float docBoost() {
            return this.docBoost;
        }

        @Override
        public void docBoost(float docBoost) {
            this.docBoost = docBoost;
        }

        /**
         * A string builder that can be used to construct complex names for example.
         * Its better to reuse the.
         */
        @Override
        public StringBuilder stringBuilder() {
            stringBuilder.setLength(0);
            return this.stringBuilder;
        }

        @Override
        public void addDynamicMappingsUpdate(Mapper mapper) {
            assert mapper instanceof RootObjectMapper : mapper;
            if (dynamicMappingsUpdate == null) {
                dynamicMappingsUpdate = mapper;
            } else {
                MapperUtils.merge(dynamicMappingsUpdate, mapper);
            }
        }

        @Override
        public Mapper dynamicMappingsUpdate() {
            return dynamicMappingsUpdate;
        }
    }

    public abstract boolean flyweight();

    public abstract DocumentMapperParser docMapperParser();

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

    public abstract String index();

    @Nullable
    public abstract Settings indexSettings();

    public abstract String type();

    public abstract SourceToParse sourceToParse();

    public abstract BytesReference source();

    // only should be used by SourceFieldMapper to update with a compressed source
    public abstract void source(BytesReference source);

    public abstract ContentPath path();

    public abstract XContentParser parser();

    public abstract Document rootDoc();

    public abstract List<Document> docs();

    public abstract Document doc();

    public abstract void addDoc(Document doc);

    public abstract RootObjectMapper root();

    public abstract DocumentMapper docMapper();

    public abstract AnalysisService analysisService();

    public abstract MapperService mapperService();

    public abstract String id();

    public abstract void ignoredValue(String indexName, String value);

    public abstract String ignoredValue(String indexName);

    /**
     * Really, just the id mapper should set this.
     */
    public abstract void id(String id);

    public abstract Field uid();

    /**
     * Really, just the uid mapper should set this.
     */
    public abstract void uid(Field uid);

    public abstract Field version();

    public abstract void version(Field version);

    public final boolean includeInAll(Boolean includeInAll, FieldMapper mapper) {
        return includeInAll(includeInAll, mapper.fieldType().indexOptions() != IndexOptions.NONE);
    }

    /**
     * Is all included or not. Will always disable it if {@link org.elasticsearch.index.mapper.internal.AllFieldMapper#enabled()}
     * is <tt>false</tt>. If its enabled, then will return <tt>true</tt> only if the specific flag is <tt>null</tt> or
     * its actual value (so, if not set, defaults to "true") and the field is indexed.
     */
    private boolean includeInAll(Boolean specificIncludeInAll, boolean indexed) {
        if (isWithinCopyTo()) {
            return false;
        }
        if (isWithinMultiFields()) {
            return false;
        }
        if (!docMapper().allFieldMapper().enabled()) {
            return false;
        }
        // not explicitly set
        if (specificIncludeInAll == null) {
            return indexed;
        }
        return specificIncludeInAll;
    }

    public abstract AllEntries allEntries();

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
        if (!externalValueSet() || externalValue() == null) {
            return null;
        }

        if (!clazz.isInstance(externalValue())) {
            throw new IllegalArgumentException("illegal external value class ["
                    + externalValue().getClass().getName() + "]. Should be " + clazz.getName());
        }
        return clazz.cast(externalValue());
    }

    public abstract float docBoost();

    public abstract void docBoost(float docBoost);

    /**
     * A string builder that can be used to construct complex names for example.
     * Its better to reuse the.
     */
    public abstract StringBuilder stringBuilder();

    /**
     * Add a dynamic update to the root object mapper.
     */
    public abstract void addDynamicMappingsUpdate(Mapper update);

    /**
     * Get dynamic updates to the root object mapper.
     */
    public abstract Mapper dynamicMappingsUpdate();
}
