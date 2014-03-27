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

import com.carrotsearch.hppc.ObjectObjectMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.google.common.collect.Lists;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.all.AllEntries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.object.RootObjectMapper;

import java.util.*;

/**
 *
 */
public class ParseContext {

    /** Fork of {@link org.apache.lucene.document.Document} with additional functionality. */
    public static class Document implements Iterable<IndexableField> {

        private final List<IndexableField> fields;
        private ObjectObjectMap<Object, IndexableField> keyedFields;

        public Document() {
            fields = Lists.newArrayList();
        }

        @Override
        public Iterator<IndexableField> iterator() {
            return fields.iterator();
        }

        public List<IndexableField> getFields() {
            return fields;
        }

        public void add(IndexableField field) {
            fields.add(field);
        }

        /** Add fields so that they can later be fetched using {@link #getByKey(Object)}. */
        public void addWithKey(Object key, IndexableField field) {
            if (keyedFields == null) {
                keyedFields = new ObjectObjectOpenHashMap<>();
            } else if (keyedFields.containsKey(key)) {
                throw new ElasticsearchIllegalStateException("Only one field can be stored per key");
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

    private final DocumentMapper docMapper;

    private final DocumentMapperParser docMapperParser;

    private final ContentPath path;

    private XContentParser parser;

    private Document document;

    private List<Document> documents = Lists.newArrayList();

    private Analyzer analyzer;

    private final String index;

    @Nullable
    private final Settings indexSettings;

    private SourceToParse sourceToParse;
    private BytesReference source;

    private String id;

    private DocumentMapper.ParseListener listener;

    private Field uid, version;

    private StringBuilder stringBuilder = new StringBuilder();

    private Map<String, String> ignoredValues = new HashMap<>();

    private boolean mappingsModified = false;
    private boolean withinNewMapper = false;
    private boolean withinCopyTo = false;
    private boolean withinMultiFields = false;

    private boolean externalValueSet;

    private Object externalValue;

    private AllEntries allEntries = new AllEntries();

    private float docBoost = 1.0f;

    public ParseContext(String index, @Nullable Settings indexSettings, DocumentMapperParser docMapperParser, DocumentMapper docMapper, ContentPath path) {
        this.index = index;
        this.indexSettings = indexSettings;
        this.docMapper = docMapper;
        this.docMapperParser = docMapperParser;
        this.path = path;
    }

    public void reset(XContentParser parser, Document document, SourceToParse source, DocumentMapper.ParseListener listener) {
        this.parser = parser;
        this.document = document;
        if (document != null) {
            this.documents = Lists.newArrayList();
            this.documents.add(document);
        } else {
            this.documents = null;
        }
        this.analyzer = null;
        this.uid = null;
        this.version = null;
        this.id = null;
        this.sourceToParse = source;
        this.source = source == null ? null : sourceToParse.source();
        this.path.reset();
        this.mappingsModified = false;
        this.withinNewMapper = false;
        this.listener = listener == null ? DocumentMapper.ParseListener.EMPTY : listener;
        this.allEntries = new AllEntries();
        this.ignoredValues.clear();
        this.docBoost = 1.0f;
    }

    public boolean flyweight() {
        return sourceToParse.flyweight();
    }

    public DocumentMapperParser docMapperParser() {
        return this.docMapperParser;
    }

    public boolean mappingsModified() {
        return this.mappingsModified;
    }

    public void setMappingsModified() {
        this.mappingsModified = true;
    }

    public void setWithinNewMapper() {
        this.withinNewMapper = true;
    }

    public void clearWithinNewMapper() {
        this.withinNewMapper = false;
    }

    public boolean isWithinNewMapper() {
        return withinNewMapper;
    }

    public void setWithinCopyTo() {
        this.withinCopyTo = true;
    }

    public void clearWithinCopyTo() {
        this.withinCopyTo = false;
    }

    public boolean isWithinCopyTo() {
        return withinCopyTo;
    }

    public void setWithinMultiFields() {
        this.withinMultiFields = true;
    }

    public void clearWithinMultiFields() {
        this.withinMultiFields = false;
    }

    public String index() {
        return this.index;
    }

    @Nullable
    public Settings indexSettings() {
        return this.indexSettings;
    }

    public String type() {
        return sourceToParse.type();
    }

    public SourceToParse sourceToParse() {
        return this.sourceToParse;
    }

    public BytesReference source() {
        return source;
    }

    // only should be used by SourceFieldMapper to update with a compressed source
    public void source(BytesReference source) {
        this.source = source;
    }

    public ContentPath path() {
        return this.path;
    }

    public XContentParser parser() {
        return this.parser;
    }

    public DocumentMapper.ParseListener listener() {
        return this.listener;
    }

    public Document rootDoc() {
        return documents.get(0);
    }

    public List<Document> docs() {
        return this.documents;
    }

    public Document doc() {
        return this.document;
    }

    public void addDoc(Document doc) {
        this.documents.add(doc);
    }

    public Document switchDoc(Document doc) {
        Document prev = this.document;
        this.document = doc;
        return prev;
    }

    public RootObjectMapper root() {
        return docMapper.root();
    }

    public DocumentMapper docMapper() {
        return this.docMapper;
    }

    public AnalysisService analysisService() {
        return docMapperParser.analysisService;
    }

    public String id() {
        return id;
    }

    public void ignoredValue(String indexName, String value) {
        ignoredValues.put(indexName, value);
    }

    public String ignoredValue(String indexName) {
        return ignoredValues.get(indexName);
    }

    /**
     * Really, just the id mapper should set this.
     */
    public void id(String id) {
        this.id = id;
    }

    public Field uid() {
        return this.uid;
    }

    /**
     * Really, just the uid mapper should set this.
     */
    public void uid(Field uid) {
        this.uid = uid;
    }

    public Field version() {
        return this.version;
    }

    public void version(Field version) {
        this.version = version;
    }

    public boolean includeInAll(Boolean includeInAll, FieldMapper mapper) {
        return includeInAll(includeInAll, mapper.fieldType().indexed());
    }

    /**
     * Is all included or not. Will always disable it if {@link org.elasticsearch.index.mapper.internal.AllFieldMapper#enabled()}
     * is <tt>false</tt>. If its enabled, then will return <tt>true</tt> only if the specific flag is <tt>null</tt> or
     * its actual value (so, if not set, defaults to "true") and the field is indexed.
     */
    private boolean includeInAll(Boolean specificIncludeInAll, boolean indexed) {
        if (withinCopyTo) {
            return false;
        }
        if (withinMultiFields) {
            return false;
        }
        if (!docMapper.allFieldMapper().enabled()) {
            return false;
        }
        // not explicitly set
        if (specificIncludeInAll == null) {
            return indexed;
        }
        return specificIncludeInAll;
    }

    public AllEntries allEntries() {
        return this.allEntries;
    }

    public Analyzer analyzer() {
        return this.analyzer;
    }

    public void analyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public void externalValue(Object externalValue) {
        this.externalValueSet = true;
        this.externalValue = externalValue;
    }

    public boolean externalValueSet() {
        return this.externalValueSet;
    }

    public Object externalValue() {
        externalValueSet = false;
        return externalValue;
    }

    /**
     * Try to parse an externalValue if any
     * @param clazz Expected class for external value
     * @return null if no external value has been set or the value
     */
    public <T> T parseExternalValue(Class<T> clazz) {
        if (!externalValueSet() || externalValue() == null) {
            return null;
        }

        if (!clazz.isInstance(externalValue())) {
            throw new ElasticsearchIllegalArgumentException("illegal external value class ["
                    + externalValue().getClass().getName() + "]. Should be " + clazz.getName());
        }
        return (T) externalValue();
    }

    public float docBoost() {
        return this.docBoost;
    }

    public void docBoost(float docBoost) {
        this.docBoost = docBoost;
    }

    /**
     * A string builder that can be used to construct complex names for example.
     * Its better to reuse the.
     */
    public StringBuilder stringBuilder() {
        stringBuilder.setLength(0);
        return this.stringBuilder;
    }

}
