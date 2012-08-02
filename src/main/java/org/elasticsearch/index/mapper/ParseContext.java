/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.all.AllEntries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.object.RootObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ParseContext {

    private final DocumentMapper docMapper;

    private final DocumentMapperParser docMapperParser;

    private final ContentPath path;

    private XContentParser parser;

    private Document document;

    private List<Document> documents = new ArrayList<Document>();

    private Analyzer analyzer;

    private final String index;

    @Nullable
    private final Settings indexSettings;

    private SourceToParse sourceToParse;
    private BytesReference source;

    private String id;

    private DocumentMapper.ParseListener listener;

    private String uid;

    private StringBuilder stringBuilder = new StringBuilder();

    private Map<String, String> ignoredValues = new HashMap<String, String>();

    private boolean mappersAdded = false;

    private boolean externalValueSet;

    private Object externalValue;

    private AllEntries allEntries = new AllEntries();

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
            this.documents = new ArrayList<Document>();
            this.documents.add(document);
        } else {
            this.documents = null;
        }
        this.analyzer = null;
        this.uid = null;
        this.id = null;
        this.sourceToParse = source;
        this.source = source == null ? null : sourceToParse.source();
        this.path.reset();
        this.mappersAdded = false;
        this.listener = listener == null ? DocumentMapper.ParseListener.EMPTY : listener;
        this.allEntries = new AllEntries();
        this.ignoredValues.clear();
    }

    public boolean flyweight() {
        return sourceToParse.flyweight();
    }

    public DocumentMapperParser docMapperParser() {
        return this.docMapperParser;
    }

    public boolean mappersAdded() {
        return this.mappersAdded;
    }

    public void addedMapper() {
        this.mappersAdded = true;
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

    public String uid() {
        return this.uid;
    }

    /**
     * Really, just the uid mapper should set this.
     */
    public void uid(String uid) {
        this.uid = uid;
    }

    public boolean includeInAll(Boolean includeInAll, FieldMapper mapper) {
        return includeInAll(includeInAll, mapper.index());
    }

    /**
     * Is all included or not. Will always disable it if {@link org.elasticsearch.index.mapper.internal.AllFieldMapper#enabled()}
     * is <tt>false</tt>. If its enabled, then will return <tt>true</tt> only if the specific flag is <tt>null</tt> or
     * its actual value (so, if not set, defaults to "true") and the field is indexed.
     */
    private boolean includeInAll(Boolean specificIncludeInAll, Field.Index index) {
        if (!docMapper.allFieldMapper().enabled()) {
            return false;
        }
        // not explicitly set
        if (specificIncludeInAll == null) {
            return index != Field.Index.NO;
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
     * A string builder that can be used to construct complex names for example.
     * Its better to reuse the.
     */
    public StringBuilder stringBuilder() {
        stringBuilder.setLength(0);
        return this.stringBuilder;
    }
}
