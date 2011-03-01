/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.mapper.xcontent;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.elasticsearch.common.lucene.all.AllEntries;
import org.elasticsearch.common.util.concurrent.NotThreadSafe;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.DocumentMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
@NotThreadSafe
public class ParseContext {

    private final XContentDocumentMapper docMapper;

    private final XContentDocumentMapperParser docMapperParser;

    private final ContentPath path;

    private XContentParser parser;

    private Document document;

    private Analyzer analyzer;

    private String index;

    private String type;

    private byte[] source;

    private String id;

    private boolean flyweight;

    private DocumentMapper.ParseListener listener;

    private String uid;

    private StringBuilder stringBuilder = new StringBuilder();

    private Map<String, String> ignoredValues = new HashMap<String, String>();

    private ParsedIdState parsedIdState;

    private boolean mappersAdded = false;

    private boolean externalValueSet;

    private Object externalValue;

    private AllEntries allEntries = new AllEntries();

    public ParseContext(String index, XContentDocumentMapperParser docMapperParser, XContentDocumentMapper docMapper, ContentPath path) {
        this.index = index;
        this.docMapper = docMapper;
        this.docMapperParser = docMapperParser;
        this.path = path;
    }

    public void reset(XContentParser parser, Document document, String type, byte[] source, boolean flyweight, DocumentMapper.ParseListener listener) {
        this.parser = parser;
        this.document = document;
        this.analyzer = null;
        this.uid = null;
        this.id = null;
        this.type = type;
        this.source = source;
        this.flyweight = flyweight;
        this.path.reset();
        this.parsedIdState = ParsedIdState.NO;
        this.mappersAdded = false;
        this.listener = listener == null ? DocumentMapper.ParseListener.EMPTY : listener;
        this.allEntries = new AllEntries();
        this.ignoredValues.clear();
    }

    public boolean flyweight() {
        return this.flyweight;
    }

    public XContentDocumentMapperParser docMapperParser() {
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

    public String type() {
        return this.type;
    }

    public byte[] source() {
        return this.source;
    }

    // only should be used by SourceFieldMapper to update with a compressed source
    public void source(byte[] source) {
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

    public Document doc() {
        return this.document;
    }

    public RootObjectMapper root() {
        return docMapper.root();
    }

    public XContentDocumentMapper docMapper() {
        return this.docMapper;
    }

    public AnalysisService analysisService() {
        return docMapperParser.analysisService;
    }

    public String id() {
        return id;
    }

    public void parsedId(ParsedIdState parsedIdState) {
        this.parsedIdState = parsedIdState;
    }

    public ParsedIdState parsedIdState() {
        return this.parsedIdState;
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

    /**
     * Is all included or not. Will always disable it if {@link org.elasticsearch.index.mapper.AllFieldMapper#enabled()}
     * is <tt>false</tt>. If its enabled, then will return <tt>true</tt> only if the specific flag is <tt>null</tt> or
     * its actual value (so, if not set, defaults to "true").
     */
    public boolean includeInAll(Boolean specificIncludeInAll) {
        if (!docMapper.allFieldMapper().enabled()) {
            return false;
        }
        return specificIncludeInAll == null || specificIncludeInAll;
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

    public static enum ParsedIdState {
        NO,
        PARSED,
        EXTERNAL
    }
}
