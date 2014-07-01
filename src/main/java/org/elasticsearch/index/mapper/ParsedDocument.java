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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.mapper.ParseContext.Document;

import java.util.List;

/**
 * The result of parsing a document.
 */
public class ParsedDocument {

    private final Field uid, version;

    private final String id;

    private final String type;

    private final String routing;

    private final long timestamp;

    private final long ttl;

    private final List<Document> documents;

    private final Analyzer analyzer;

    private BytesReference source;

    private boolean mappingsModified;

    private String parent;

    public ParsedDocument(Field uid, Field version, String id, String type, String routing, long timestamp, long ttl, List<Document> documents, Analyzer analyzer, BytesReference source, boolean mappingsModified) {
        this.uid = uid;
        this.version = version;
        this.id = id;
        this.type = type;
        this.routing = routing;
        this.timestamp = timestamp;
        this.ttl = ttl;
        this.documents = documents;
        this.source = source;
        this.analyzer = analyzer;
        this.mappingsModified = mappingsModified;
    }

    public Field uid() {
        return this.uid;
    }

    public Field version() {
        return version;
    }

    public String id() {
        return this.id;
    }

    public String type() {
        return this.type;
    }

    public String routing() {
        return this.routing;
    }

    public long timestamp() {
        return this.timestamp;
    }

    public long ttl() {
        return this.ttl;
    }

    public Document rootDoc() {
        return documents.get(documents.size() - 1);
    }

    public List<Document> docs() {
        return this.documents;
    }

    public Analyzer analyzer() {
        return this.analyzer;
    }

    public BytesReference source() {
        return this.source;
    }

    public void setSource(BytesReference source) {
        this.source = source;
    }

    public ParsedDocument parent(String parent) {
        this.parent = parent;
        return this;
    }

    public String parent() {
        return this.parent;
    }

    /**
     * Has the parsed document caused mappings to be modified?
     */
    public boolean mappingsModified() {
        return mappingsModified;
    }

    /**
     * latches the mapping to be marked as modified.
     */
    public void setMappingsModified() {
        this.mappingsModified = true;
    }

    /**
     * Uses the value of get document or create to automatically set if mapping is
     * modified or not.
     */
    public ParsedDocument setMappingsModified(Tuple<DocumentMapper, Boolean> docMapper) {
        if (docMapper.v2()) {
            setMappingsModified();
        }
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Document ").append("uid[").append(uid).append("] doc [").append(documents).append("]");
        return sb.toString();
    }
}
