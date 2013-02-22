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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.UidField;

import java.util.List;

/**
 * The result of parsing a document.
 */
public class ParsedDocument {

    private final UidField uid;

    private final String id;

    private final String type;

    private final String routing;

    private final long timestamp;

    private final long ttl;

    private final List<Document> documents;

    private final Analyzer analyzer;

    private final BytesReference source;

    private boolean mappingsModified;

    private String parent;

    public ParsedDocument(UidField uid, String id, String type, String routing, long timestamp, long ttl, List<Document> documents, Analyzer analyzer, BytesReference source, boolean mappingsModified) {
        this.uid = uid;
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

    public UidField uid() {
        return this.uid;
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

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Document ").append("uid[").append(uid).append("] doc [").append(documents).append("]");
        return sb.toString();
    }
}
