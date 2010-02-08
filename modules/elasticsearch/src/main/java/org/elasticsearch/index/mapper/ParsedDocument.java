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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Document;

/**
 * @author kimchy (Shay Banon)
 */
public class ParsedDocument {

    private final String uid;

    private final String id;

    private final String type;

    private final Document document;

    private final String source;

    public ParsedDocument(String uid, String id, String type, Document document, String source) {
        this.uid = uid;
        this.id = id;
        this.type = type;
        this.document = document;
        this.source = source;
    }

    public String uid() {
        return this.uid;
    }

    public String id() {
        return this.id;
    }

    public String type() {
        return this.type;
    }

    public Document doc() {
        return this.document;
    }

    public String source() {
        return this.source;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Document ").append("uid[").append(uid).append("] doc [").append(document).append("]");
        return sb.toString();
    }
}
