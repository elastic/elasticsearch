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

package org.elasticsearch.ingest;

import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.util.Map;

/**
 * Represents the data and meta data (like id and type) of a single document that is going to be indexed.
 */
public final class Data {

    private final String index;
    private final String type;
    private final String id;
    private final Map<String, Object> document;

    public Data(String index, String type, String id, Map<String, Object> document) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.document = document;
    }

    @SuppressWarnings("unchecked")
    public <T> T getProperty(String path) {
        return (T) XContentMapValues.extractValue(path, document);
    }

    public void addField(String field, String value) {
        document.put(field, value);
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public Map<String, Object> getDocument() {
        return document;
    }
}
