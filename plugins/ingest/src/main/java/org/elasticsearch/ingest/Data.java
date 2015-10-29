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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents the data and meta data (like id and type) of a single document that is going to be indexed.
 */
public final class Data {

    private final String index;
    private final String type;
    private final String id;
    private final Map<String, Object> document;

    private boolean modified = false;

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

    /**
     * add `value` to path in document. If path does not exist,
     * nested hashmaps will be put in as parent key values until
     * leaf key name in path is reached.
     *
     * @param path The path within the document in dot-notation
     * @param value The value to put in for the path key
     */
    public void addField(String path, Object value) {
        modified = true;

        String[] pathElements = Strings.splitStringToArray(path, '.');

        String writeKey = pathElements[pathElements.length - 1];
        Map<String, Object> inner = document;

        for (int i = 0; i < pathElements.length - 1; i++) {
            if (!inner.containsKey(pathElements[i])) {
                inner.put(pathElements[i], new HashMap<String, Object>());
            }
            inner = (Map<String, Object>) inner.get(pathElements[i]);
        }

        inner.put(writeKey, value);
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

    public boolean isModified() {
        return modified;
    }
}
