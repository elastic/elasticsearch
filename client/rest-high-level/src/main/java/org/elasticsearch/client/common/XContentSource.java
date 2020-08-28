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

package org.elasticsearch.client.common;

import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates the xcontent source
 */
public class XContentSource {

    private final Object data;

    /**
     * Constructs a new XContentSource out of the given parser
     */
    public XContentSource(XContentParser parser) throws IOException {
        this.data = XContentUtils.readValue(parser, parser.nextToken());
    }

    /**
     * @return true if the top level value of the source is a map
     */
    public boolean isMap() {
        return data instanceof Map;
    }

    /**
     * @return The source as a map
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getAsMap() {
        return (Map<String, Object>) data;
    }

    /**
     * @return true if the top level value of the source is a list
     */
    public boolean isList() {
        return data instanceof List;
    }

    /**
     * @return The source as a list
     */
    @SuppressWarnings("unchecked")
    public List<Object> getAsList() {
        return (List<Object>) data;
    }

    /**
     * Extracts a value identified by the given path in the source.
     *
     * @param path a dot notation path to the requested value
     * @return The extracted value or {@code null} if no value is associated with the given path
     */
    @SuppressWarnings("unchecked")
    public <T> T getValue(String path) {
        return (T) ObjectPath.eval(path, data);
    }

}
