/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
