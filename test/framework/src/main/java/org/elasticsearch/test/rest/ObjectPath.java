/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Holds an object and allows extraction of specific values from it, given their path
 */
public class ObjectPath {

    private final Object object;

    public static ObjectPath createFromResponse(Response response) throws IOException {
        byte[] bytes = EntityUtils.toByteArray(response.getEntity());
        String contentType = response.getHeader("Content-Type");
        XContentType xContentType = XContentType.fromMediaType(contentType);
        return ObjectPath.createFromXContent(xContentType.xContent(), new BytesArray(bytes));
    }

    public static ObjectPath createFromXContent(XContent xContent, BytesReference input) throws IOException {
        try (
            XContentParser parser = xContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                input.streamInput()
            )
        ) {
            if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                return new ObjectPath(parser.listOrderedMap());
            }
            return new ObjectPath(parser.mapOrdered());
        }
    }

    public ObjectPath(Object object) {
        this.object = object;
    }

    /**
     * A utility method that creates an {@link ObjectPath} via {@link #ObjectPath(Object)} returns
     * the result of calling {@link #evaluate(String)} on it.
     */
    public static <T> T evaluate(Object object, String path) throws IOException {
        return new ObjectPath(object).evaluate(path, Stash.EMPTY);
    }

    /**
     * Returns the object corresponding to the provided path if present, null otherwise
     */
    public <T> T evaluate(String path) throws IOException {
        return evaluate(path, Stash.EMPTY);
    }

    /**
     * Returns the object corresponding to the provided path if present, null otherwise
     */
    @SuppressWarnings("unchecked")
    public <T> T evaluate(String path, Stash stash) throws IOException {
        String[] parts = parsePath(path);
        Object result = this.object;
        for (String part : parts) {
            result = evaluate(part, result, stash);
            if (result == null) {
                return null;
            }
        }
        return (T) result;
    }

    @SuppressWarnings("unchecked")
    private Object evaluate(String key, Object objectToEvaluate, Stash stash) throws IOException {
        if (stash.containsStashedValue(key)) {
            key = stash.getValue(key).toString();
        }

        if (objectToEvaluate instanceof Map) {
            final Map<String, Object> objectAsMap = (Map<String, Object>) objectToEvaluate;
            if ("_arbitrary_key_".equals(key)) {
                if (objectAsMap.isEmpty()) {
                    throw new IllegalArgumentException("requested [" + key + "] but the map was empty");
                }
                if (objectAsMap.containsKey(key)) {
                    throw new IllegalArgumentException("requested meta-key [" + key + "] but the map unexpectedly contains this key");
                }
                return objectAsMap.keySet().iterator().next();
            }
            return objectAsMap.get(key);
        }
        if (objectToEvaluate instanceof List) {
            List<Object> list = (List<Object>) objectToEvaluate;
            try {
                return list.get(Integer.parseInt(key));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("element was a list, but [" + key + "] was not numeric", e);
            } catch (IndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "element was a list with " + list.size() + " elements, but [" + key + "] was out of bounds",
                    e
                );
            }
        }

        throw new IllegalArgumentException(
            "no object found for [" + key + "] within object of class [" + objectToEvaluate.getClass() + "]"
        );
    }

    private String[] parsePath(String path) {
        List<String> list = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean escape = false;
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '\\') {
                escape = true;
                continue;
            }

            if (c == '.') {
                if (escape) {
                    escape = false;
                } else {
                    if (current.length() > 0) {
                        list.add(current.toString());
                        current.setLength(0);
                    }
                    continue;
                }
            }

            current.append(c);
        }

        if (current.length() > 0) {
            list.add(current.toString());
        }

        return list.toArray(new String[0]);
    }

    /**
     * Create a new {@link XContentBuilder} from the xContent object underlying this {@link ObjectPath}.
     * This only works for {@link ObjectPath} instances created from an xContent object, not from nested
     * substructures. We throw an {@link UnsupportedOperationException} in those cases.
     */
    @SuppressWarnings("unchecked")
    public XContentBuilder toXContentBuilder(XContent xContent) throws IOException {
        XContentBuilder builder = XContentBuilder.builder(xContent);
        if (this.object instanceof Map) {
            builder.map((Map<String, Object>) this.object);
        } else {
            throw new UnsupportedOperationException("Only ObjectPath created from a map supported.");
        }
        return builder;
    }
}
