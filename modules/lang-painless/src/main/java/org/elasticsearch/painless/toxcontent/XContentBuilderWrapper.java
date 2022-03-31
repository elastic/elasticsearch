/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.toxcontent;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public class XContentBuilderWrapper {
    public final XContentBuilder builder;

    public XContentBuilderWrapper(XContentBuilder builder) {
        this.builder = Objects.requireNonNull(builder);
    }

    public XContentBuilderWrapper() {
        XContentBuilder jsonBuilder;
        try {
            jsonBuilder = XContentFactory.jsonBuilder();
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
        this.builder = jsonBuilder.prettyPrint();
    }

    public void startObject() {
        try {
            builder.startObject();
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void startObject(String name) {
        try {
            builder.startObject(name);
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void endObject() {
        try {
            builder.endObject();
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void startArray() {
        try {
            builder.startArray();
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void startArray(String name) {
        try {
            builder.startArray(name);
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void endArray() {
        try {
            builder.endArray();
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void field(String name) {
        try {
            builder.field(name);
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void field(String name, Object value) {
        try {
            if (value instanceof Character) {
                builder.field(name, ((Character) value).charValue());
            } else if (value instanceof Pattern) {
                // This does not serialize the flags
                builder.field(name, ((Pattern) value).pattern());
            } else {
                builder.field(name, value);
            }
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void field(String name, String value) {
        try {
            builder.field(name, value);
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void field(String name, Class<?> value) {
        field(name, value.getName());
    }

    public void field(String name, int value) {
        try {
            builder.field(name, value);
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void field(String name, boolean value) {
        try {
            builder.field(name, value);
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void field(String name, List<String> values) {
        try {
            builder.field(name, values);
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void value(String value) {
        try {
            builder.value(value);
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public String toString() {
        try {
            builder.flush();
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
        return builder.getOutputStream().toString();
    }
}
