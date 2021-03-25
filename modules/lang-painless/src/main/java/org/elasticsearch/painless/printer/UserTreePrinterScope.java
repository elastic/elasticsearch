/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.printer;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.painless.symbol.ScriptScope;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class UserTreePrinterScope {
    public final XContentBuilder builder;
    public final ScriptScope scriptScope;

    public UserTreePrinterScope(XContentBuilder builder, ScriptScope scriptScope) {
        this.builder = Objects.requireNonNull(builder);
        this.scriptScope = Objects.requireNonNull(scriptScope);
    }

    public void startObject() {
        try {
            builder.startObject();
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void startObject(String name)  {
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

    public void field(String name)  {
        try {
            builder.field(name);
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void field(String name, String value)  {
        try {
            builder.field(name, value);
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void field(String name, Class<?> value)  {
        field(name, value.getName());
    }

    public void field(String name, int value)  {
        try {
            builder.field(name, value);
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void field(String name, boolean value)  {
        try {
            builder.field(name, value);
        } catch (IOException io) {
            throw new IllegalStateException(io);
        }
    }

    public void field(String name, List<String> values)  {
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
}
