/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class NestedDocument extends WriteField {
    public NestedDocument(String path, Supplier<Map<String, Object>> rootSupplier) {
        super(path, rootSupplier);
    }

    public WriteField field(String path) {
        throw new UnsupportedOperationException("unimplemented");
    }

    public Stream<WriteField> fields(String glob) {
        throw new UnsupportedOperationException("unimplemented");
    }

    public int index() {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public boolean exists() {
        throw new UnsupportedOperationException("unimplemented");
    }
}
