/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class NestedDocument {
    private final WriteField parent;
    private final Map<String, Object> doc;

    public NestedDocument(WriteField parent, Map<String, Object> doc) {
        this.parent = parent;
        this.doc = Objects.requireNonNull(doc);
    }

    /**
     * Get a {@link WriteField} inside this NestedDocument, all operation on the {@link WriteField} are relative to this NestedDocument.
     */
    public WriteField field(String path) {
        return new WriteField(path, this::getDoc);
    }

    /**
     * Stream all {@link WriteField}s in this NestedDocument.
     */
    public Stream<WriteField> fields(String glob) {
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Are there any {@link WriteField}s inside this NestedDocument?
     */
    public boolean isEmpty() {
        return doc.isEmpty();
    }

    /**
     * The number of fields in this NestedDocument.
     */
    public int size() {
        return doc.size();
    }

    /**
     * Has this NestedDocument been removed?
     */
    public boolean exists() {
        return parent.exists();
    }

    /**
     * Remove this NestedDocument
     */
    public void remove() {
        parent.remove(doc);
    }

    /**
     * Return the underlying doc for using this class as a root supplier.
     */
    protected Map<String, Object> getDoc() {
        return doc;
    }
}
