/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;

/**
 * Extracts field values from {@link SearchHit#field(String)}.
 */
class DocValueExtractor implements HitExtractor {
    static final String NAME = "f";
    private final String fieldName;

    DocValueExtractor(String name) {
        this.fieldName = name;
    }

    DocValueExtractor(StreamInput in) throws IOException {
        fieldName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object get(SearchHit hit) {
        // NOCOMMIT we should think about what to do with multi-valued fields.
        DocumentField field = hit.field(fieldName);
        return field != null ? field.getValue() : null;
    }

    @Override
    public String innerHitName() {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        DocValueExtractor other = (DocValueExtractor) obj;
        return fieldName.equals(other.fieldName);
    }

    @Override
    public int hashCode() {
        return fieldName.hashCode();
    }

    @Override
    public String toString() {
        /* % kind of looks like two 0s with a column separator between
         * them so it makes me think of columnar storage which doc
         * values are. */
        return "%" + fieldName;
    }
}