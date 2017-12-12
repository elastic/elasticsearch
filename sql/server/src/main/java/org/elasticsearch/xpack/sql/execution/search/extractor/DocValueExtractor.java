/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.joda.time.ReadableInstant;

import java.io.IOException;

/**
 * Extracts field values from {@link SearchHit#field(String)}.
 */
public class DocValueExtractor implements HitExtractor {
    static final String NAME = "f";
    private final String fieldName;

    public DocValueExtractor(String name) {
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
        // TODO we should think about what to do with multi-valued fields.
        // Tracked by https://github.com/elastic/x-pack-elasticsearch/issues/2874
        DocumentField field = hit.field(fieldName);
        if (field != null) {
            Object value = field.getValue();
            if (value != null && value instanceof ReadableInstant) {
                return ((ReadableInstant) value).getMillis();
            } else {
                return value;
            }
        } else {
            return null;
        }
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