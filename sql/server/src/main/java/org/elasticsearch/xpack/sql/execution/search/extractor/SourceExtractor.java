/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.Map;

public class SourceExtractor implements HitExtractor {
    public static final String NAME = "s";
    private final String fieldName;

    public SourceExtractor(String name) {
        this.fieldName = name;
    }

    SourceExtractor(StreamInput in) throws IOException {
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
        Map<String, Object> source = hit.getSourceAsMap();
        // TODO I think this will not work with dotted field names (objects or actual dots in the names)
        // confusingly, I think this is actually handled by InnerHitExtractor. This needs investigating or renaming
        // Tracked by https://github.com/elastic/x-pack-elasticsearch/issues/2874
        return source != null ? source.get(fieldName) : null;
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
        SourceExtractor other = (SourceExtractor) obj;
        return fieldName.equals(other.fieldName);
    }

    @Override
    public int hashCode() {
        return fieldName.hashCode();
    }

    @Override
    public String toString() {
        /* # is sometimes known as the "hash" sign which reminds
         * me of a hash table lookup. */
        return "#" + fieldName;
    }
}