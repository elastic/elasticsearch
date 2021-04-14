/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.enrich;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class PutPolicyRequest implements Validatable, ToXContentObject {

    private final String name;
    private final String type;
    private BytesReference query;
    private final List<String> indices;
    private final String matchField;
    private final List<String> enrichFields;
    private final String format;

    public PutPolicyRequest(String name, String type, List<String> indices, String matchField, List<String> enrichFields) {
        this(name, type, indices, matchField, enrichFields, null);
    }

    public PutPolicyRequest(String name, String type, List<String> indices, String matchField, List<String> enrichFields, String format) {
        if (Strings.hasLength(name) == false) {
            throw new IllegalArgumentException("name must be a non-null and non-empty string");
        }
        if (Strings.hasLength(type) == false) {
            throw new IllegalArgumentException("type must be a non-null and non-empty string");
        }
        if (indices == null || indices.isEmpty()) {
            throw new IllegalArgumentException("indices must be specified");
        }
        if (Strings.hasLength(matchField) == false) {
            throw new IllegalArgumentException("matchField must be a non-null and non-empty string");
        }
        if (enrichFields == null || enrichFields.isEmpty()) {
            throw new IllegalArgumentException("enrichFields must be specified");
        }

        this.name = name;
        this.type = type;
        this.indices = indices;
        this.matchField = matchField;
        this.enrichFields = enrichFields;
        this.format = format;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getFormat() {
        return format;
    }

    public BytesReference getQuery() {
        return query;
    }

    // package private for testing only
    void setQuery(BytesReference query) {
        assert query == null || XContentHelper.xContentType(query).canonical() == XContentType.JSON :
                "Only accepts JSON encoded query but received [" + Strings.toString(query) + "]";
        this.query = query;
    }

    public void setQuery(QueryBuilder query) throws IOException {
        setQuery(xContentToBytes(query));
    }

    public List<String> getIndices() {
        return indices;
    }

    public String getMatchField() {
        return matchField;
    }

    public List<String> getEnrichFields() {
        return enrichFields;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject(type);
            {
                builder.field(NamedPolicy.INDICES_FIELD.getPreferredName(), indices);
                if (query != null) {
                    builder.field(NamedPolicy.QUERY_FIELD.getPreferredName(), asMap(query, XContentType.JSON));
                }
                builder.field(NamedPolicy.MATCH_FIELD_FIELD.getPreferredName(), matchField);
                builder.field(NamedPolicy.ENRICH_FIELDS_FIELD.getPreferredName(), enrichFields);

                if (format != null) {
                    builder.field(NamedPolicy.FORMAT_FIELD.getPreferredName(), format);
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutPolicyRequest that = (PutPolicyRequest) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(type, that.type) &&
            Objects.equals(query, that.query) &&
            Objects.equals(indices, that.indices) &&
            Objects.equals(matchField, that.matchField) &&
            Objects.equals(enrichFields, that.enrichFields) &&
            Objects.equals(format, that.format);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, query, indices, matchField, enrichFields, format);
    }

    private static BytesReference xContentToBytes(ToXContentObject object) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            object.toXContent(builder, ToXContentObject.EMPTY_PARAMS);
            return BytesReference.bytes(builder);
        }
    }

    static Map<String, Object> asMap(BytesReference bytesReference, XContentType xContentType) {
        return bytesReference == null ? null : XContentHelper.convertToMap(bytesReference, true, xContentType).v2();
    }
}
