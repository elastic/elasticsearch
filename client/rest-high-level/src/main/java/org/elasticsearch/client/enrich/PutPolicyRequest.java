/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

    public PutPolicyRequest(String name, String type, List<String> indices, String matchField, List<String> enrichFields) {
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
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public BytesReference getQuery() {
        return query;
    }

    public void setQuery(BytesReference query) {
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
                    builder.field(NamedPolicy.QUERY_FIELD.getPreferredName(), asMap(query, builder.contentType()));
                }
                builder.field(NamedPolicy.MATCH_FIELD_FIELD.getPreferredName(), matchField);
                builder.field(NamedPolicy.ENRICH_FIELDS_FIELD.getPreferredName(), enrichFields);
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
            Objects.equals(enrichFields, that.enrichFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, query, indices, matchField, enrichFields);
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
