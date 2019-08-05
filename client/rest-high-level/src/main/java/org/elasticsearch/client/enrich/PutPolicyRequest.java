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
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.ParseField;
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
import java.util.Optional;

public class PutPolicyRequest implements Validatable, ToXContentObject {

    static final ParseField TYPE_FIELD = new ParseField("type");
    static final ParseField QUERY_FIELD = new ParseField("query");
    static final ParseField INDICES_FIELD = new ParseField("indices");
    static final ParseField ENRICH_KEY_FIELD = new ParseField("enrich_key");
    static final ParseField ENRICH_VALUES_FIELD = new ParseField("enrich_values");

    private String name;
    private String type;
    private BytesReference query;
    private List<String> indices;
    private String enrichKey;
    private List<String> enrichValues;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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

    public void setIndices(List<String> indices) {
        this.indices = indices;
    }

    public String getEnrichKey() {
        return enrichKey;
    }

    public void setEnrichKey(String enrichKey) {
        this.enrichKey = enrichKey;
    }

    public List<String> getEnrichValues() {
        return enrichValues;
    }

    public void setEnrichValues(List<String> enrichValues) {
        this.enrichValues = enrichValues;
    }

    @Override
    public Optional<ValidationException> validate() {
        final ValidationException validationException = new ValidationException();
        if (Strings.hasLength(name) == false) {
            validationException.addValidationError("name must be a non-null and non-empty string");
        }
        if (Strings.hasLength(type) == false) {
            validationException.addValidationError("type must be a non-null and non-empty string");
        }
        if (indices == null || indices.isEmpty()) {
            validationException.addValidationError("indices must be specified");
        }
        if (Strings.hasLength(enrichKey) == false) {
            validationException.addValidationError("enrichKey must be a non-null and non-empty string");
        }
        if (enrichValues == null || enrichValues.isEmpty()) {
            validationException.addValidationError("enrichValues must be specified");
        }

        if (validationException.validationErrors().isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(validationException);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE_FIELD.getPreferredName(), type);
        builder.field(INDICES_FIELD.getPreferredName(), indices);
        if (query != null) {
            builder.field(QUERY_FIELD.getPreferredName(), asMap(query, builder.contentType()));
        }
        builder.field(ENRICH_KEY_FIELD.getPreferredName(), enrichKey);
        builder.field(ENRICH_VALUES_FIELD.getPreferredName(), enrichValues);
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
            Objects.equals(enrichKey, that.enrichKey) &&
            Objects.equals(enrichValues, that.enrichValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, query, indices, enrichKey, enrichValues);
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
