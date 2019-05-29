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

package org.elasticsearch.client.security;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public final class InvalidateApiKeyResponse {

    private final List<String> invalidatedApiKeys;
    private final List<String> previouslyInvalidatedApiKeys;
    private final List<ElasticsearchException> errors;

    /**
     * Constructor for API keys invalidation response
     * @param invalidatedApiKeys list of invalidated API key ids
     * @param previouslyInvalidatedApiKeys list of previously invalidated API key ids
     * @param errors list of encountered errors while invalidating API keys
     */
    public InvalidateApiKeyResponse(List<String> invalidatedApiKeys, List<String> previouslyInvalidatedApiKeys,
                                    @Nullable List<ElasticsearchException> errors) {
        this.invalidatedApiKeys = Objects.requireNonNull(invalidatedApiKeys, "invalidated_api_keys must be provided");
        this.previouslyInvalidatedApiKeys = Objects.requireNonNull(previouslyInvalidatedApiKeys,
                "previously_invalidated_api_keys must be provided");
        if (null != errors) {
            this.errors = errors;
        } else {
            this.errors = Collections.emptyList();
        }
    }

    public static InvalidateApiKeyResponse emptyResponse() {
        return new InvalidateApiKeyResponse(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    public List<String> getInvalidatedApiKeys() {
        return invalidatedApiKeys;
    }

    public List<String> getPreviouslyInvalidatedApiKeys() {
        return previouslyInvalidatedApiKeys;
    }

    public List<ElasticsearchException> getErrors() {
        return errors;
    }

    @SuppressWarnings("unchecked")
    static ConstructingObjectParser<InvalidateApiKeyResponse, Void> PARSER = new ConstructingObjectParser<>("invalidate_api_key_response",
            args -> {
                return new InvalidateApiKeyResponse((List<String>) args[0], (List<String>) args[1], (List<ElasticsearchException>) args[3]);
            });
    static {
        PARSER.declareStringArray(constructorArg(), new ParseField("invalidated_api_keys"));
        PARSER.declareStringArray(constructorArg(), new ParseField("previously_invalidated_api_keys"));
        // error count is parsed but ignored as we have list of errors
        PARSER.declareInt(constructorArg(), new ParseField("error_count"));
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> ElasticsearchException.fromXContent(p),
                new ParseField("error_details"));
    }

    public static InvalidateApiKeyResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public int hashCode() {
        return Objects.hash(invalidatedApiKeys, previouslyInvalidatedApiKeys, errors);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        InvalidateApiKeyResponse other = (InvalidateApiKeyResponse) obj;
        return Objects.equals(invalidatedApiKeys, other.invalidatedApiKeys)
                && Objects.equals(previouslyInvalidatedApiKeys, other.previouslyInvalidatedApiKeys)
                && Objects.equals(errors, other.errors);
    }

    @Override
    public String toString() {
        return "ApiKeysInvalidationResult [invalidatedApiKeys=" + invalidatedApiKeys + ", previouslyInvalidatedApiKeys="
                + previouslyInvalidatedApiKeys + ", errors=" + errors + "]";
    }
}
