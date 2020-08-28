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

import org.elasticsearch.client.security.support.ApiKey;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Response for get API keys.<br>
 * The result contains information about the API keys that were found.
 */
public final class GetApiKeyResponse {

    private final List<ApiKey> foundApiKeysInfo;

    public GetApiKeyResponse(List<ApiKey> foundApiKeysInfo) {
        Objects.requireNonNull(foundApiKeysInfo, "found_api_keys_info must be provided");
        this.foundApiKeysInfo = Collections.unmodifiableList(foundApiKeysInfo);
    }

    public static GetApiKeyResponse emptyResponse() {
        return new GetApiKeyResponse(Collections.emptyList());
    }

    public List<ApiKey> getApiKeyInfos() {
        return foundApiKeysInfo;
    }

    @Override
    public int hashCode() {
        return Objects.hash(foundApiKeysInfo);
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
        final GetApiKeyResponse other = (GetApiKeyResponse) obj;
        return Objects.equals(foundApiKeysInfo, other.foundApiKeysInfo);
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<GetApiKeyResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_api_key_response",
        args -> { return (args[0] == null) ? GetApiKeyResponse.emptyResponse() : new GetApiKeyResponse((List<ApiKey>) args[0]); }
    );
    static {
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> ApiKey.fromXContent(p), new ParseField("api_keys"));
    }

    public static GetApiKeyResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        return "GetApiKeyResponse [foundApiKeysInfo=" + foundApiKeysInfo + "]";
    }
}
