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

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Request for retrieving information for API key(s) owned by the authenticated user.
 */
public final class GetMyApiKeyRequest implements Validatable, ToXContentObject {

    private final String id;
    private final String name;

    public GetMyApiKeyRequest(@Nullable String apiKeyId, @Nullable String apiKeyName) {
        this.id = apiKeyId;
        this.name = apiKeyName;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    /**
     * Creates request for given api key id
     * @param apiKeyId api key id
     * @return {@link GetMyApiKeyRequest}
     */
    public static GetMyApiKeyRequest usingApiKeyId(String apiKeyId) {
        return new GetMyApiKeyRequest(apiKeyId, null);
    }

    /**
     * Creates request for given api key name
     * @param apiKeyName api key name
     * @return {@link GetMyApiKeyRequest}
     */
    public static GetMyApiKeyRequest usingApiKeyName(String apiKeyName) {
        return new GetMyApiKeyRequest(null, apiKeyName);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

}
