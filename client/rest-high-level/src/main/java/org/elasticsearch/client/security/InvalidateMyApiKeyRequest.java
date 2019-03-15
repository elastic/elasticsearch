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
 * Request for invalidating API key(s) for the authenticated user so that it can no longer be used.
 */
public final class InvalidateMyApiKeyRequest implements Validatable, ToXContentObject {

    private final String id;
    private final String name;

    public InvalidateMyApiKeyRequest(@Nullable String apiKeyId, @Nullable String apiKeyName) {
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
     * Creates invalidate API key request for given api key id
     * @param apiKeyId api key id
     * @return {@link InvalidateMyApiKeyRequest}
     */
    public static InvalidateMyApiKeyRequest usingApiKeyId(String apiKeyId) {
        return new InvalidateMyApiKeyRequest(apiKeyId, null);
    }

    /**
     * Creates invalidate API key request for given api key name
     * @param apiKeyName api key name
     * @return {@link InvalidateMyApiKeyRequest}
     */
    public static InvalidateMyApiKeyRequest usingApiKeyName(String apiKeyName) {
        return new InvalidateMyApiKeyRequest(null, apiKeyName);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (id != null) {
            builder.field("id", id);
        }
        if (name != null) {
            builder.field("name", name);
        }
        return builder.endObject();
    }
}
