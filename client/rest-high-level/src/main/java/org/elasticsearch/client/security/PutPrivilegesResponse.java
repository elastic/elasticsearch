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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Response when creating/updating one or more application privileges to the
 * security index.
 */
public final class PutPrivilegesResponse {

    /*
     * Map of application name to a map of privilege name to boolean denoting
     * created or update status.
     */
    private final Map<String, Map<String, Boolean>> applicationPrivilegesCreatedOrUpdated;

    public PutPrivilegesResponse(final Map<String, Map<String, Boolean>> applicationPrivilegesCreatedOrUpdated) {
        this.applicationPrivilegesCreatedOrUpdated = Collections.unmodifiableMap(applicationPrivilegesCreatedOrUpdated);
    }

    /**
     * Get response status for the request to create or update application
     * privileges.
     *
     * @param applicationName application name as specified in the request
     * @param privilegeName privilege name as specified in the request
     * @return {@code true} if the privilege was created, {@code false} if the
     * privilege was updated
     * @throws IllegalArgumentException thrown for unknown application name or
     * privilege name.
     */
    public boolean wasCreated(final String applicationName, final String privilegeName) {
        if (Strings.hasText(applicationName) == false) {
            throw new IllegalArgumentException("application name is required");
        }
        if (Strings.hasText(privilegeName) == false) {
            throw new IllegalArgumentException("privilege name is required");
        }
        if (applicationPrivilegesCreatedOrUpdated.get(applicationName) == null
                || applicationPrivilegesCreatedOrUpdated.get(applicationName).get(privilegeName) == null) {
            throw new IllegalArgumentException("application name or privilege name not found in the response");
        }
        return applicationPrivilegesCreatedOrUpdated.get(applicationName).get(privilegeName);
    }

    @SuppressWarnings("unchecked")
    public static PutPrivilegesResponse fromXContent(final XContentParser parser) throws IOException {
        final Map<String, Map<String, Boolean>> applicationPrivilegesCreatedOrUpdated = new HashMap<>();
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        final Map<String, Object> appNameToPrivStatus = parser.map();
        for (Entry<String, Object> entry : appNameToPrivStatus.entrySet()) {
            if (entry.getValue() instanceof Map) {
                final Map<String, Boolean> privilegeToStatus = applicationPrivilegesCreatedOrUpdated.computeIfAbsent(entry.getKey(),
                        (a) -> new HashMap<>());
                final Map<String, Object> createdOrUpdated = (Map<String, Object>) entry.getValue();
                for (String privilegeName : createdOrUpdated.keySet()) {
                    if (createdOrUpdated.get(privilegeName) instanceof Map) {
                        final Map<String, Object> statusMap = (Map<String, Object>) createdOrUpdated.get(privilegeName);
                        final Object status = statusMap.get("created");
                        if (status instanceof Boolean) {
                            privilegeToStatus.put(privilegeName, (Boolean) status);
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Failed to parse object, unexpected structure");
                        }
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "Failed to parse object, unexpected structure");
                    }
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Failed to parse object, unexpected structure");
            }
        }
        return new PutPrivilegesResponse(applicationPrivilegesCreatedOrUpdated);
    }
}
