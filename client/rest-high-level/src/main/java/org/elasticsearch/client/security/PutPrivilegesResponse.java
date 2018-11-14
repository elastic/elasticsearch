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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Response when creating/updating one or more application privileges to the
 * security index.
 */
public final class PutPrivilegesResponse {
    public enum Status {
        CREATED, UPDATED, UNKNOWN
    }

    private final Map<String, Map<String, Status>> applicationPrivilegesCreatedOrUpdated;

    public PutPrivilegesResponse(final Map<String, Map<String, Status>> applicationPrivilegesCreatedOrUpdated) {
        this.applicationPrivilegesCreatedOrUpdated = Collections.unmodifiableMap(applicationPrivilegesCreatedOrUpdated);
    }

    /**
     * Get response status for the request to create or update application
     * privileges.
     *
     * @param applicationName application name as specified in the request
     * @param privilegeName privilege name as specified in the request
     * @return {@link Status#CREATED} if the privilege was created,
     * {@link Status#UPDATED} if the privilege was updated and for unknown
     * application name / privilege name the status is {@link Status#UNKNOWN}
     */
    public Status status(final String applicationName, final String privilegeName) {
        if (Strings.hasText(applicationName) == false) {
            throw new IllegalArgumentException("application name is required");
        }
        if (Strings.hasText(privilegeName) == false) {
            throw new IllegalArgumentException("privilege name is required");
        }
        return applicationPrivilegesCreatedOrUpdated.getOrDefault(applicationName, Collections.emptyMap())
                .getOrDefault(privilegeName, Status.UNKNOWN);
    }

    public static PutPrivilegesResponse fromXContent(final XContentParser parser) throws IOException {
        final Map<String, Map<String, Status>> applicationPrivilegesCreatedOrUpdated = new HashMap<>();
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
            final String application = parser.currentName();
            final Map<String, Status> privilegeToStatus = applicationPrivilegesCreatedOrUpdated.computeIfAbsent(application,
                    (a) -> new HashMap<>());

            token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
                final String privilege = parser.currentName();
                token = parser.nextToken();
                ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        if ("created".equals(currentFieldName)) {
                            privilegeToStatus.put(privilege, (parser.booleanValue()) ? Status.CREATED : Status.UPDATED);
                        }
                    }
                }
            }
        }
        return new PutPrivilegesResponse(applicationPrivilegesCreatedOrUpdated);
    }
}
