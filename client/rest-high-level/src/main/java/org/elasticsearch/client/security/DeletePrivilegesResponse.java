/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Response for application privileges deletion
 */
public final class DeletePrivilegesResponse {

    private final String application;
    private final List<String> privileges;

    DeletePrivilegesResponse(String application, List<String> privileges) {
        this.application = Objects.requireNonNull(application, "application is required");
        this.privileges = Objects.requireNonNull(privileges, "privileges are required");
    }

    public String getApplication() {
        return application;
    }

    /**
     * Indicates if the given privilege was successfully found and deleted from the list of application privileges.
     *
     * @param privilege the privilege
     * @return true if the privilege was found and deleted, false otherwise.
     */
    public boolean isFound(final String privilege) {
        return privileges.contains(privilege);
    }

    public static DeletePrivilegesResponse fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        final String application = parser.currentName();
        final List<String> foundAndDeletedPrivileges = new ArrayList<>();
        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String privilege = parser.currentName();
                    token = parser.nextToken();
                    if (token == XContentParser.Token.START_OBJECT) {
                        String currentFieldName = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                                if ("found".equals(currentFieldName) && parser.booleanValue()) {
                                    foundAndDeletedPrivileges.add(privilege);
                                }
                            }
                        }
                    }
                }
            }
        }
        return new DeletePrivilegesResponse(application, foundAndDeletedPrivileges);
    }
}
