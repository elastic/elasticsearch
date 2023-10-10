/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Request builder for {@link PutPrivilegesRequest}
 */
public final class PutPrivilegesRequestBuilder extends ActionRequestBuilder<PutPrivilegesRequest, PutPrivilegesResponse>
    implements
        WriteRequestBuilder<PutPrivilegesRequestBuilder> {

    public PutPrivilegesRequestBuilder(ElasticsearchClient client) {
        super(client, PutPrivilegesAction.INSTANCE, new PutPrivilegesRequest());
    }

    static ApplicationPrivilegeDescriptor parsePrivilege(XContentParser parser, String applicationName, String privilegeName)
        throws IOException {
        ApplicationPrivilegeDescriptor privilege = ApplicationPrivilegeDescriptor.parse(parser, applicationName, privilegeName, false);
        checkPrivilegeName(privilege, applicationName, privilegeName);
        return privilege;
    }

    /**
     * Populate the put privileges request using the given source, application name and privilege name
     * The source must contain a top-level object, keyed by application name.
     * The value for each application-name, is an object keyed by privilege name.
     * The value for each privilege-name is a privilege object which much match the application and privilege names in which it is nested.
     */
    public PutPrivilegesRequestBuilder source(BytesReference source, XContentType xContentType) throws IOException {
        Objects.requireNonNull(xContentType);
        // EMPTY is ok here because we never call namedObject
        try (
            InputStream stream = source.streamInput();
            XContentParser parser = xContentType.xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)
        ) {
            XContentParser.Token token = parser.currentToken();
            if (token == null) {
                token = parser.nextToken();
            }
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("expected object but found {} instead", token);
            }

            List<ApplicationPrivilegeDescriptor> privileges = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                token = parser.currentToken();
                assert token == XContentParser.Token.FIELD_NAME : "Invalid token " + token;
                final String applicationName = parser.currentName();

                token = parser.nextToken();
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new ElasticsearchParseException(
                        "expected the value for {} to be an object, but found {} instead",
                        applicationName,
                        token
                    );
                }

                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    token = parser.currentToken();
                    assert (token == XContentParser.Token.FIELD_NAME);
                    final String privilegeName = parser.currentName();

                    token = parser.nextToken();
                    if (token != XContentParser.Token.START_OBJECT) {
                        throw new ElasticsearchParseException(
                            "expected the value for {} to be an object, but found {} instead",
                            applicationName,
                            token
                        );
                    }
                    privileges.add(parsePrivilege(parser, applicationName, privilegeName));
                }
            }
            request.setPrivileges(privileges);
        }
        return this;
    }

    private static void checkPrivilegeName(ApplicationPrivilegeDescriptor privilege, String applicationName, String providedName) {
        final String privilegeName = privilege.getName();
        if (Strings.isNullOrEmpty(applicationName) == false && applicationName.equals(privilege.getApplication()) == false) {
            throw new IllegalArgumentException(
                "privilege application ["
                    + privilege.getApplication()
                    + "] in source does not match the provided application ["
                    + applicationName
                    + "]"
            );
        }
        if (Strings.isNullOrEmpty(providedName) == false && providedName.equals(privilegeName) == false) {
            throw new IllegalArgumentException(
                "privilege name [" + privilegeName + "] in source does not match the provided name [" + providedName + "]"
            );
        }
    }
}
