/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.user.privileges;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Represents privileges over resources that are scoped under an application.
 * The application, resources and privileges are completely managed by the
 * client and can be arbitrary string identifiers. Elasticsearch is not
 * concerned by any resources under an application scope.
 */
public final class ApplicationResourcePrivileges implements ToXContentObject {

    private static final ParseField APPLICATION = new ParseField("application");
    private static final ParseField PRIVILEGES = new ParseField("privileges");
    private static final ParseField RESOURCES = new ParseField("resources");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<ApplicationResourcePrivileges, Void> PARSER = new ConstructingObjectParser<>(
            "application_privileges", false, constructorObjects -> {
                // Don't ignore unknown fields. It is dangerous if the object we parse is also
                // part of a request that we build later on, and the fields that we now ignore will
                // end up being implicitly set to null in that request.
                int i = 0;
                final String application = (String) constructorObjects[i++];
                final List<String> privileges = (List<String>) constructorObjects[i++];
                final List<String> resources = (List<String>) constructorObjects[i];
                return new ApplicationResourcePrivileges(application, privileges, resources);
            });

    static {
        PARSER.declareString(constructorArg(), APPLICATION);
        PARSER.declareStringArray(constructorArg(), PRIVILEGES);
        PARSER.declareStringArray(constructorArg(), RESOURCES);
    }

    private final String application;
    private final List<String> privileges;
    private final List<String> resources;

    /**
     * Constructs privileges for resources under an application scope.
     *
     * @param application
     *            The application name. This identifier is completely under the
     *            clients control.
     * @param privileges
     *            The privileges names. Cannot be null or empty. Privilege
     *            identifiers are completely under the clients control.
     * @param resources
     *            The resources names. Cannot be null or empty. Resource identifiers
     *            are completely under the clients control.
     */
    public ApplicationResourcePrivileges(String application, List<String> privileges, List<String> resources) {
        if (Strings.isNullOrEmpty(application)) {
            throw new IllegalArgumentException("application privileges must have an application name");
        }
        if (null == privileges || privileges.isEmpty()) {
            throw new IllegalArgumentException("application privileges must define at least one privilege");
        }
        if (null == resources || resources.isEmpty()) {
            throw new IllegalArgumentException("application privileges must refer to at least one resource");
        }
        this.application = application;
        this.privileges = List.copyOf(privileges);
        this.resources = List.copyOf(resources);
    }

    public String getApplication() {
        return application;
    }

    public List<String> getResources() {
        return this.resources;
    }

    public List<String> getPrivileges() {
        return this.privileges;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        ApplicationResourcePrivileges that = (ApplicationResourcePrivileges) o;
        return application.equals(that.application)
                && privileges.equals(that.privileges)
                && resources.equals(that.resources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(application, privileges, resources);
    }

    @Override
    public String toString() {
        try {
            return XContentHelper.toXContent(this, XContentType.JSON, true).utf8ToString();
        } catch (IOException e) {
            throw new RuntimeException("Unexpected", e);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(APPLICATION.getPreferredName(), application);
        builder.field(PRIVILEGES.getPreferredName(), privileges);
        builder.field(RESOURCES.getPreferredName(), resources);
        return builder.endObject();
    }

    public static ApplicationResourcePrivileges fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

}
