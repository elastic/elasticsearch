/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.privilege.ApplicationPrivilegesRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * The {@code ManageApplicationPrivileges} privilege is a {@link GlobalClusterPrivilege} that grants the
 * ability to execute actions related to the management of application privileges (Get, Put, Delete) for a subset
 * of applications (identified by a wildcard-aware application-name).
 */
public final class ManageApplicationPrivileges implements GlobalClusterPrivilege {

    private static final ClusterPrivilege PRIVILEGE = ClusterPrivilege.get(
        Collections.singleton("cluster:admin/xpack/security/privilege/*")
    ).v1();
    public static final String WRITEABLE_NAME = "manage-application-privileges";

    private final Set<String> applicationNames;
    private final Predicate<String> applicationPredicate;
    private final BiPredicate<TransportRequest, Authentication> requestPredicate;

    public ManageApplicationPrivileges(Set<String> applicationNames) {
        this.applicationNames = Collections.unmodifiableSet(applicationNames);
        this.applicationPredicate = Automatons.predicate(applicationNames);
        this.requestPredicate = (request, authentication) -> {
            if (request instanceof ApplicationPrivilegesRequest) {
                final ApplicationPrivilegesRequest privRequest = (ApplicationPrivilegesRequest) request;
                final Collection<String> requestApplicationNames = privRequest.getApplicationNames();
                return requestApplicationNames.isEmpty() ? this.applicationNames.contains("*")
                    : requestApplicationNames.stream().allMatch(application -> applicationPredicate.test(application));
            }
            return false;
        };
    }

    @Override
    public Category getCategory() {
        return Category.APPLICATION;
    }

    @Override
    public ClusterPrivilege getPrivilege() {
        return PRIVILEGE;
    }

    @Override
    public BiPredicate<TransportRequest, Authentication> getRequestPredicate() {
        return this.requestPredicate;
    }

    public Collection<String> getApplicationNames() {
        return Collections.unmodifiableCollection(this.applicationNames);
    }

    @Override
    public String getWriteableName() {
        return WRITEABLE_NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(this.applicationNames, StreamOutput::writeString);
    }

    public static ManageApplicationPrivileges createFrom(StreamInput in) throws IOException {
        final Set<String> applications = in.readSet(StreamInput::readString);
        return new ManageApplicationPrivileges(applications);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(Fields.MANAGE.getPreferredName(),
            Collections.singletonMap(Fields.APPLICATIONS.getPreferredName(), applicationNames)
        );
    }

    public static ManageApplicationPrivileges parse(XContentParser parser) throws IOException {
        expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);
        expectFieldName(parser, Fields.MANAGE);
        expectedToken(parser.nextToken(), parser, XContentParser.Token.START_OBJECT);
        expectedToken(parser.nextToken(), parser, XContentParser.Token.FIELD_NAME);
        expectFieldName(parser, Fields.APPLICATIONS);
        expectedToken(parser.nextToken(), parser, XContentParser.Token.START_ARRAY);
        final String[] applications = XContentUtils.readStringArray(parser, false);
        expectedToken(parser.nextToken(), parser, XContentParser.Token.END_OBJECT);
        return new ManageApplicationPrivileges(new LinkedHashSet<>(Arrays.asList(applications)));
    }

    @Override
    public String toString() {
        return "{" + getCategory() + ":" + Fields.MANAGE.getPreferredName() + ":" + Fields.APPLICATIONS.getPreferredName() + "="
            + Strings.collectionToDelimitedString(applicationNames, ",") + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ManageApplicationPrivileges that = (ManageApplicationPrivileges) o;
        return this.applicationNames.equals(that.applicationNames);
    }

    @Override
    public int hashCode() {
        return applicationNames.hashCode();
    }

    interface Fields {
        ParseField MANAGE = new ParseField("manage");
        ParseField APPLICATIONS = new ParseField("applications");
    }

    private static void expectedToken(XContentParser.Token read, XContentParser parser, XContentParser.Token expected) {
        if (read != expected) {
            throw new XContentParseException(parser.getTokenLocation(),
                "failed to parse privilege. expected [" + expected + "] but found [" + read + "] instead");
        }
    }

    private static void expectFieldName(XContentParser parser, ParseField... fields) throws IOException {
        final String fieldName = parser.currentName();
        if (Arrays.stream(fields).anyMatch(pf -> pf.match(fieldName, parser.getDeprecationHandler())) == false) {
            throw new XContentParseException(parser.getTokenLocation(),
                "failed to parse privilege. expected " + (fields.length == 1 ? "field name" : "one of") + " ["
                    + Strings.arrayToCommaDelimitedString(fields) + "] but found [" + fieldName + "] instead");
        }
    }
}
