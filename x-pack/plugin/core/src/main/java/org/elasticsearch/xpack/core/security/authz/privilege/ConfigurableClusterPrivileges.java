/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.privilege.ApplicationPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataAction;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataRequest;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege.Category;
import org.elasticsearch.xpack.core.security.support.StringMatcher;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Static utility class for working with {@link ConfigurableClusterPrivilege} instances
 */
public final class ConfigurableClusterPrivileges {

    public static final ConfigurableClusterPrivilege[] EMPTY_ARRAY = new ConfigurableClusterPrivilege[0];

    public static final Writeable.Reader<ConfigurableClusterPrivilege> READER = in1 -> in1.readNamedWriteable(
        ConfigurableClusterPrivilege.class
    );
    public static final Writeable.Writer<ConfigurableClusterPrivilege> WRITER = (out1, value) -> out1.writeNamedWriteable(value);

    private ConfigurableClusterPrivileges() {}

    /**
     * Utility method to read an array of {@link ConfigurableClusterPrivilege} objects from a {@link StreamInput}
     */
    public static ConfigurableClusterPrivilege[] readArray(StreamInput in) throws IOException {
        return in.readArray(READER, ConfigurableClusterPrivilege[]::new);
    }

    /**
     * Utility method to write an array of {@link ConfigurableClusterPrivilege} objects to a {@link StreamOutput}
     */
    public static void writeArray(StreamOutput out, ConfigurableClusterPrivilege[] privileges) throws IOException {
        out.writeArray(WRITER, privileges);
    }

    /**
     * Writes a single object value to the {@code builder} that contains each of the provided privileges.
     * The privileges are grouped according to their {@link ConfigurableClusterPrivilege#getCategory() categories}
     */
    public static XContentBuilder toXContent(
        XContentBuilder builder,
        ToXContent.Params params,
        Collection<ConfigurableClusterPrivilege> privileges
    ) throws IOException {
        builder.startObject();
        for (Category category : Category.values()) {
            builder.startObject(category.field.getPreferredName());
            for (ConfigurableClusterPrivilege privilege : privileges) {
                if (category == privilege.getCategory()) {
                    privilege.toXContent(builder, params);
                }
            }
            builder.endObject();
        }
        return builder.endObject();
    }

    /**
     * Read a list of privileges from the parser. The parser should be positioned at the
     * {@link XContentParser.Token#START_OBJECT} token for the privileges value
     */
    public static List<ConfigurableClusterPrivilege> parse(XContentParser parser) throws IOException {
        List<ConfigurableClusterPrivilege> privileges = new ArrayList<>();

        expectedToken(parser.currentToken(), parser, XContentParser.Token.START_OBJECT);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);

            expectFieldName(parser, Category.APPLICATION.field, Category.PROFILE.field);
            if (Category.APPLICATION.field.match(parser.currentName(), parser.getDeprecationHandler())) {
                expectedToken(parser.nextToken(), parser, XContentParser.Token.START_OBJECT);
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);

                    expectFieldName(parser, ManageApplicationPrivileges.Fields.MANAGE);
                    privileges.add(ManageApplicationPrivileges.parse(parser));
                }
            } else {
                assert Category.PROFILE.field.match(parser.currentName(), parser.getDeprecationHandler());
                expectedToken(parser.nextToken(), parser, XContentParser.Token.START_OBJECT);
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);

                    expectFieldName(parser, WriteProfileDataPrivileges.Fields.WRITE);
                    privileges.add(WriteProfileDataPrivileges.parse(parser));
                }
            }
        }

        return privileges;
    }

    private static void expectedToken(XContentParser.Token read, XContentParser parser, XContentParser.Token expected) {
        if (read != expected) {
            throw new XContentParseException(
                parser.getTokenLocation(),
                "failed to parse privilege. expected [" + expected + "] but found [" + read + "] instead"
            );
        }
    }

    private static void expectFieldName(XContentParser parser, ParseField... fields) throws IOException {
        final String fieldName = parser.currentName();
        if (Arrays.stream(fields).anyMatch(pf -> pf.match(fieldName, parser.getDeprecationHandler())) == false) {
            throw new XContentParseException(
                parser.getTokenLocation(),
                "failed to parse privilege. expected "
                    + (fields.length == 1 ? "field name" : "one of")
                    + " ["
                    + Strings.arrayToCommaDelimitedString(fields)
                    + "] but found ["
                    + fieldName
                    + "] instead"
            );
        }
    }

    /**
     * The {@link WriteProfileDataPrivileges} privilege is a {@link ConfigurableClusterPrivilege} that grants the
     * ability to write the {@code data} and {@code access} sections of any user profile.
     * The privilege is namespace configurable such that only specific top-level keys in the {@code data} and {@code access}
     * dictionary permit writes (wildcards and regexps are supported, but exclusions are not).
     */
    public static class WriteProfileDataPrivileges implements ConfigurableClusterPrivilege {
        public static final String WRITEABLE_NAME = "write-profile-data-privileges";

        private final Set<String> applicationNames;
        private final Predicate<String> applicationPredicate;
        private final Predicate<TransportRequest> requestPredicate;

        public WriteProfileDataPrivileges(Set<String> applicationNames) {
            this.applicationNames = Collections.unmodifiableSet(applicationNames);
            this.applicationPredicate = StringMatcher.of(applicationNames);
            this.requestPredicate = request -> {
                if (request instanceof final UpdateProfileDataRequest updateProfileRequest) {
                    assert null == updateProfileRequest.validate();
                    final Collection<String> requestApplicationNames = updateProfileRequest.getApplicationNames();
                    return requestApplicationNames.stream().allMatch(application -> applicationPredicate.test(application));
                }
                return false;
            };
        }

        @Override
        public Category getCategory() {
            return Category.PROFILE;
        }

        public Collection<String> getApplicationNames() {
            return this.applicationNames;
        }

        @Override
        public String getWriteableName() {
            return WRITEABLE_NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(this.applicationNames, StreamOutput::writeString);
        }

        public static WriteProfileDataPrivileges createFrom(StreamInput in) throws IOException {
            final Set<String> applications = in.readSet(StreamInput::readString);
            return new WriteProfileDataPrivileges(applications);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(Fields.WRITE.getPreferredName(), Map.of(Fields.APPLICATIONS.getPreferredName(), applicationNames));
        }

        public static WriteProfileDataPrivileges parse(XContentParser parser) throws IOException {
            expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);
            expectFieldName(parser, Fields.WRITE);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.START_OBJECT);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.FIELD_NAME);
            expectFieldName(parser, Fields.APPLICATIONS);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.START_ARRAY);
            final String[] applications = XContentUtils.readStringArray(parser, false);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.END_OBJECT);
            return new WriteProfileDataPrivileges(new LinkedHashSet<>(Arrays.asList(applications)));
        }

        @Override
        public String toString() {
            return "{"
                + getCategory()
                + ":"
                + Fields.WRITE.getPreferredName()
                + ":"
                + Fields.APPLICATIONS.getPreferredName()
                + "="
                + Strings.collectionToDelimitedString(applicationNames, ",")
                + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final WriteProfileDataPrivileges that = (WriteProfileDataPrivileges) o;
            return this.applicationNames.equals(that.applicationNames);
        }

        @Override
        public int hashCode() {
            return applicationNames.hashCode();
        }

        @Override
        public ClusterPermission.Builder buildPermission(ClusterPermission.Builder builder) {
            return builder.add(this, Set.of(UpdateProfileDataAction.NAME), requestPredicate);
        }

        private interface Fields {
            ParseField WRITE = new ParseField("write");
            ParseField APPLICATIONS = new ParseField("applications");
        }
    }

    /**
     * The {@code ManageApplicationPrivileges} privilege is a {@link ConfigurableClusterPrivilege} that grants the
     * ability to execute actions related to the management of application privileges (Get, Put, Delete) for a subset
     * of applications (identified by a wildcard-aware application-name).
     */
    public static class ManageApplicationPrivileges implements ConfigurableClusterPrivilege {
        public static final String WRITEABLE_NAME = "manage-application-privileges";

        private final Set<String> applicationNames;
        private final Predicate<String> applicationPredicate;
        private final Predicate<TransportRequest> requestPredicate;

        public ManageApplicationPrivileges(Set<String> applicationNames) {
            this.applicationNames = Collections.unmodifiableSet(applicationNames);
            this.applicationPredicate = StringMatcher.of(applicationNames);
            this.requestPredicate = request -> {
                if (request instanceof final ApplicationPrivilegesRequest privRequest) {
                    final Collection<String> requestApplicationNames = privRequest.getApplicationNames();
                    return requestApplicationNames.isEmpty()
                        ? this.applicationNames.contains("*")
                        : requestApplicationNames.stream().allMatch(application -> applicationPredicate.test(application));
                }
                return false;
            };

        }

        @Override
        public Category getCategory() {
            return Category.APPLICATION;
        }

        public Collection<String> getApplicationNames() {
            return this.applicationNames;
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
            return builder.field(Fields.MANAGE.getPreferredName(), Map.of(Fields.APPLICATIONS.getPreferredName(), applicationNames));
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
            return "{"
                + getCategory()
                + ":"
                + Fields.MANAGE.getPreferredName()
                + ":"
                + Fields.APPLICATIONS.getPreferredName()
                + "="
                + Strings.collectionToDelimitedString(applicationNames, ",")
                + "}";
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

        @Override
        public ClusterPermission.Builder buildPermission(final ClusterPermission.Builder builder) {
            return builder.add(this, Set.of("cluster:admin/xpack/security/privilege/*"), requestPredicate);
        }

        private interface Fields {
            ParseField MANAGE = new ParseField("manage");
            ParseField APPLICATIONS = new ParseField("applications");
        }
    }
}
