/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequest;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

public class ClusterPrivilegePolicy implements ToXContentObject, Writeable {
    public static final ClusterPrivilegePolicy EMPTY = new ClusterPrivilegePolicy();

    private final Map<Category, List<ConditionalPrivilege>> privileges;

    protected ClusterPrivilegePolicy() {
        this(Collections.emptyMap());
    }

    private ClusterPrivilegePolicy(Map<Category, List<ConditionalPrivilege>> privileges) {
        this.privileges = privileges;
    }

    public static ClusterPrivilegePolicy createFrom(StreamInput in) throws IOException {
        final Map<Category, List<ConditionalPrivilege>> map = in.readMapOfLists(
            Category::read,
            i -> i.readNamedWriteable(ConditionalPrivilege.class)
        );
        return new ClusterPrivilegePolicy(map);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMapOfLists(this.privileges, StreamOutput::writeEnum, StreamOutput::writeNamedWriteable);
    }

    public static ClusterPrivilegePolicy parse(XContentParser parser) throws IOException {
        Map<Category, List<ConditionalPrivilege>> map = new HashMap<>();

        if (parser.currentToken() == null) {
            parser.nextToken();
        }

        expectedToken(parser.currentToken(), parser, XContentParser.Token.START_OBJECT);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);

            expectFieldName(parser, Category.APPLICATION.field);
            final Category currentCategory = Category.APPLICATION;
            final List<ConditionalPrivilege> currentPrivilegeList = new ArrayList<>();
            map.put(currentCategory, currentPrivilegeList);

            expectedToken(parser.nextToken(), parser, XContentParser.Token.START_OBJECT);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.FIELD_NAME);

            expectFieldName(parser, ManageApplicationPrivileges.Fields.MANAGE);
            currentPrivilegeList.add(ManageApplicationPrivileges.parse(parser));
            expectedToken(parser.nextToken(), parser, XContentParser.Token.END_OBJECT);
        }

        return new ClusterPrivilegePolicy(map);
    }

    private static void expectedToken(XContentParser.Token read, XContentParser parser, XContentParser.Token expected) {
        if (read != expected) {
            throw new XContentParseException(parser.getTokenLocation(),
                "failed to parse privilege. expected [" + expected + "] but found [" + parser.currentToken() + "] instead");
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


    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Category category : privileges.keySet()) {
            builder.startObject(category.field.getPreferredName());
            for (ConditionalPrivilege privilege : privileges.get(category)) {
                privilege.toXContent(builder, params);
            }
            builder.endObject();
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClusterPrivilegePolicy that = (ClusterPrivilegePolicy) o;
        return this.privileges.equals(that.privileges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(privileges);
    }

    public boolean isEmpty() {
        return privileges.isEmpty();
    }

    @Override
    public String toString() {
        return privileges.toString();
    }

    public Collection<ConditionalPrivilege> get(Category category) {
        return Collections.unmodifiableCollection(privileges.getOrDefault(category, Collections.emptyList()));
    }

    public static Builder builder() {
        return new Builder();
    }

    public interface ConditionalPrivilege extends NamedWriteable, ToXContentFragment {
        Category getCategory();

        ClusterPrivilege getPrivilege();

        Predicate<TransportRequest> getRequestPredicate();
    }

    public static class ManageApplicationPrivileges implements ConditionalPrivilege {

        private static final ClusterPrivilege PRIVILEGE = ClusterPrivilege.get(
            Collections.singleton("cluster:admin/xpack/security/privilege/*")
        );
        public static final String NAME = "manage-application-privileges";

        private final Set<String> applicationNames;
        private final Predicate<String> applicationPredicate;
        private final Predicate<TransportRequest> requestPredicate;

        public ManageApplicationPrivileges(Set<String> applicationNames) {
            this.applicationNames = Collections.unmodifiableSet(applicationNames);
            this.applicationPredicate = Automatons.predicate(applicationNames);
            this.requestPredicate = new Predicate<TransportRequest>() {

                private boolean testApplication(String application) {
                    return applicationPredicate.test(application);
                }

                private boolean testPrivileges(List<ApplicationPrivilegeDescriptor> privileges) {
                    return privileges.stream().map(ApplicationPrivilegeDescriptor::getApplication).allMatch(this::testApplication);
                }

                @Override
                public boolean test(TransportRequest request) {
                    if (request instanceof GetPrivilegesRequest) {
                        return testApplication(((GetPrivilegesRequest) request).application());
                    }
                    if (request instanceof PutPrivilegesRequest) {
                        return testPrivileges(((PutPrivilegesRequest) request).getPrivileges());
                    }
                    if (request instanceof DeletePrivilegesRequest) {
                        return testApplication(((DeletePrivilegesRequest) request).application());
                    }
                    return false;
                }
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
        public Predicate<TransportRequest> getRequestPredicate() {
            return this.requestPredicate;
        }

        public Collection<String> getApplicationNames() {
            return Collections.unmodifiableCollection(this.applicationNames);
        }

        @Override
        public String getWriteableName() {
            return NAME;
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
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
            }
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
            return "{" + Fields.MANAGE.getPreferredName() + " " + Fields.APPLICATIONS.getPreferredName() + "="
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
            return Objects.hash(applicationNames);
        }

        private interface Fields {
            ParseField MANAGE = new ParseField("manage");
            ParseField APPLICATIONS = new ParseField("applications");
        }
    }

    public enum Category {
        APPLICATION(new ParseField("application"));

        public final ParseField field;

        Category(ParseField field) {
            this.field = field;
        }

        private static Category read(StreamInput input) throws IOException {
            return input.readEnum(Category.class);
        }
    }

    public static class Builder {
        private final Map<Category, List<ConditionalPrivilege>> privileges = new HashMap<>();

        public Builder add(ConditionalPrivilege privilege) {
            this.privileges.computeIfAbsent(privilege.getCategory(), (k) -> new ArrayList<>()).add(privilege);
            return this;
        }

        public ClusterPrivilegePolicy build() {
            return new ClusterPrivilegePolicy(privileges);
        }
    }
}
