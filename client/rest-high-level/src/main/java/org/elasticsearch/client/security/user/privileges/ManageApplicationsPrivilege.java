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

package org.elasticsearch.client.security.user.privileges;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public final class ManageApplicationsPrivilege implements ToXContentObject {

    public static final ParseField CATEGORY = new ParseField("application");
    public static final ParseField SCOPE = new ParseField("manage");
    public static final ParseField APPLICATIONS = new ParseField("applications");

    private static final ConstructingObjectParser<ManageApplicationsPrivilege, Void> PARSER =
        new ConstructingObjectParser<>("application_privileges", false, constructorObjects -> {
                return (ManageApplicationsPrivilege) constructorObjects[0];
            });

    static {
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<ManageApplicationsPrivilege, Void> apps_parser =
            new ConstructingObjectParser<>("apps_parser", false, constructorObjects -> {
                    final Collection<String> applications = (Collection<String>) constructorObjects[0];
                    return new ManageApplicationsPrivilege(applications);
                });
        apps_parser.declareStringArray(constructorArg(), APPLICATIONS);

        final ConstructingObjectParser<ManageApplicationsPrivilege, Void> scope_parser =
                new ConstructingObjectParser<>("scope_parser", false, constructorObjects -> {
                        return (ManageApplicationsPrivilege) constructorObjects[0];
                    });
        scope_parser.declareObject(constructorArg(), apps_parser, SCOPE);

        PARSER.declareObject(constructorArg(), scope_parser, CATEGORY);
    }

    // uniqueness and order are important for equals and hashcode
    private final SortedSet<String> applications;

    private ManageApplicationsPrivilege(Collection<String> applications) {
        // we do all null checks inside the constructor
        if (null == applications || applications.isEmpty()) {
            throw new IllegalArgumentException("managed applications list should not be null");
        }
        this.applications = Collections.unmodifiableSortedSet(new TreeSet<>(applications));
    }

    public SortedSet<String> getApplications() {
        return applications;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ManageApplicationsPrivilege that = (ManageApplicationsPrivilege) o;
        return applications.equals(that.applications);
    }

    @Override
    public int hashCode() {
        return applications.hashCode();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getSimpleName()).append("[");
        sb.append(APPLICATIONS.getPreferredName()).append("=[").append(Strings.collectionToCommaDelimitedString(applications));
        sb.append("]]");
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(CATEGORY.getPreferredName());
        builder.startObject(SCOPE.getPreferredName());
        builder.field(APPLICATIONS.getPreferredName(), applications);
        builder.endObject();
        builder.endObject();
        return builder.endObject();
    }

    public static ManageApplicationsPrivilege fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private @Nullable Collection<String> applications = null;

        private Builder() {
        }

        public Builder applications(@Nullable String... applications) {
            if (applications == null) {
                // null is a no-op to be programmer friendly
                return this;
            }
            return applications(Arrays.asList(applications));
        }

        public Builder applications(@Nullable Collection<String> applications) {
            this.applications = applications;
            return this;
        }

        public ManageApplicationsPrivilege build() {
            return new ManageApplicationsPrivilege(applications);
        }
    }
}
