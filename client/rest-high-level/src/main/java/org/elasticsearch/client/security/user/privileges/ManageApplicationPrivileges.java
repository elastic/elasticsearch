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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public final class ManageApplicationPrivileges implements ToXContentObject {

    public static final ParseField CATEGORY = new ParseField("application");
    public static final ParseField SCOPE = new ParseField("manage");
    public static final ParseField APPLICATIONS = new ParseField("applications");

    private static final ConstructingObjectParser<ManageApplicationPrivileges, Void> PARSER =
        new ConstructingObjectParser<>("application_privileges", false, constructorObjects -> {
                return (ManageApplicationPrivileges) constructorObjects[0];
            });

    static {
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<ManageApplicationPrivileges, Void> apps_parser =
            new ConstructingObjectParser<>("apps_parser", false, constructorObjects -> {
                    final Collection<String> applications = (Collection<String>) constructorObjects[0];
                    return new ManageApplicationPrivileges(applications);
                });
        apps_parser.declareStringArray(constructorArg(), APPLICATIONS);

        final ConstructingObjectParser<ManageApplicationPrivileges, Void> scope_parser =
                new ConstructingObjectParser<>("scope_parser", false, constructorObjects -> {
                        return (ManageApplicationPrivileges) constructorObjects[0];
                    });
        scope_parser.declareObject(constructorArg(), apps_parser, SCOPE);

        PARSER.declareObject(constructorArg(), scope_parser, CATEGORY);
    }

    private final Collection<String> applications;

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

    public static ManageApplicationPrivileges fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private ManageApplicationPrivileges(@Nullable Collection<String> applications) {
        // we do all null checks inside the constructor
        if (null == applications || applications.isEmpty()) {
            throw new IllegalArgumentException("managed applications list should not be null");
        }
        this.applications = Collections.unmodifiableCollection(applications);
    }

    public static class Builder {

        private @Nullable Set<String> applications = null;

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
            if (applications == null) {
                // null is a no-op to be programmer friendly
                return this;
            }
            this.applications = new HashSet<>(applications);
            return this;
        }

        public ManageApplicationPrivileges build() {
            return new ManageApplicationPrivileges(applications);
        }
    }
}
