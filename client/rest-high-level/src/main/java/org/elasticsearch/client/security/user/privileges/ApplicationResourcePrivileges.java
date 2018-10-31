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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public final class ApplicationResourcePrivileges implements ToXContentObject {

    private static final ParseField APPLICATION = new ParseField("application");
    private static final ParseField PRIVILEGES = new ParseField("privileges");
    private static final ParseField RESOURCES = new ParseField("resources");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ApplicationResourcePrivileges, Void> PARSER = new ConstructingObjectParser<>(
            "application_privileges", false, constructorObjects -> {
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

    private ApplicationResourcePrivileges(String application, List<String> privileges, List<String> resources) {
        // we do all null checks inside the constructor
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
        this.privileges = Collections.unmodifiableList(privileges);
        this.resources = Collections.unmodifiableList(resources);
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
        return application.equals(that.application) && privileges.equals(that.privileges) && resources.equals(that.resources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(application, privileges, resources);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName()).append("[");
                sb.append(APPLICATION.getPreferredName()).append("=[").append(application).append("], ");
                sb.append(PRIVILEGES.getPreferredName()).append("=[").append(Strings.collectionToCommaDelimitedString(privileges)).append("], ");
                sb.append(RESOURCES.getPreferredName()).append("=[").append(Strings.collectionToCommaDelimitedString(resources)).append("]]");
        return sb.toString();
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private @Nullable String application = null;
        private @Nullable List<String> privileges = null;
        private @Nullable List<String> resources = null;

        private Builder() {
        }

        public Builder application(@Nullable String appName) {
            this.application = appName;
            return this;
        }

        public Builder resources(@Nullable String... resources) {
            if (resources == null) {
                // null is a no-op to be programmer friendly
                return this;
            }
            return resources(Arrays.asList(resources));
        }

        public Builder resources(@Nullable List<String> resources) {
            this.resources = resources;
            return this;
        }

        public Builder privileges(@Nullable String... privileges) {
            if (privileges == null) {
                // null is a no-op to be programmer friendly
                return this;
            }
            return privileges(Arrays.asList(privileges));
        }

        public Builder privileges(@Nullable List<String> privileges) {
            this.privileges = privileges;
            return this;
        }

        public ApplicationResourcePrivileges build() {
            return new ApplicationResourcePrivileges(application, privileges, resources);
        }

    }
}