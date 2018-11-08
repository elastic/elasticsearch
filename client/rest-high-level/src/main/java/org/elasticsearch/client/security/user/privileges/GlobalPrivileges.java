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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represents global privileges. "Global Privilege" is a mantra for granular
 * privileges over applications. {@code ApplicationResourcePrivileges} model
 * application privileges over resources. This models user privileges over
 * applications. Every client is responsible to manage the applications as well
 * as the privileges for them.
 */
public final class GlobalPrivileges implements ToXContentObject {

    // when categories change, adapting this field should suffice
    static final List<String> CATEGORIES = Collections.unmodifiableList(Arrays.asList("application"));

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<GlobalPrivileges, Void> PARSER = new ConstructingObjectParser<>("global_application_privileges",
            false, constructorObjects -> {
                // ignore_unknown_fields is irrelevant here anyway, but let's keep it to false
                // because this conveys strictness (woop woop)
                return new GlobalPrivileges((Collection<GlobalOperationPrivilege>) constructorObjects[0]);
            });

    static {
        for (final String category : CATEGORIES) {
            PARSER.declareNamedObjects(optionalConstructorArg(),
                    (parser, context, operation) -> GlobalOperationPrivilege.fromXContent(category, operation, parser),
                    new ParseField(category));
        }
    }

    private final Set<? extends GlobalOperationPrivilege> privileges;

    /**
     * Constructs global privileges by bundling the set of application privileges.
     * 
     * @param applicationPrivileges
     *            The privileges over applications.
     */
    public GlobalPrivileges(Collection<? extends GlobalOperationPrivilege> applicationPrivileges) {
        if (applicationPrivileges == null || applicationPrivileges.isEmpty()) {
            throw new IllegalArgumentException("Application privileges cannot be empty or null");
        }
        this.privileges = Collections.unmodifiableSet(new HashSet<>(Objects.requireNonNull(applicationPrivileges)));
        final Set<String> allScopes = this.privileges.stream().map(p -> p.getScope()).collect(Collectors.toSet());
        if (allScopes.size() != this.privileges.size()) {
            throw new IllegalArgumentException(
                    "Application privileges have the same scope but the privileges differ. Only one privilege for any one scope is allowed.");
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (final String category : CATEGORIES) {
            builder.startObject(category);
            for (final GlobalOperationPrivilege privilege : privileges) {
                builder.field(privilege.getScope(), privilege.getRaw());
            }
            builder.endObject();
        }
        return builder.endObject();
    }

    public static GlobalPrivileges fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public Set<? extends GlobalOperationPrivilege> getPrivileges() {
        return privileges;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final GlobalPrivileges that = (GlobalPrivileges) o;
        return privileges.equals(that.privileges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(privileges);
    }

}