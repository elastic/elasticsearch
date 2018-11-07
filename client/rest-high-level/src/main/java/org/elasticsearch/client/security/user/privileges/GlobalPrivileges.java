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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represents global privileges. "Global Privilege" is a mantra for granular
 * privileges over applications. {@code ApplicationResourcePrivileges} model
 * application privileges over resources. This models user privileges over
 * applications. Every client is responsible to manage the applications as well
 * as the privileges for them.
 */
public final class GlobalPrivileges implements ToXContentObject {

    static final ParseField APPLICATION = new ParseField("application");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<GlobalPrivileges, Void> PARSER = new ConstructingObjectParser<>("global_application_privileges",
            true, constructorObjects -> {
                return new GlobalPrivileges((Collection<GlobalScopedPrivilege>) constructorObjects[0]);
            });

    static {
        PARSER.declareNamedObjects(optionalConstructorArg(), (p, c, n) -> GlobalScopedPrivilege.fromXContent(n, p), APPLICATION);
    }

    private final Set<? extends GlobalScopedPrivilege> applicationPrivileges;

    /**
     * Constructs global privileges from the set of application privileges.
     * 
     * @param applicationPrivileges
     *            The privileges over applications.
     */
    public GlobalPrivileges(Collection<? extends GlobalScopedPrivilege> applicationPrivileges) {
        this.applicationPrivileges = applicationPrivileges == null ? Collections.emptySet()
                : Collections.unmodifiableSet(new HashSet<>(applicationPrivileges));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(APPLICATION.getPreferredName());
        for (final GlobalScopedPrivilege privilege : applicationPrivileges) {
            builder.field(privilege.getScope(), privilege.getRaw());
        }
        builder.endObject();
        return builder.endObject();
    }

    public static GlobalPrivileges fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public Set<? extends GlobalScopedPrivilege> getPrivileges() {
        return applicationPrivileges;
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
        return applicationPrivileges.equals(that.applicationPrivileges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(applicationPrivileges);
    }

}