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

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class GlobalApplicationPrivileges implements ToXContentObject {
    
    static final ParseField APPLICATION = new ParseField("application");
    
    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<GlobalApplicationPrivileges, Void> PARSER = new ConstructingObjectParser<>(
            "global_application_privileges", false, constructorObjects -> {
                return new GlobalApplicationPrivileges((Collection<GlobalScopedPrivilege>) constructorObjects[0]);
            });

    static {
        PARSER.declareNamedObjects(constructorArg(), (p, c, n) -> GlobalScopedPrivilege.fromXContent(n, p), APPLICATION);
    }

    private final Set<? extends GlobalScopedPrivilege> privileges;

    public GlobalApplicationPrivileges(Collection<? extends GlobalScopedPrivilege> privileges) {
        this.privileges = Collections.unmodifiableSet(new HashSet<>(Objects.requireNonNull(privileges)));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (final GlobalScopedPrivilege privilege : privileges) {
            builder.field(privilege.getScope(), privilege.getRaw());
        }
        return builder.endObject();
    }

    public static GlobalApplicationPrivileges fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public Set<? extends GlobalScopedPrivilege> getPrivileges() {
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
        final GlobalApplicationPrivileges that = (GlobalApplicationPrivileges) o;
        return privileges.equals(that.privileges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(privileges);
    }

}