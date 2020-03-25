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

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.util.set.Sets;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Request object to retrieve roles from the native roles store
 */
public final class GetRolesRequest implements Validatable {

    private final Set<String> roleNames;

    public GetRolesRequest(final String... roleNames) {
        if (roleNames != null) {
            this.roleNames = Collections.unmodifiableSet(Sets.newHashSet(roleNames));
        } else {
            this.roleNames = Collections.emptySet();
        }
    }

    public Set<String> getRoleNames() {
        return roleNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final GetRolesRequest that = (GetRolesRequest) o;
        return Objects.equals(roleNames, that.roleNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roleNames);
    }
}
