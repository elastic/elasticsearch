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
 * Request object to get role mappings
 */
public final class GetRoleMappingsRequest implements Validatable {
    private final Set<String> roleMappingNames;

    public GetRoleMappingsRequest(final String... roleMappingNames) {
        if (roleMappingNames != null) {
           this.roleMappingNames = Collections.unmodifiableSet(Sets.newHashSet(roleMappingNames));
        } else {
           this.roleMappingNames = Collections.emptySet();
        }
    }

    public Set<String> getRoleMappingNames() {
        return roleMappingNames;
    }

    @Override
    public int hashCode() {
        return Objects.hash(roleMappingNames);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final GetRoleMappingsRequest other = (GetRoleMappingsRequest) obj;

        return Objects.equals(roleMappingNames, other.roleMappingNames);
    }

}
