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
import org.elasticsearch.client.security.user.privileges.ApplicationResourcePrivileges;
import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;

/**
 * Request to determine whether the current user has a list of privileges.
 */
public final class HasPrivilegesRequest implements Validatable, ToXContentObject {

    private final Set<String> clusterPrivileges;
    private final Set<IndicesPrivileges> indexPrivileges;
    private final Set<ApplicationResourcePrivileges> applicationPrivileges;

    public HasPrivilegesRequest(@Nullable Set<String> clusterPrivileges,
                                @Nullable Set<IndicesPrivileges> indexPrivileges,
                                @Nullable Set<ApplicationResourcePrivileges> applicationPrivileges) {
        this.clusterPrivileges = clusterPrivileges == null ? emptySet() : unmodifiableSet(clusterPrivileges);
        this.indexPrivileges = indexPrivileges == null ? emptySet() : unmodifiableSet(indexPrivileges);
        this.applicationPrivileges = applicationPrivileges == null ? emptySet() : unmodifiableSet(applicationPrivileges);

        if (this.clusterPrivileges.isEmpty() && this.indexPrivileges.isEmpty() && this.applicationPrivileges.isEmpty()) {
            throw new IllegalArgumentException("At last 1 privilege must be specified");
        }
    }

    public Set<String> getClusterPrivileges() {
        return clusterPrivileges;
    }

    public Set<IndicesPrivileges> getIndexPrivileges() {
        return indexPrivileges;
    }

    public Set<ApplicationResourcePrivileges> getApplicationPrivileges() {
        return applicationPrivileges;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("cluster", clusterPrivileges)
            .field("index", indexPrivileges)
            .field("application", applicationPrivileges)
            .endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final HasPrivilegesRequest that = (HasPrivilegesRequest) o;
        return Objects.equals(clusterPrivileges, that.clusterPrivileges) &&
            Objects.equals(indexPrivileges, that.indexPrivileges) &&
            Objects.equals(applicationPrivileges, that.applicationPrivileges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterPrivileges, indexPrivileges, applicationPrivileges);
    }
}
