/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.security.user.privileges.ApplicationResourcePrivileges;
import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.core.Nullable;
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
