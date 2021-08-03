/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.security.user.privileges.ApplicationPrivilege;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Request object for creating/updating application privileges.
 */
public final class PutPrivilegesRequest implements Validatable, ToXContentObject {

    private final Map<String, List<ApplicationPrivilege>> privileges;
    private final RefreshPolicy refreshPolicy;

    public PutPrivilegesRequest(final List<ApplicationPrivilege> privileges, @Nullable final RefreshPolicy refreshPolicy) {
        if (privileges == null || privileges.isEmpty()) {
            throw new IllegalArgumentException("privileges are required");
        }
        this.privileges = Collections.unmodifiableMap(privileges.stream()
                .collect(Collectors.groupingBy(ApplicationPrivilege::getApplication, TreeMap::new, Collectors.toList())));
        this.refreshPolicy = refreshPolicy == null ? RefreshPolicy.IMMEDIATE : refreshPolicy;
    }

    /**
     * @return a map of application name to list of
     * {@link ApplicationPrivilege}s
     */
    public Map<String, List<ApplicationPrivilege>> getPrivileges() {
        return privileges;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(privileges, refreshPolicy);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || (this.getClass() != o.getClass())) {
            return false;
        }
        final PutPrivilegesRequest that = (PutPrivilegesRequest) o;
        return privileges.equals(that.privileges) && (refreshPolicy == that.refreshPolicy);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        for (Entry<String, List<ApplicationPrivilege>> entry : privileges.entrySet()) {
            builder.field(entry.getKey());
            builder.startObject();
            for (ApplicationPrivilege applicationPrivilege : entry.getValue()) {
                builder.field(applicationPrivilege.getName());
                applicationPrivilege.toXContent(builder, params);
            }
            builder.endObject();
        }
        return builder.endObject();
    }

}
