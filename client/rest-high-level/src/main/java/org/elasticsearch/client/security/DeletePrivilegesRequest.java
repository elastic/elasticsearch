/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Nullable;

/**
 * A request to delete application privileges
 */
public final class DeletePrivilegesRequest implements Validatable {

    private final String application;
    private final String[] privileges;
    private final RefreshPolicy refreshPolicy;

    /**
     * Creates a new {@link DeletePrivilegesRequest} using the default {@link RefreshPolicy#getDefault()} refresh policy.
     *
     * @param application   the name of the application for which the privileges will be deleted
     * @param privileges    the privileges to delete
     */
    public DeletePrivilegesRequest(String application, String... privileges) {
        this(application, privileges, null);
    }

    /**
     * Creates a new {@link DeletePrivilegesRequest}.
     *
     * @param application   the name of the application for which the privileges will be deleted
     * @param privileges    the privileges to delete
     * @param refreshPolicy the refresh policy {@link RefreshPolicy} for the request, defaults to {@link RefreshPolicy#getDefault()}
     */
    public DeletePrivilegesRequest(String application, String[] privileges, @Nullable RefreshPolicy refreshPolicy) {
        if (Strings.hasText(application) == false) {
            throw new IllegalArgumentException("application name is required");
        }
        if (CollectionUtils.isEmpty(privileges)) {
            throw new IllegalArgumentException("privileges are required");
        }
        this.application = application;
        this.privileges = privileges;
        this.refreshPolicy = (refreshPolicy == null) ? RefreshPolicy.getDefault() : refreshPolicy;
    }

    public String getApplication() {
        return application;
    }

    public String[] getPrivileges() {
        return privileges;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }
}
