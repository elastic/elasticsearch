/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.common.settings.Settings;

/**
 * Provides utility methods for validating user and role names stored in the native realm.
 * Note: This class is a wrapper of the {@link Validation} which allow names to have {@link #MAX_NAME_LENGTH}.
 */
public final class NativeRealmValidationUtil {

    /**
     * In the native realm, the maximal user and role name lenght is influenced by the maximal document ID
     * (see {@link org.elasticsearch.action.index.IndexRequest#MAX_DOCUMENT_ID_LENGTH_IN_BYTES}).
     * Because the document IDs are prefixed by either {@code user-} or {@code role-},
     * the actual possible max length is 507 chars.
     *
     * Note: If we choose (in the future) to allow more than 507 chars we should
     * either consider increasing the max allowed size for document ID or
     * consider hashing names longer than 507 in order to fit into document ID.
     */
    public static final int MAX_NAME_LENGTH = 507;

    public static Validation.Error validateUsername(String username, boolean allowReserved, Settings settings) {
        return Validation.Users.validateUsername(username, allowReserved, settings, MAX_NAME_LENGTH);
    }

    public static Validation.Error validateRoleName(String roleName, boolean allowReserved) {
        return Validation.Roles.validateRoleName(roleName, allowReserved, MAX_NAME_LENGTH);
    }

    private NativeRealmValidationUtil() {
        throw new IllegalAccessError("Not allowed!");
    }

}
