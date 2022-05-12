/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.file;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.support.Validation;

/**
 * Provides utility methods for validating user and role names stored in the file realm.
 * Note: This class is a wrapper of the {@link Validation} which allows longer names
 * than what's allowed by default ({@link Validation#DEFAULT_MAX_NAME_LENGTH}).
 */
public final class FileRealmValidationUtil {

    /**
     * Maximal length of user and role names for file realm.
     */
    public static final int MAX_NAME_LENGTH = 1024;

    public static Validation.Error validateUsername(String username, boolean allowReserved, Settings settings) {
        return Validation.Users.validateUsername(username, allowReserved, settings, MAX_NAME_LENGTH);
    }

    public static Validation.Error validateRoleName(String roleName, boolean allowReserved) {
        return Validation.Roles.validateRoleName(roleName, allowReserved, MAX_NAME_LENGTH);
    }

    private FileRealmValidationUtil() {
        throw new IllegalAccessError("Not allowed!");
    }
}
