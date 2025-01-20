/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package com.nimbusds.jose.util;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * This class wraps {@link JSONStringUtils}, which is copied directly from the source library, and delegates to
 * that class as quickly as possible. This layer is only here to provide a point at which we can insert
 * {@link java.security.AccessController#doPrivileged(PrivilegedAction)} calls as necessary. We don't do anything here
 * other than ensure gson has the proper security manager permissions.
 */
public class JSONStringUtils {

    public static String toJSONString(final String string) {
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> JSONStringUtils.toJSONString(string));
    }

    private JSONStringUtils() {}
}
