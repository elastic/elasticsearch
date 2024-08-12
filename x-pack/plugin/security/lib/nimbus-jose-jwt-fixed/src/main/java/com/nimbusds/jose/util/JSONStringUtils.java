/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package com.nimbusds.jose.util;

import java.security.AccessController;
import java.security.PrivilegedAction;

public class JSONStringUtils {

    public static String toJSONString(final String string) {
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> InnerJSONStringUtils.toJSONString(string));
    }

    private JSONStringUtils() {}
}
