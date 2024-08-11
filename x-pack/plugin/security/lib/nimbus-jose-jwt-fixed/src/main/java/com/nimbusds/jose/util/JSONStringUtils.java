/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package com.nimbusds.jose.util;

import com.nimbusds.jose.shaded.gson.Gson;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Copied from nimbus-jose-jwt version 9.37.3.
 *
 * Original code Copyright 2012-2016, Connect2id Ltd. Licensed under the Apache License, Version 2.0
 *
 * The only modifications in this file are:
 * 1) {@link AccessController#doPrivileged(PrivilegedAction)} calls to make gson work with the security manager
 * 2) Formatting/Warning suppression as necessary to work with our infrastructure
 * 3) This comment and the license comment
 */
public class JSONStringUtils {
    public static String toJSONString(String string) {
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> (new Gson()).toJson(string));
    }

    private JSONStringUtils() {}
}
