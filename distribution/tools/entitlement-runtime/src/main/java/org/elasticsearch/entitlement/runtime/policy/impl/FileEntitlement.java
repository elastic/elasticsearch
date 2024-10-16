/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy.impl;

public class FileEntitlement {

    public static final int READ_ACTION = 0x1;
    public static final int WRITE_ACTION = 0x2;

    private final String path;
    private final int actions;

    public FileEntitlement(String path, int actions) {
        this.path = path;
        this.actions = actions;
    }
}
