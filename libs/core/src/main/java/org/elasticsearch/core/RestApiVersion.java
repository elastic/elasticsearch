/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

/**
 * Vestigial marker to make v8-style version-aware routing backportable to 7.x.
 */
public enum RestApiVersion {

    V_7;

    public static RestApiVersion current() {
        return V_7;
    }

}
