/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.user;

/**
 * Fields/constants associated with the {@link org.elasticsearch.action.support.user.ActionUser interface}
 */
public class ActionUserFields {

    /**
     * For internal use only, publicly accessible because it is needed across packages.
     * Direct use of this field may lead to incorrect or inconsistent security information.
     */
    public static final String TRANSIENT_HEADER = "action.user";
}
