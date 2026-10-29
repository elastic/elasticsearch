/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.TransportVersion;

public enum Nullability {
    /**
     * Whether the expression can become null
     */
    TRUE,

    /**
     * The expression can never become null
     */
    FALSE,

    /**
     * Cannot determine if the expression supports possible null folding
     */
    UNKNOWN;

    /**
     * Deliberately unused: this branch keeps serializing {@link Nullability} as an enum. Newer branches serialize it as a
     * boolean, but treat this branch's patch id as still using the enum format. It's only referenced so the transport
     * version resources match the main branch.
     */
    private static final TransportVersion ESQL_NULLABILITY_REMOVE_UNKNOWN = TransportVersion.fromName("esql_nullability_remove_unknown");
}
