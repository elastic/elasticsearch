/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz;

public final class IndicesAndAliasesResolverField {
    //placeholder used in the security plugin to indicate that the request is authorized knowing that it will yield an empty response
    public static final String NO_INDEX_PLACEHOLDER = "-*";

    private IndicesAndAliasesResolverField() {}

}
