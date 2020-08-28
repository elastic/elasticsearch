/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.support.mapper;

public final class NativeRoleMappingStoreField {

    public static final String DOC_TYPE_FIELD = "doc_type";
    public static final String DOC_TYPE_ROLE_MAPPING = "role-mapping";
    public static final String ID_PREFIX = DOC_TYPE_ROLE_MAPPING + "_";

    private NativeRoleMappingStoreField() {}
}
