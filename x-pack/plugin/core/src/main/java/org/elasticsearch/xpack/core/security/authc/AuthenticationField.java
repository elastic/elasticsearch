/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

public final class AuthenticationField {

    public static final String AUTHENTICATION_KEY = "_xpack_security_authentication";
    public static final String PRIVILEGE_CATEGORY_KEY = "_security_privilege_category";
    public static final String PRIVILEGE_CATEGORY_VALUE_OPERATOR = "operator";
    public static final String PRIVILEGE_CATEGORY_VALUE_EMPTY = "__empty";

    public static final String CLOUD_API_KEY_REALM_NAME = "_cloud_api_key";
    public static final String CLOUD_API_KEY_REALM_TYPE = "_cloud_api_key";
    public static final String API_KEY_REALM_NAME = "_es_api_key";
    public static final String API_KEY_REALM_TYPE = "_es_api_key";

    public static final String API_KEY_CREATOR_REALM_NAME = "_security_api_key_creator_realm_name";
    public static final String API_KEY_CREATOR_REALM_TYPE = "_security_api_key_creator_realm_type";
    public static final String API_KEY_ID_KEY = "_security_api_key_id";
    public static final String API_KEY_NAME_KEY = "_security_api_key_name";
    public static final String API_KEY_INTERNAL_KEY = "_security_api_key_internal";
    public static final String API_KEY_TYPE_KEY = "_security_api_key_type";
    public static final String API_KEY_METADATA_KEY = "_security_api_key_metadata";
    public static final String API_KEY_ROLE_DESCRIPTORS_KEY = "_security_api_key_role_descriptors";
    public static final String API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY = "_security_api_key_limited_by_role_descriptors";
    public static final String MANAGED_BY_KEY = "_security_managed_by";

    public static final String ANONYMOUS_REALM_NAME = "__anonymous";
    public static final String ANONYMOUS_REALM_TYPE = "__anonymous";

    public static final String FALLBACK_REALM_NAME = "__fallback";
    public static final String FALLBACK_REALM_TYPE = "__fallback";

    public static final String ATTACH_REALM_NAME = "__attach";
    public static final String ATTACH_REALM_TYPE = "__attach";

    public static final String CROSS_CLUSTER_ACCESS_REALM_NAME = "_es_cross_cluster_access";
    public static final String CROSS_CLUSTER_ACCESS_REALM_TYPE = "_es_cross_cluster_access";
    public static final String CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY = "_security_cross_cluster_access_authentication";
    public static final String CROSS_CLUSTER_ACCESS_ROLE_DESCRIPTORS_KEY = "_security_cross_cluster_access_role_descriptors";

    private AuthenticationField() {}
}
