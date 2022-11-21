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

    public static final String API_KEY_REALM_NAME = "_es_api_key";
    public static final String API_KEY_REALM_TYPE = "_es_api_key";

    public static final String API_KEY_CREATOR_REALM_NAME = "_security_api_key_creator_realm_name";
    public static final String API_KEY_CREATOR_REALM_TYPE = "_security_api_key_creator_realm_type";
    public static final String API_KEY_ID_KEY = "_security_api_key_id";
    public static final String API_KEY_NAME_KEY = "_security_api_key_name";
    public static final String API_KEY_METADATA_KEY = "_security_api_key_metadata";
    public static final String API_KEY_ROLE_DESCRIPTORS_KEY = "_security_api_key_role_descriptors";
    public static final String API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY = "_security_api_key_limited_by_role_descriptors";

    public static final String ANONYMOUS_REALM_NAME = "__anonymous";
    public static final String ANONYMOUS_REALM_TYPE = "__anonymous";

    public static final String FALLBACK_REALM_NAME = "__fallback";
    public static final String FALLBACK_REALM_TYPE = "__fallback";

    public static final String ATTACH_REALM_NAME = "__attach";
    public static final String ATTACH_REALM_TYPE = "__attach";

    /* Cross Cluster Security */

    // TODO Would _remote_cluster_security_cluster_credential be a better name?
    public static final String RCS_CLUSTER_CREDENTIAL_HEADER_KEY = "_remote_access_cluster_credential";
    // TODO Would _remote_cluster_security_subject_access be a better name?
    public static final String RCS_SUBJECT_ACCESS_HEADER_KEY = "_remote_access_authentication";
    public static final String RCS_REALM_NAME = "_es_remote_cluster_security";
    public static final String RCS_REALM_TYPE = RCS_REALM_NAME;
    public static final String RCS_QC_SUBJECT_AUTHENTICATION_KEY = "_remote_cluster_security_qc_subject_authentication";
    public static final String RCS_QC_API_KEY_ID_KEY = "_remote_cluster_security_qc_api_key_id";
    public static final String RCS_FC_API_KEY_ID_KEY = "_remote_cluster_security_fc_api_key_id";
    public static final String RCS_QC_ROLE_DESCRIPTORS_SETS_KEY = "_remote_cluster_security_qc_role_descriptors_sets";
    public static final String RCS_FC_ROLE_DESCRIPTORS_SETS_KEY = "_remote_cluster_security_fc_role_descriptors_sets";

    private AuthenticationField() {}
}
