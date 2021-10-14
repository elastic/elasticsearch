/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

public class ApiKeyServiceField {
    public static final String API_KEY_REALM_NAME = "_es_api_key";
    public static final String API_KEY_REALM_TYPE = "_es_api_key";

    public static final String API_KEY_ID_KEY = "_security_api_key_id";
    public static final String API_KEY_NAME_KEY = "_security_api_key_name";
    public static final String API_KEY_METADATA_KEY = "_security_api_key_metadata";
    public static final String API_KEY_ROLE_DESCRIPTORS_KEY = "_security_api_key_role_descriptors";
    public static final String API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY = "_security_api_key_limited_by_role_descriptors";
    public static final String API_KEY_CREATOR_REALM_NAME = "_security_api_key_creator_realm_name";
    public static final String API_KEY_CREATOR_REALM_TYPE = "_security_api_key_creator_realm_type";

    private ApiKeyServiceField() {
    }
}
