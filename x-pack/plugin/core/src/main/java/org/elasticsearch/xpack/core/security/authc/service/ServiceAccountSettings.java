/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.service;

public final class ServiceAccountSettings {

    public static final String REALM_TYPE = "_service_account";
    public static final String REALM_NAME = "_service_account";
    public static final String TOKEN_NAME_FIELD = "_token_name";
    public static final String TOKEN_SOURCE_FIELD = "_token_source";
    public static final String BUILTIN_SERVICE_ACCOUNT_FIELD = "_elastic_service_account";
    public static final String MANAGED_SERVICE_ACCOUNT_FIELD = "_managed_service_account";

    private ServiceAccountSettings() {}
}
