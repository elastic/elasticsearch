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

    /**
     * The namespace of the built-in service accounts that ship with Elasticsearch. It is reserved: user-managed
     * accounts may not be created in it, so a principal's namespace alone determines which kind of account it names.
     */
    public static final String BUILTIN_NAMESPACE = "elastic";

    /**
     * Marks the {@link org.elasticsearch.xpack.core.security.user.User#metadata()} of an authenticated built-in
     * service account. Surfaced verbatim by the {@code _authenticate} API, so the key is part of the wire contract.
     */
    public static final String BUILTIN_SERVICE_ACCOUNT_FIELD = "_elastic_service_account";

    /**
     * The user-managed counterpart of {@link #BUILTIN_SERVICE_ACCOUNT_FIELD}. Authorization reads it to decide
     * whether to resolve the account's privileges from an inline role descriptor or from named roles, so it must be
     * set for every user-managed account. Also surfaced by {@code _authenticate}.
     */
    public static final String USER_MANAGED_SERVICE_ACCOUNT_FIELD = "_user_managed_service_account";

    private ServiceAccountSettings() {}
}
