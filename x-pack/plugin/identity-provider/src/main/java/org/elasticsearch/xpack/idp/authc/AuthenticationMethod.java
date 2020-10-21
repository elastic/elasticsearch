/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.authc;

/**
 * Denotes support authentication methods for users
 */
public enum AuthenticationMethod {
    PASSWORD,
    KERBEROS,
    TLS_CLIENT_AUTH,
    PRIOR_SESSION,
}
