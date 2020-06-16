/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.authc;

/**
 * Denotes network based controls that were applied during authentication of a user
 */
public enum NetworkControl {
    IP_FILTER,
    TLS,
}
