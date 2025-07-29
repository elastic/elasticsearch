/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

/**
 * This exception is used to track validation errors thrown during the construction
 * of entitlements. By using this instead of other exception types the policy
 * parser is able to wrap this exception with a line/character number for
 * additional useful error information.
 */
public class PolicyValidationException extends RuntimeException {

    public PolicyValidationException(String message) {
        super(message);
    }

    public PolicyValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
