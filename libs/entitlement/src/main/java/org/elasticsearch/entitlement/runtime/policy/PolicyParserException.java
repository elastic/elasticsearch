/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.xcontent.XContentLocation;

/**
 * An exception specifically for policy parsing errors.
 */
public class PolicyParserException extends RuntimeException {

    public static PolicyParserException newPolicyParserException(XContentLocation location, String policyName, String message) {
        return new PolicyParserException(
            "[" + location.lineNumber() + ":" + location.columnNumber() + "] policy parsing error for [" + policyName + "]: " + message
        );
    }

    public static PolicyParserException newPolicyParserException(
        XContentLocation location,
        String policyName,
        String scopeName,
        String message
    ) {
        if (scopeName == null) {
            return new PolicyParserException(
                "[" + location.lineNumber() + ":" + location.columnNumber() + "] policy parsing error for [" + policyName + "]: " + message
            );
        } else {
            return new PolicyParserException(
                "["
                    + location.lineNumber()
                    + ":"
                    + location.columnNumber()
                    + "] policy parsing error for ["
                    + policyName
                    + "] in scope ["
                    + scopeName
                    + "]: "
                    + message
            );
        }
    }

    public static PolicyParserException newPolicyParserException(
        XContentLocation location,
        String policyName,
        String scopeName,
        String entitlementType,
        String message
    ) {
        if (scopeName == null) {
            return new PolicyParserException(
                "["
                    + location.lineNumber()
                    + ":"
                    + location.columnNumber()
                    + "] policy parsing error for ["
                    + policyName
                    + "] for entitlement type ["
                    + entitlementType
                    + "]: "
                    + message
            );
        } else {
            return new PolicyParserException(
                "["
                    + location.lineNumber()
                    + ":"
                    + location.columnNumber()
                    + "] policy parsing error for ["
                    + policyName
                    + "] in scope ["
                    + scopeName
                    + "] for entitlement type ["
                    + entitlementType
                    + "]: "
                    + message
            );
        }
    }

    public static PolicyParserException newPolicyParserException(
        XContentLocation location,
        String policyName,
        String scopeName,
        String entitlementType,
        PolicyValidationException cause
    ) {
        assert (scopeName != null);
        return new PolicyParserException(
            "["
                + location.lineNumber()
                + ":"
                + location.columnNumber()
                + "] policy parsing error for ["
                + policyName
                + "] in scope ["
                + scopeName
                + "] for entitlement type ["
                + entitlementType
                + "]: "
                + cause.getMessage(),
            cause
        );
    }

    private PolicyParserException(String message) {
        super(message);
    }

    private PolicyParserException(String message, PolicyValidationException cause) {
        super(message, cause);
    }
}
