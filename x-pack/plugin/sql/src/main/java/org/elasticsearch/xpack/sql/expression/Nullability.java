/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

public enum Nullability {
    TRUE((byte) 1),    // Whether the expression can become null
    FALSE((byte) 3),   // The expression can never become null
    UNKNOWN((byte) 0); // Cannot determine if the expression supports possible null folding

    private final byte bitmask;

    Nullability(byte bitmask) {
        this.bitmask = bitmask;
    }

    public static Nullability and(Nullability... nullabilities) {
        if (nullabilities.length == 0) {
            return UNKNOWN;
        }

        // UKNOWN AND <anything> => UKNOWN
        // FALSE AND FALSE => FALSE
        // POSSIBLE AND FALSE/POSSIBLE => POSSIBLE
        byte bitmask = nullabilities[0].bitmask;
        for (int i = 1; i < nullabilities.length; i++) {
            bitmask &= nullabilities[i].bitmask;
        }
        return fromBitmask(bitmask);
    }

    private static Nullability fromBitmask(byte bitmask) {
        switch (bitmask) {
            case 0:
                return UNKNOWN;
            case 1:
                return TRUE;
            case 3:
                return FALSE;
            default:
                throw new SqlIllegalArgumentException(
                    "[{}] is not a valud bitmask value for Nullability, this is likely a bug", bitmask);
        }
    }
}
