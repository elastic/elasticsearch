/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.license;

import java.util.Locale;

public enum LicensesStatus {
    VALID((byte) 0),
    INVALID((byte) 1),
    EXPIRED((byte) 2);

    private final byte id;

    LicensesStatus(byte id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public static LicensesStatus fromId(int id) {
        if (id == 0) {
            return VALID;
        } else if (id == 1) {
            return INVALID;
        } else if (id == 2) {
            return EXPIRED;
        } else {
            throw new IllegalStateException("no valid LicensesStatus for id=" + id);
        }
    }


    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }

    public static LicensesStatus fromString(String value) {
        switch (value) {
            case "valid":
                return VALID;
            case "invalid":
                return INVALID;
            case "expired":
                return EXPIRED;
            default:
                throw new IllegalArgumentException("unknown licenses status [" + value + "]");
        }
    }
}
