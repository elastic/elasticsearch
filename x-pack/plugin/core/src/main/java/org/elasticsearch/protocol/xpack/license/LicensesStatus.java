/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.protocol.xpack.license;

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

}
