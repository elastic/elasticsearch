/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

public record BBQEncoding(byte dataBits, byte queryBits) {

    public BBQEncoding {
        if (dataBits < 1 || dataBits > 8) {
            throw new IllegalArgumentException("dataBits must be between 1 and 8");
        }
        if (queryBits < 1 || queryBits > 8) {
            throw new IllegalArgumentException("queryBits must be between 1 and 8");
        }
        if (queryBits < dataBits) {
            throw new IllegalArgumentException("queryBits must be greater than or equal to dataBits");
        }
    }

    public BBQEncoding(int dataBits, int queryBits) {
        this((byte) dataBits, (byte) queryBits);
    }

    public int toSwitchValue() {
        return (dataBits << 8) | queryBits;
    }

    @Override
    public String toString() {
        return "BBQEncoding(D" + dataBits + "Q" + queryBits + ")";
    }
}
