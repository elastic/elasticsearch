/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.elasticsearch.core.Strings;

class FixedSizeScratch {
    private final int size;
    private byte[] scratch;

    FixedSizeScratch(int size) {
        this.size = size;
    }

    byte[] getScratch(int len) {
        if (len != size) {
            throw new IllegalArgumentException(Strings.format("Illegal size: FixedSizeScratch has a size of %d, requested %d", size, len));
        }
        if (scratch == null) {
            scratch = new byte[size];
        }
        return scratch;
    }
}
