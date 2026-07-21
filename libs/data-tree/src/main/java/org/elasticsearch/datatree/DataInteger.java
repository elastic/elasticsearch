/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datatree;

import java.math.BigInteger;
import java.util.Objects;

/**
 * An immutable, arbitrary-precision JSON integer, used only for integers that do not fit in a signed 64-bit
 * {@code long}; integers that fit are held as a {@link DataLong} (see {@link DataValue#of(BigInteger)}).
 * <p>
 * The value is preserved with full precision; how a target that cannot hold it losslessly renders it is the
 * converter's concern, not the model's.
 */
public record DataInteger(BigInteger value) implements DataValue {
    public DataInteger {
        Objects.requireNonNull(value, "DataInteger value must not be null");
    }
}
