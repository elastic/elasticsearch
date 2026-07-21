/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datatree;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * An immutable, arbitrary-precision JSON decimal, used only for decimals that are not exactly representable as a
 * 64-bit {@code double}; decimals that are representable are held as a {@link DataDouble} (see
 * {@link DataValue#of(BigDecimal)}).
 * <p>
 * The value is preserved with full precision; how a target that cannot hold it losslessly renders it is the
 * converter's concern, not the model's.
 */
public record DataDecimal(BigDecimal value) implements DataValue {
    public DataDecimal {
        Objects.requireNonNull(value, "DataDecimal value must not be null");
    }
}
