/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import java.util.Locale;

/**
 * Which edge of a decision point a cost component belongs to, so that the same term on different inputs can be told apart.
 * New values are only ever appended because the ordinal is the wire format.
 */
public enum Side {
    /** The left input of a join. */
    LEFT,
    /** The right input of a join. */
    RIGHT,
    /** The single input of a unary decision point. */
    INPUT,
    /** An exchange internal to the operator, such as between partial and final aggregation. */
    INTERMEDIATE,
    /** The edge from the decision point to its parent. */
    OUTPUT;

    /** The name used for this value in the profile output. */
    public String displayName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
