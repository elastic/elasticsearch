/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.ElementType;

import java.util.List;

/** Intermediate aggregation state descriptor. Intermediate state is a list of these. */
public record IntermediateStateDesc(String name, ElementType type) {

    public static final IntermediateStateDesc SINGLE_UNKNOWN = new IntermediateStateDesc("aggstate", ElementType.UNKNOWN);

    public static final List<IntermediateStateDesc> AGG_STATE = List.of(SINGLE_UNKNOWN);

}
