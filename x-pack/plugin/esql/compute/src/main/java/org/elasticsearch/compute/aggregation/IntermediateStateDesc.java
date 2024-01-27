/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.ElementType;

/** Intermediate aggregation state descriptor. Intermediate state is a list of these. */
public record IntermediateStateDesc(String name, ElementType type) {}
