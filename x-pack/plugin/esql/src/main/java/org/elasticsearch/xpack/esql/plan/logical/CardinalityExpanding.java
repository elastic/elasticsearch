/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

/**
 * This interface marks a command which can increase the number of rows. Examples of such commands:
 * - `MV_EXPAND`
 * - Joins
 */
public interface CardinalityExpanding {}
