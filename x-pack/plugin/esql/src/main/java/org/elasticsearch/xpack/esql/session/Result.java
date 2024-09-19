/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/**
 * Results from running a chunk of ESQL.
 * @param schema "Schema" of the {@link Attribute}s that are produced by the {@link LogicalPlan}
 *               that was run. Each {@link Page} contains a {@link Block} of values for each
 *               attribute in this list.
 * @param pages Actual values produced by running the ESQL.
 * @param profiles {@link DriverProfile}s from all drivers that ran to produce the output. These
 *                 are quite cheap to build, so we build them for all ESQL runs, regardless of if
 *                 users have asked for them. But we only include them in the results if users ask
 *                 for them.
 */
public record Result(List<Attribute> schema, List<Page> pages, List<DriverProfile> profiles) {}
