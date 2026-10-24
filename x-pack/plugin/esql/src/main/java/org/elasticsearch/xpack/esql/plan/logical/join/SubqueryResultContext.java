/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Resources used when a join consumes its materialized sub-plan result.
 */
public record SubqueryResultContext(
    int inSubqueryHashJoinThreshold,
    ByteSizeValue intermediateLocalRelationMaxSize,
    BlockFactory blockFactory,
    @Nullable AtomicReference<Page> pageHolder
) {}
