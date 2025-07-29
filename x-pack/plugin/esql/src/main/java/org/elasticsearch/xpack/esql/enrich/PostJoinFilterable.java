/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.xpack.esql.plan.physical.FilterExec;

/**
 * An interface for a join operator that needs to have a filter applied after the join happens
 * For now we use this for applying filters that are not translatable after a lookup join
 */
public interface PostJoinFilterable {
    FilterExec getPostJoinFilter();
}
