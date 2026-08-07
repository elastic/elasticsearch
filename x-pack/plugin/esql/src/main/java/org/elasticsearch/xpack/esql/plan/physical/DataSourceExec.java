/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;

/**
 * represents a data source in the physical plan, such as an ES index or an external data source.
 * This is mainly used by EXPLAIN to identify data source nodes in the plan and replace them with empty sources for profiling.
 */
public interface DataSourceExec {

    Source source();

    List<Attribute> output();

}
