/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;

/**
 * Temporary Path B pilot wiring: shared side-channel state between {@code TopNOperator}
 * and {@code LuceneSourceOperator} for a single datetime sort key over Lucene.
 */
public record LuceneMinCompetitivePilot(SharedMinCompetitive.Supplier supplier, String sortFieldName) {}
