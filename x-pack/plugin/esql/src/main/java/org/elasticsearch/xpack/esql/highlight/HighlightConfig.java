/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.highlight;

import org.elasticsearch.xpack.esql.plan.logical.HighlightOptions;

/**
 * Compute-side configuration handed straight to {@link HighlightOperator}.
 * The planner folds the user-facing inputs into this record, and the operator builds its Lucene
 * machinery from it.
 */
public record HighlightConfig(String queryText, HighlightOptions options) {}
