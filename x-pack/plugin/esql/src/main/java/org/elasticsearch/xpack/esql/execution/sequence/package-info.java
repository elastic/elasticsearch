/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Correlates event-preserving ES|QL stages with bounded latest-valid backward search. The coordinator source
 * manages matching state, typed stage payloads, source paging, output pruning and cancellation.
 */
package org.elasticsearch.xpack.esql.execution.sequence;
