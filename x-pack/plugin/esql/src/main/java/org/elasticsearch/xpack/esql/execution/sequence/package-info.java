/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Correlates event-preserving ES|QL stages using EQL's positive-sequence selection. Compact matching state is kept
 * separate from typed payload retrieval so source paging, output pruning and cancellation retain their ES|QL contracts.
 */
package org.elasticsearch.xpack.esql.execution.sequence;
