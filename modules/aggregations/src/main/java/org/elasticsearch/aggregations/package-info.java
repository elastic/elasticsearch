/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * <h2>Aggregations</h2>
 * This package contains all common aggregations that should be available in the default distribution.
 * There are three types of aggregations and each type has its own subpackage:
 * <ul>
 *     <li>Bucket aggregations - which group documents (e.g. a term or histogram)</li>
 *     <li>Metric aggregations - which compute a summary value from several
 *     documents (e.g. a sum)</li>
 *     <li>Pipeline aggregations - which run as a separate step and compute
 *     values across buckets</li>
 * </ul>
 */
package org.elasticsearch.aggregations;
