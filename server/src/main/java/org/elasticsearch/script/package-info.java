/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * Support for running user provided scripts (in the request, in cluster state, etc) in portions of various requests
 * ({@link org.elasticsearch.common.lucene.search.function.FunctionScoreQuery}, {@link org.elasticsearch.search.aggregations.Aggregation},
 * {@link org.elasticsearch.action.update.UpdateAction}, etc). Pluggable via implementing {@link org.elasticsearch.plugins.ScriptPlugin}.
 */
package org.elasticsearch.script;
