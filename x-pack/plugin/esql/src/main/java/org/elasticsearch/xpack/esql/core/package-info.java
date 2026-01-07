/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * ES|QL core package
 *
 * This package originated as a copy of the `esql-core` project, which was a copy of the `ql` x-pack plugin.
 * It contains some fundamental classes used in `esql`, like `Node`, its subclasses `Expression`, `QueryPlan`, and the plan optimizer code.
 * Originally, `ql` shared classes between ES|QL, SQL and EQL, but ES|QL diverged far enough to justify a split.
 *
 * ## Warning
 *
 * - **Consider the contents of this package untested.**
 *   There may be some tests in `sql` and `eql` that may have indirectly covered the initial version of this (when it was copied from `ql`);
 *   but neither do these tests apply to `esql`, nor do they even run against this.
 * - **Consider this package technical debt.**
 *   The contents of this package need to be consolidated with the `esql` plugin.
 *   In particular, there is a significant amount of code (or code paths) that are not used/executed at all in `esql`.
 */
package org.elasticsearch.xpack.esql.core;
