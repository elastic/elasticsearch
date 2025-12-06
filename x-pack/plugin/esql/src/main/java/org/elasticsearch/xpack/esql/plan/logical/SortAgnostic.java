/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

/**
 * This interface is intended to check redundancy of a previous SORT.
 *<p>
 *
 * An example is with commands that compute values record by record, regardless of the input order
 * and that don't rely on the context (intended as previous/next records).
 *
 * <hr>
 * <p>
 *
 * Example 1: if a MY_COMMAND that implements this interface is used between two sorts,
 * then we can assume that
 * <p>
 * <code>
 * | SORT x, y, z | MY_COMMAND | SORT a, b, c
 * </code>
 * <p>
 * is equivalent to
 * <p>
 * <code>
 * | MY_COMMAND | SORT a, b, c
 * </code>
 *
 * <hr>
 * <p>
 *
 * Example 2: commands that make previous order irrelevant, eg. because they collapse the results;
 * STATS is one of them, eg.
 *
 * <p>
 * <code>
 * | SORT x, y, z | STATS count(*)
 * </code>
 * <p>
 * is equivalent to
 * <p>
 * <code>
 * | STATS count(*)
 * </code>
 * <p>
 *
 * and if MY_COMMAND implements this interface, then
 *
 * <p>
 * <code>
 * | SORT x, y, z | MY_COMMAND | STATS count(*)
 * </code>
 * <p>
 * is equivalent to
 * <p>
 * <code>
 * | MY_COMMAND | STATS count(*)
 * </code>
 *
 * <hr>
 * <p>
 *
 * In all the other cases, eg. if the command does not implement this interface
 * then we assume that the previous SORT is still relevant and cannot be pruned.
 *
 * <hr>
 * <p>
 *
 * Eg. LIMIT does <b>not</b> implement this interface, because
 *
 * <p>
 * <code>
 * | SORT x, y, z | LIMIT 10 | SORT a, b, c
 * </code>
 * <p>
 * is <b>NOT</b> equivalent to
 * <p>
 * <code>
 * | LIMIT 10 | SORT a, b, c
 * </code>
 *
 * <hr>
 * <p>
 *
 * For n-ary plans that implement this interface,
 * we assume that the above applies to all the children
 *
 */
public interface SortAgnostic {}
