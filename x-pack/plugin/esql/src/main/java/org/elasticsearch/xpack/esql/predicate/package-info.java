/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Source-independent predicate execution.
 * <p>
 * A source adapter owns candidate production, pruning, and column access. The engine owns exact expression evaluation,
 * three-valued Boolean composition, and collection reduction. A filtering cursor may return extra rows and must include
 * every row that could satisfy the whole predicate. A guarantee about one comparison does not become a guarantee about
 * the plan that contains it. The caller closes each loaded column before releasing its batch.
 * <p>
 * These types stay a prototype. Production {@code NESTED_ANY} already has this shape:
 * {@code NestedChildren} emits a {@code Page} whose first block is the parent position and whose later blocks are the
 * child fields, the child ids are compacted before that page is allocated, and the residual runs on the expression
 * evaluator {@code FilterExec} uses. {@code NestedPredicateEvaluator} ORs those results per parent and stops reading a
 * parent once a page has matched. A later source produces its own page and uses that evaluator.
 */
package org.elasticsearch.xpack.esql.predicate;
