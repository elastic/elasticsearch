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
 * three-valued Boolean composition, and collection reduction. A source may drop a row only when it can prove that row
 * cannot satisfy the whole predicate. A guarantee about one comparison does not become a guarantee about the plan that
 * contains it.
 */
package org.elasticsearch.xpack.esql.predicate;
