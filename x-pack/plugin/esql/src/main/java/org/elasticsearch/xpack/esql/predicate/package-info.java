/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Source-independent predicate execution.
 * <p>
 * The engine owns three-valued Boolean composition and residual expression evaluation. A source adapter owns physical
 * access: it may produce a stream of candidate rows, refine positions already in a batch, and load columns for the
 * positions that are still candidates. A source may eliminate a row only when it can prove the row cannot satisfy the
 * predicate.
 */
package org.elasticsearch.xpack.esql.predicate;
