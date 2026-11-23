/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/**
 * A row's identity inside one {@link RowSpace}. The id is not meaningful in any other space: doc id {@code 0} in two
 * Lucene segments is two rows.
 */
public record SourceRow(RowSpace space, int id) {}
