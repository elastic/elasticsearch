/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/**
 * Values of one field for the positions of a batch. A null return is a null value. This stands in for a compute
 * {@code Block} until a source adapter binds the real loader.
 */
public interface Column {
    Object value(int position);
}
