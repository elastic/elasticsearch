/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

/**
 * Marker interface for vector functions. Makes possible to do implicit casting
 * from multi values to dense_vector field types, so parameters are actually
 * processed as dense_vectors in vector functions
 */
public interface VectorFunction {}
