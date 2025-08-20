/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram;

/**
 * An exponential histogram bucket represented by its index and associated bucket count.
 * @param index the index of the bucket
 * @param count the number of values in that bucket.
 */
public record IndexWithCount(long index, long count) {}
