/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.esql.spi;

/**
 * The span of the time buckets an aggregating ES|QL query groups by, as ES|QL reports it in {@code column_metadata}.
 * The unit is ES|QL's own name, such as {@code minute}, {@code hour}, {@code day} or {@code month}; calendar units are not fixed durations.
 */
public record TimeBucket(long interval, String unit) {}
