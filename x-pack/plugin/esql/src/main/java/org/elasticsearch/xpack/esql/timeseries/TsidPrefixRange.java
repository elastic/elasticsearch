/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.timeseries;

/**
 * The {@code _tsid} prefixes {@code lower} (inclusive) to {@code upper} (exclusive), and {@code prefixBits} wide.
 */
public record TsidPrefixRange(int prefixBits, int lower, int upper) {}
