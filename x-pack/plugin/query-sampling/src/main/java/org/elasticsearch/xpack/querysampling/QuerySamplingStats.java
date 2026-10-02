/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

/**
 * Where the searches seen by one node ended up, stage by stage. Every number only counts what happened on
 * this node since it started.
 *
 * @param knnSearches        eligible kNN searches the capture gate looked at
 * @param captured           of those, searches the gate picked
 * @param dropped            captures lost because the hand-off queue was full
 * @param distinctQueries    distinct queries being counted
 * @param untrackedArrivals  arrivals of queries that could not be counted because the counter was full
 * @param picked             distinct queries picked for the sample
 * @param buffered           picked queries currently held in Tier 1
 * @param rejected           picked queries turned away because Tier 1 was full
 */
public record QuerySamplingStats(
    long knnSearches,
    long captured,
    long dropped,
    long distinctQueries,
    long untrackedArrivals,
    long picked,
    long buffered,
    long rejected
) {}
