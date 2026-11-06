/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;

/** Describes how rows cross a stage boundary. */
public enum ExchangeDistribution {
    GATHER,
    HASH,
    /** Copies each producer page to every consumer lane. */
    BROADCAST;

    /** Transport version that introduced multi-producer stages and broadcast shuffle joins. */
    public static final TransportVersion ESQL_SHUFFLE_STAGE_EXECUTION = TransportVersion.fromName("esql_shuffle_stage_execution");
}
