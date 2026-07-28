/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.iplocation.api;

/**
 * Identifies the type of consumer requesting IP database downloads.
 * The {@link IpLocationService} implementation uses this to determine which
 * nodes are relevant for downloading databases (e.g., ingest consumers
 * trigger downloads only on ingest-capable nodes) and to scope cancellations
 * so that one consumer's cancel does not affect another consumer's downloads
 * for the same project.
 */
public enum IpLocationConsumer {
    /**
     * The ingest pipeline consumer. Databases are needed on ingest-capable nodes.
     */
    INGEST,

    /**
     * The ES|QL query consumer. Databases are needed on data-containing nodes (which run the
     * data-node portion of an ES|QL physical plan) and on dedicated coordinating-only nodes
     * (which may run the coordinator portion of a plan that includes an IP-location lookup
     * above the exchange — e.g. an EVAL after a STATS, or a ROW-based query).
     */
    ESQL
}
