/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.metrics;

public interface MetricAttributes {
    /** Node id of the metered node (as opposed to the metering node). */
    String ES_NODE_ID = "es_node_id";

    /** Node id of the metered node (as opposed to the metering node in {@code service.node.name}). */
    String ES_NODE_NAME = "es_node_name";

    /** Node tier of the metered node (as opposed to the metering node in {@code labels.node_tier}). */
    String ES_NODE_TIER = "es_node_tier";

    // refers to https://opentelemetry.io/docs/specs/semconv/registry/attributes/error/#error-type
    String ERROR_TYPE = "error_type";
}
