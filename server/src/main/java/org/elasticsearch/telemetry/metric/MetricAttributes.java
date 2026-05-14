/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.metric;

import java.util.Set;

public interface MetricAttributes {
    /** Node id of the metered node (as opposed to the metering node). */
    String ES_NODE_ID = "es_node_id";

    /** Node id of the metered node (as opposed to the metering node in {@code service.node.name}). */
    String ES_NODE_NAME = "es_node_name";

    /** Node tier of the metered node (as opposed to the metering node in {@code labels.node_tier}). */
    String ES_NODE_TIER = "es_node_tier";

    // refers to https://opentelemetry.io/docs/specs/semconv/registry/attributes/error/#error-type
    String ERROR_TYPE = "error_type";

    /** The version of the stack. */
    String ES_STACK_VERSION = "es_stack_version";

    /** Indicates whether the node is a production release (i.e. not a snapshot, alpha, etc.) */
    String ES_PRODUCTION_RELEASE = "es_production_release";

    /**
     * This should be used to hold the {@code X-Elastic-Product-Origin} header value.
     * <p>
     * <b>NOTE: The {@code X-Elastic-Product-Origin} header could have unbounded cardinality.</b>
     */
    String ES_PRODUCT_ORIGIN = "es_product_origin";

    /**
     * Known values recorded for {@link #ES_PRODUCT_ORIGIN}. Entries reflect known producers of the {@code X-Elastic-Product-Origin}
     * header across the elastic org (Kibana, Logstash, Fleet, Beats, Connectors, etc.).
     * <p>
     * <b>Entries must be lowercase.</b>
     */
    Set<String> KNOWN_PRODUCT_ORIGINS = Set.of(
        // https://github.com/elastic/kibana/blob/main/src/platform/packages/shared/kbn-telemetry/src/init_autoinstrumentations.ts#L33
        "kibana",
        // https://github.com/elastic/fleet-server/blob/main/internal/pkg/config/output.go#L161
        "fleet",
        // https://github.com/elastic/logstash/blob/main/x-pack/lib/template.cfg.erb#L70
        "logstash",
        // https://github.com/elastic/apm-server/blob/main/internal/kibana/client.go#L47
        "observability",
        // https://github.com/elastic/connectors/blob/main/app/connectors_service/connectors/es/client.py#L112
        "connectors",
        // Elastic Inference Service synthetics tests
        "elastic-synthetics"
    );

}
