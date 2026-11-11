/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.core.Nullable;

/**
 * The path one query takes through the fixed usage tree: who sent it, which part of that caller sent it, and the source command
 * it starts with. The flat {@code queries}, {@code kibana_features}, {@code direct_clients} and {@code leading_commands}
 * counters each answer one of these on its own; counting every path also answers the combinations, such as how many Discover
 * queries start with {@code FROM}.
 *
 * @param source the {@link KibanaExecutionContextClassifier.Feature} for Kibana, the {@link DirectClient} for queries without an
 *               Elastic product origin, and {@link #NO_SOURCE} for every other caller
 */
public record QueryPath(QueryMetric caller, String source, LeadingCommand leadingCommand) {

    public static final String USAGE_KEY = "query_paths";
    public static final String NO_SOURCE = "_none";
    public static final String CALLER = "caller";
    public static final String SOURCE = "source";
    public static final String LEADING_COMMAND = "leading_command";
    public static final String TOTAL = "total";
    public static final String FAILED = "failed";
    public static final String COMMANDS = "commands";

    /**
     * The path of a query from {@code caller}. {@code directClient} is only read for {@link QueryMetric#REST} queries.
     */
    public static QueryPath of(QueryCaller caller, @Nullable DirectClient directClient, LeadingCommand leadingCommand) {
        String source = switch (caller.client()) {
            case KIBANA -> caller.kibanaFeature() == null ? NO_SOURCE : caller.kibanaFeature().toString();
            case REST -> directClient == null ? NO_SOURCE : directClient.toString();
            case FLEET, LOGSTASH, OBSERVABILITY, CONNECTORS, ELASTIC_SYNTHETICS, OTHER -> NO_SOURCE;
        };
        return new QueryPath(caller.client(), source, leadingCommand);
    }
}
