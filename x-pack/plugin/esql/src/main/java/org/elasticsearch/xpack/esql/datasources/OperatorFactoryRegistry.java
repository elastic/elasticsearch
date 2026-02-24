/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorFactoryProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Registry for source operator factories.
 *
 * <p>This registry provides a single entry point for creating source operator factories.
 * It supports two modes:
 * <ol>
 *   <li><b>ExternalSourceFactory</b>: Unified factories for connectors, table catalogs,
 *       and file-based sources. ConnectorFactory instances are dispatched via the connector
 *       protocol; file-based sources go through {@code FileSourceFactory.operatorFactory()};
 *       other factories use their {@code operatorFactory()} capability.</li>
 *   <li><b>Plugin factories</b>: Backward-compat bridge for {@code DataSourcePlugin.operatorFactories()}.</li>
 * </ol>
 */
public class OperatorFactoryRegistry {

    private final Map<String, ExternalSourceFactory> sourceFactories;
    // FIXME: pluginFactories is a backward-compat bridge for DataSourcePlugin.operatorFactories().
    // Once plugins migrate to ExternalSourceFactory.operatorFactory(), remove this field.
    private final Map<String, SourceOperatorFactoryProvider> pluginFactories;
    private final Executor executor;

    public OperatorFactoryRegistry(
        Map<String, ExternalSourceFactory> sourceFactories,
        Map<String, SourceOperatorFactoryProvider> pluginFactories,
        Executor executor
    ) {
        if (executor == null) {
            throw new IllegalArgumentException("executor cannot be null");
        }
        this.sourceFactories = sourceFactories != null ? Map.copyOf(sourceFactories) : Map.of();
        this.pluginFactories = pluginFactories != null ? Map.copyOf(pluginFactories) : Map.of();
        this.executor = executor;
    }

    public SourceOperator.SourceOperatorFactory factory(SourceOperatorContext context) {
        String sourceType = context.sourceType();

        ExternalSourceFactory sf = sourceType != null ? sourceFactories.get(sourceType) : null;
        if (sf != null) {
            if (sf instanceof ConnectorFactory cf) {
                Connector connector = cf.open(context.config());
                List<String> projectedColumns = new ArrayList<>(context.attributes().size());
                for (Attribute attr : context.attributes()) {
                    projectedColumns.add(attr.name());
                }
                // Use the target from resolved config (e.g. path component for Flight URIs);
                // fall back to the full path string for connectors that don't set it.
                Object targetObj = context.config().get("target");
                String target = targetObj != null ? targetObj.toString() : context.path().toString();
                QueryRequest request = new QueryRequest(
                    target,
                    projectedColumns,
                    context.attributes(),
                    context.config(),
                    context.batchSize(),
                    null
                );
                return new AsyncConnectorSourceOperatorFactory(connector, request, context.maxBufferSize(), executor);
            }
            SourceOperatorFactoryProvider opFactory = sf.operatorFactory();
            if (opFactory != null) {
                return opFactory.create(context);
            }
        }

        // FIXME: backward-compat bridge for standalone pluginFactories
        if (sourceType != null && pluginFactories.containsKey(sourceType)) {
            return pluginFactories.get(sourceType).create(context);
        }

        throw new IllegalArgumentException("No operator factory for sourceType: " + sourceType);
    }

    public boolean hasPluginFactory(String sourceType) {
        return sourceType != null && (sourceFactories.containsKey(sourceType) || pluginFactories.containsKey(sourceType));
    }

    public Executor executor() {
        return executor;
    }
}
