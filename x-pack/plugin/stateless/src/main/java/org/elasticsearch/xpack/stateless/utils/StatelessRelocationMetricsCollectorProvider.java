/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.utils;

import org.elasticsearch.xpack.stateless.recovery.metering.StatelessRelocationMetricsCollector;

/// Carrier that lets Guice inject an optionally-present [StatelessRelocationMetricsCollector] into transport actions that are instantiated
/// on every node, such as [org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction].
///
/// [StatelessRelocationMetricsCollector] exists only on index nodes; search nodes do not have one.
/// Because Guice cannot express optional bindings, this class acts as an indirection: the plugin
/// registers it with a `null` collector on search nodes, and call sites unwrap the value via [#get()].
public class StatelessRelocationMetricsCollectorProvider {

    private final StatelessRelocationMetricsCollector instance;

    public StatelessRelocationMetricsCollectorProvider(final StatelessRelocationMetricsCollector instance) {
        this.instance = instance;
    }

    public StatelessRelocationMetricsCollector get() {
        assert instance != null : "relocation metrics collector must be initialized for index nodes";
        return instance;
    }
}
