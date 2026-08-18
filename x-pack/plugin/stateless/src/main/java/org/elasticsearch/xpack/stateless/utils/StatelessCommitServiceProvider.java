/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.utils;

import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;

/// Carrier that lets Guice inject an optionally-present [StatelessCommitService] into transport actions that are instantiated on every
/// node, such as [org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction].
///
/// [StatelessCommitService] exists only on index nodes; search nodes do not have one.
/// Because Guice cannot express optional bindings, this class acts as an indirection: the plugin
/// registers it with a `null` commit service on search nodes, and call sites unwrap the value via [#get()].
public class StatelessCommitServiceProvider {

    private final StatelessCommitService instance;

    public StatelessCommitServiceProvider(final StatelessCommitService instance) {
        this.instance = instance;
    }

    /// The commit service should never be null on index nodes, but may be null on search nodes where should not be called.
    public StatelessCommitService get() {
        assert instance != null : "commit service must be initialized on index nodes";
        return instance;
    }
}
