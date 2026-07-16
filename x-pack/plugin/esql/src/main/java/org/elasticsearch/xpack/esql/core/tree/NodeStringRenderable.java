/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.tree;

/**
 * A non-{@link Node} property that knows how to render itself through the {@code nodeString}
 * pipeline. The reflective property walker ({@code NodePropertiesToString}) dispatches to this
 * instead of {@code String.valueOf} so structured properties (dissect / grok patterns, index
 * maps, regex patterns) route their embedded identifiers through the {@link NodeStringMapper} on
 * the single render path — no per-node {@code nodeString} override, no {@code == IDENTITY} branch.
 * <p>
 * Contract: under {@link NodeStringMapper#IDENTITY} the appended output must equal the property's
 * plain {@code toString()}, so identity rendering (EXPLAIN, debug logs, golden fixtures) is
 * unchanged; under any other mapper the same shape is emitted with identifiers substituted. The
 * recommended way to honor the first half is to make {@code toString()} delegate here with the
 * identity mapper.
 */
public interface NodeStringRenderable {
    void nodeString(StringBuilder sb, Node.NodeStringFormat format, NodeStringMapper mapper);
}
