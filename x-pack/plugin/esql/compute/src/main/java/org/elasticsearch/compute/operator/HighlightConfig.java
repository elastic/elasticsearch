/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

/**
 * Compute-side configuration handed straight to {@link HighlightOperator}.
 * The planner resolves the user-facing {@code WITH { ... }} options into the primitive values carried here, and the
 * operator builds its Lucene machinery from them. Keeping this record in the compute module (rather than referencing
 * the ES|QL planning-layer options type) means the operator depends only on plain primitives.
 */
public record HighlightConfig(
    String queryText,
    String preTag,
    String postTag,
    String encoder,
    int numberOfFragments,
    int fragmentSize,
    int noMatchSize
) {

    /** Encoder name that escapes HTML markup in the highlighted text; any other value uses the default (no escaping). */
    public static final String HTML_ENCODER = "html";
}
