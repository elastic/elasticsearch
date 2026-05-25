/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

/**
 * Thrown by a {@link RoutingExtractor} during {@link org.elasticsearch.eirf.EirfEncoder#parseToScratch}
 * when the extractor encounters input it cannot turn into a routing decision (today: an array at a
 * matched routing column). Callers — typically
 * {@link org.elasticsearch.action.bulk.BulkOperation} via
 * {@code BulkBatchEncoders.tryEncodeAndRoute} — catch this, log, and abandon EIRF encoding for the
 * remainder of the bulk so every item is routed through the inline-source path.
 */
public final class RoutingExtractionException extends RuntimeException {
    public RoutingExtractionException(String message) {
        super(message);
    }
}
