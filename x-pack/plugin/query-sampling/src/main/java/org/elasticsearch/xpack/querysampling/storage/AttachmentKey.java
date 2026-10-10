/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

/**
 * Names a kind of payload that a use of the sample can attach to a {@link SampledQuery}, such as the
 * ground truth for recall estimation. Keys are compared by identity, so each use declares its key once as
 * a constant and nobody else can clash with it by picking the same name.
 *
 * @param name for diagnostics only
 * @param type the type of the payload
 */
public record AttachmentKey<T>(String name, Class<T> type) {

    @Override
    public boolean equals(Object other) {
        return this == other;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }
}
