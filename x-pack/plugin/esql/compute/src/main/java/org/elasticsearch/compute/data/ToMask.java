/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.core.Releasable;

/**
 * Result from calling {@link BooleanBlock#toMask}.
 */
public record ToMask(BooleanVector mask, boolean hadMultivaluedFields) implements Releasable {
    @Override
    public void close() {
        mask.close();
    }
}
