/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

public record Column(String type, Block values) implements Releasable {
    @Override
    public void close() {
        Releasables.closeExpectNoException(values);
    }
}
