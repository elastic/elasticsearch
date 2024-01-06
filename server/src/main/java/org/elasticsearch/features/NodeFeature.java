/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features;

import java.util.Objects;

/**
 * A feature published by a node.
 *
 * @param id        The feature id. Must be unique in the node.
 */
public record NodeFeature(String id) {

    public NodeFeature {
        Objects.requireNonNull(id);
    }
}
