/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features;

/**
 * A feature published by a node.
 */
public interface NodeFeature {
    /**
     * The id of the feature. Must be unique across features.
     */
    String id();

    /**
     * The era (ES major version) this feature was first introduced
     */
    int era();
}
