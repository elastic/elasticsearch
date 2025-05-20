/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

public interface LocalNodeSpecBuilder extends LocalSpecBuilder<LocalNodeSpecBuilder> {

    /**
     * Sets the node name. By default, nodes are named after the cluster with an incrementing suffix (ex: my-cluster-0).
     */
    LocalNodeSpecBuilder name(String name);

    /**
     * Explicitly sets that this node should not have a name in configuration.
     */
    LocalNodeSpecBuilder withoutName();
}
