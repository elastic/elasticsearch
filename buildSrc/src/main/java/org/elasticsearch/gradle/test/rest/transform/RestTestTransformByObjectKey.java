/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test.rest.transform;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A type of {@link RestTestTransform} that finds the transformation by a given key in to an {@link ObjectNode}.
 */
public interface RestTestTransformByObjectKey extends RestTestTransform<ObjectNode> {
    /**
     * @return The name of key to find in the REST test
     */
    String getKeyToFind();
}
