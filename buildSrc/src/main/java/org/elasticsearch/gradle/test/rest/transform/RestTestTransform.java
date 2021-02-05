/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test.rest.transform;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A single instruction to transforms a REST test.
 */
public interface RestTestTransform<T extends JsonNode> {

    /**
     * Transform the Json structure per the given {@link RestTestTransform}
     * Implementations are expected to mutate the node (and/or it's parent) to satisfy the transformation.
     *
     * @param node The node to transform. This may also be the logical parent of the node that should be transformed.
     */
    void transformTest(T node);
}
