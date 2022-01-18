/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform;

import com.fasterxml.jackson.databind.JsonNode;

import org.gradle.api.Named;
import org.gradle.api.tasks.Input;

/**
 * A single instruction to transforms a REST test.
 */
public interface RestTestTransform<T extends JsonNode> extends Named {

    /**
     * Transform the Json structure per the given {@link RestTestTransform}
     * Implementations are expected to mutate the parent to satisfy the transformation.
     *
     * @param parent The parent of the node to transform.
     */
    void transformTest(T parent);

    /**
     * @return true if the transformation should be applied, false otherwise.
     */
    default boolean shouldApply(RestTestContext testContext) {
        return true;
    }

    @Override
    @Input
    default String getName() {
        return this.getClass().getCanonicalName();
    }
}
