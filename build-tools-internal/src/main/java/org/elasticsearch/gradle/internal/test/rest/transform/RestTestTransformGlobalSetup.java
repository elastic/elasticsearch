/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform;

import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nullable;

/**
 * A type of {@link RestTestTransform} that transformations or adds a global "setup" section.
 */
public interface RestTestTransformGlobalSetup {

    /**
     * @param setupNodeParent The parent of an existing "setup" ObjectNode, null otherwise. If null implementations may create choose to
     *                        create the section.
     */
    ObjectNode transformSetup(@Nullable ObjectNode setupNodeParent);
}
