/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform;

import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * A type of {@link RestTestTransform} that finds the transformation by a given key that has a value that is an {@link ArrayNode}.
 */
public interface RestTestTransformByParentArray extends RestTestTransform<ArrayNode> {

    /**
     * Arrays are always the value in a key/value pair. Find a key with this name that has an array as the value to identify which Array(s)
     * to transform.
     * @return The name of key to find in the REST test that has a value that is an Array
     */
    String getKeyOfArrayToFind();
}
