/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.length;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.gradle.internal.test.rest.transform.ReplaceByKey;
import org.gradle.api.tasks.Internal;

/**
 * A transformation to replace the value in a length assertion.
 * For example, change from "length":{"index._type": 1} to "length":{"index._type": 2}
 */
public class ReplaceValueInLength extends ReplaceByKey {

    public ReplaceValueInLength(String replaceKey, JsonNode replacementNode, String testName) {
        super(replaceKey, replaceKey, replacementNode, testName);
    }

    @Override
    @Internal
    public String getKeyToFind() {
        return "length";
    }

    @Override
    public void transformTest(ObjectNode lengthParent) {
        ObjectNode lengthNode = (ObjectNode) lengthParent.get(getKeyToFind());
        lengthNode.remove(requiredChildKey());
        lengthNode.set(getNewChildKey(), getReplacementNode());
    }
}
