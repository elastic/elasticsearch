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
 * A transformation to replace the key in a length assertion.
 * For example, change from "length":{"index._type": 1} to "length":{"index._doc": 1}
 */
public class ReplaceKeyInLength extends ReplaceByKey {

    public ReplaceKeyInLength(String replaceKey, String newKeyName, String testName) {
        super(replaceKey, newKeyName, null, testName);
    }

    @Override
    @Internal
    public String getKeyToFind() {
        return "length";
    }

    @Override
    public void transformTest(ObjectNode lengthParent) {
        ObjectNode lengthNode = (ObjectNode) lengthParent.get(getKeyToFind());
        JsonNode previousValue = lengthNode.get(requiredChildKey());
        lengthNode.remove(requiredChildKey());
        lengthNode.set(getNewChildKey(), previousValue);
    }
}
