/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.do_; // 'do' is a reserved word

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.gradle.internal.test.rest.transform.ReplaceByKey;
import org.gradle.api.tasks.Internal;

/**
 * A transformation to replace the catch value in a do. For example, change from "do": { catch: foo } to "do":{ catch : bar}
 */
public class ReplaceValueInCatch extends ReplaceByKey {

    private final JsonNode newValue;

    public ReplaceValueInCatch(JsonNode newValue, String testName) {
        super("catch", "catch", null, testName);
        this.newValue = newValue;
    }

    @Override
    @Internal
    public String getKeyToFind() {
        return "do";
    }

    @Override
    public void transformTest(ObjectNode doParent) {
        ObjectNode doNode = (ObjectNode) doParent.get(getKeyToFind());
        doNode.set(requiredChildKey(), newValue);
    }
}
