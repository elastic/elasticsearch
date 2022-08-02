/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.match;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.gradle.internal.test.rest.transform.ReplaceByKey;
import org.gradle.api.tasks.Internal;

/**
 * A transformation to replace the value of a match. For example, change from "match":{"_type": "foo"} to "match":{"_type": "bar"}
 */
public class ReplaceValueInMatch extends ReplaceByKey {

    public ReplaceValueInMatch(String replaceKey, JsonNode replacementNode) {
        this(replaceKey, replacementNode, null);
    }

    public ReplaceValueInMatch(String replaceKey, JsonNode replacementNode, String testName) {
        super(replaceKey, replaceKey, replacementNode, testName);
    }

    @Override
    @Internal
    public String getKeyToFind() {
        return "match";
    }

    @Override
    public void transformTest(ObjectNode matchParent) {
        ObjectNode matchNode = (ObjectNode) matchParent.get(getKeyToFind());
        matchNode.remove(requiredChildKey());
        matchNode.set(getNewChildKey(), getReplacementNode());
    }
}
