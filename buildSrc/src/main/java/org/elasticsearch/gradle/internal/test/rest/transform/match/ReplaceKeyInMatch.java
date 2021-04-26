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
import org.gradle.api.tasks.Internal;

/**
 * A transformation to replace the key in a match. For example, change from "match":{"index._type": "foo"} to "match":{"index._doc": "foo"}
 */
public class ReplaceKeyInMatch extends ReplaceMatch {

    private final String newKeyName;

    public ReplaceKeyInMatch(String replaceKey, String newKeyName, String testName) {
        super(replaceKey, newKeyName, null, testName);
        this.newKeyName = newKeyName;
    }

    @Override
    @Internal
    public String getKeyToFind() {
        return "match";
    }

    @Override
    public void transformTest(ObjectNode matchParent) {
        ObjectNode matchNode = (ObjectNode) matchParent.get(getKeyToFind());
        JsonNode value = matchNode.get(getReplaceKey());
        matchNode.remove(getReplaceKey());
        matchNode.set(newKeyName, value);
    }
}
