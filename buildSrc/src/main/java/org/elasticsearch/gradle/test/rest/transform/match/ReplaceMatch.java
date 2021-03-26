/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test.rest.transform.match;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.gradle.test.rest.transform.RestTestContext;
import org.elasticsearch.gradle.test.rest.transform.RestTestTransformByParentObject;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;

/**
 * A transformation to replace the value of a match. For example, change from "match":{"_type": "foo"} to "match":{"_type": "bar"}
 */
public class ReplaceMatch implements RestTestTransformByParentObject {
    private final String replaceKey;
    private final String newKeyName;
    private final JsonNode replacementNode;
    private final String testName;

    public ReplaceMatch(String replaceKey, JsonNode replacementNode) {
        this(replaceKey, replacementNode, null);
    }

    public ReplaceMatch(String replaceKey, JsonNode replacementNode, String testName) {
        this(replaceKey, replaceKey, replacementNode, testName);
    }

    public ReplaceMatch(String replaceKey, String newKeyName, JsonNode replacementNode, String testName) {
        this.replaceKey = replaceKey;
        this.newKeyName = newKeyName;
        this.replacementNode = replacementNode;
        this.testName = testName;
    }

    @Override
    @Internal
    public String getKeyToFind() {
        return "match";
    }

    @Override
    public String requiredChildKey() {
        return replaceKey;
    }

    @Override
    public boolean shouldApply(RestTestContext testContext) {
        return testName == null || testContext.getTestName().equals(testName);
    }

    @Override
    public void transformTest(ObjectNode matchParent) {
        ObjectNode matchNode = (ObjectNode) matchParent.get(getKeyToFind());
        matchNode.remove(replaceKey);
        matchNode.set(newKeyName, replacementNode);
    }

    @Input
    public String getReplaceKey() {
        return replaceKey;
    }

    @Input
    @Optional
    public JsonNode getReplacementNode() {
        return replacementNode;
    }

    @Input
    @Optional
    public String getTestName() {
        return testName;
    }
}
