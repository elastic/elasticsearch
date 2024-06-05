/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.text;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import org.elasticsearch.gradle.internal.test.rest.transform.RestTestContext;
import org.elasticsearch.gradle.internal.test.rest.transform.RestTestTransformByParentObject;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;

/**
 * A transformation to replace a key/value combination.
 */
public class ReplaceTextual implements RestTestTransformByParentObject {
    private final String keyToReplaceName;
    private final String valueToBeReplaced;
    private final TextNode replacementNode;
    private final String testName;

    public ReplaceTextual(String keyToReplaceName, String valueToBeReplaced, TextNode replacementNode) {
        this.keyToReplaceName = keyToReplaceName;
        this.valueToBeReplaced = valueToBeReplaced;
        this.replacementNode = replacementNode;
        this.testName = null;
    }

    public ReplaceTextual(String keyToReplaceName, String valueToBeReplaced, TextNode replacementNode, String testName) {
        this.keyToReplaceName = keyToReplaceName;
        this.valueToBeReplaced = valueToBeReplaced;
        this.replacementNode = replacementNode;
        this.testName = testName;
    }

    @Override
    @Internal
    public String getKeyToFind() {
        return keyToReplaceName;
    }

    @Override
    public String requiredChildKey() {
        return valueToBeReplaced;
    }

    @Override
    public boolean shouldApply(RestTestContext testContext) {
        return testName == null || testContext.testName().equals(testName);
    }

    @Override
    public void transformTest(ObjectNode matchParent) {
        matchParent.set(getKeyToFind(), replacementNode);
    }

    @Input
    public String getValueToBeReplaced() {
        return valueToBeReplaced;
    }

    @Input
    public JsonNode getReplacementNode() {
        return replacementNode;
    }

    @Input
    @Optional
    public String getTestName() {
        return testName;
    }

    @Override
    public boolean matches(JsonNode child) {
        return child.asText().equals(requiredChildKey());
    }

}
