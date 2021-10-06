/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform;

import com.fasterxml.jackson.databind.JsonNode;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Optional;

/**
 * An abstract common class to handle replacing key and values under a parent object
 * This class can be subclass to transform the
 * "getKeyToFind": {"requiredChildKey": "foo"}
 * into
 * "getKeyToFind": {"newChildKey": "getReplacementNode"}
 * a getKeyToFind and transformTest would have to be implemented in a subclass
 */
public abstract class ReplaceByKey implements RestTestTransformByParentObject {
    private final String requiredChildKey;
    private final String newChildKey;
    private final JsonNode replacementNode;
    private final String testName;

    public ReplaceByKey(String requiredChildKey, JsonNode replacementNode) {
        this(requiredChildKey, replacementNode, null);
    }

    public ReplaceByKey(String requiredChildKey, JsonNode replacementNode, String testName) {
        this(requiredChildKey, requiredChildKey, replacementNode, testName);
    }

    public ReplaceByKey(String requiredChildKey, String newChildKey, JsonNode replacementNode, String testName) {
        this.requiredChildKey = requiredChildKey;
        this.newChildKey = newChildKey;
        this.replacementNode = replacementNode;
        this.testName = testName;
    }

    @Override
    public String requiredChildKey() {
        return requiredChildKey;
    }

    @Input
    public String getNewChildKey() {
        return newChildKey;
    }

    @Override
    public boolean shouldApply(RestTestContext testContext) {
        return testName == null || testContext.getTestName().equals(testName);
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
