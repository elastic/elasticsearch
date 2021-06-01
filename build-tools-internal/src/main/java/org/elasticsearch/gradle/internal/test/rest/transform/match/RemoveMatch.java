/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.match;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.gradle.internal.test.rest.transform.RestTestContext;
import org.elasticsearch.gradle.internal.test.rest.transform.RestTestTransformByParentObject;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;

/**
 * A transformation to remove the key/value of a given match. To help keep logic simple, an empty match object will be left behind
 * if/when the only key/value is removed.
 */
public class RemoveMatch implements RestTestTransformByParentObject {
    private final String removeKey;
    private final String testName;

    public RemoveMatch(String removeKey) {
        this.removeKey = removeKey;
        this.testName = null;
    }

    public RemoveMatch(String removeKey, String testName) {
        this.removeKey = removeKey;
        this.testName = testName;
    }

    @Override
    @Internal
    public String getKeyToFind() {
        return "match";
    }

    @Override
    public String requiredChildKey() {
        return removeKey;
    }

    @Override
    public boolean shouldApply(RestTestContext testContext) {
        return testName == null || testContext.getTestName().equals(testName);
    }

    @Override
    public void transformTest(ObjectNode matchParent) {
        ObjectNode matchObject = (ObjectNode) matchParent.get(getKeyToFind());
        matchObject.remove(removeKey);
    }

    @Input
    public String getRemoveKey() {
        return removeKey;
    }

    @Input
    @Optional
    public String getTestName() {
        return testName;
    }
}
