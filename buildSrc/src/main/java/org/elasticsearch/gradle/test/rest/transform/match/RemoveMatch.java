/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test.rest.transform.match;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.gradle.test.rest.transform.RestTestTransformByParentObject;

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
    public String getKeyToFind() {
        return "match";
    }

    @Override
    public String requiredChildKey() {
        return removeKey;
    }

    @Override
    public String getTestName() {
        return testName;
    }

    @Override
    public void transformTest(ObjectNode matchParent) {
        ObjectNode matchObject = (ObjectNode) matchParent.get(getKeyToFind());
        matchObject.remove(removeKey);
    }
}
