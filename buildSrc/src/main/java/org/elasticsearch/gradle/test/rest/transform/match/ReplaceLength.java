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
public class ReplaceLength extends ReplaceKeyInMatch {


    public ReplaceLength(String replaceKey, String newKeyName, String testName) {
        super(replaceKey, newKeyName,  testName);
    }

    @Override
    @Internal
    public String getKeyToFind() {
        return "length";
    }
}
