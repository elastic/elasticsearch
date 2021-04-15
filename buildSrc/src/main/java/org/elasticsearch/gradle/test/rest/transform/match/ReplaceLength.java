/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test.rest.transform.match;

import org.gradle.api.tasks.Internal;

/**
 * A transformation to replace the value of a match. For example, change from "match":{"_type": "foo"} to "match":{"_type": "bar"}
 */
public class ReplaceLength extends ReplaceKeyInMatch {

    public ReplaceLength(String replaceKey, String newKeyName, String testName) {
        super(replaceKey, newKeyName, testName);
    }

    @Override
    @Internal
    public String getKeyToFind() {
        return "length";
    }
}
