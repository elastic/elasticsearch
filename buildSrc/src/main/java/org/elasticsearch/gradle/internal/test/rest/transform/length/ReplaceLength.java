/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.length;

import org.elasticsearch.gradle.internal.test.rest.transform.match.ReplaceKeyInMatch;
import org.gradle.api.tasks.Internal;

/**
 * A transformation to replace the key in a length assertion.
 * For example, change from "length ":{"index._type": 1} to "length ":{"index._doc": 1}
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
