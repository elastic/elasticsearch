/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest.transform.close_to;

import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.gradle.internal.test.rest.transform.ReplaceByKey;
import org.gradle.api.tasks.Internal;

/**
 * Replaces the value of the `value` of a close_to assertion for a given sub-node.
 * For example: close_to:   { get.fields._routing: { value: 5.1, error: 0.00001 } }
 * to           close_to:   { get.fields._routing: { value: 9.5, error: 0.00001 } }
 */
public class ReplaceValueInCloseTo extends ReplaceByKey {

    public ReplaceValueInCloseTo(String replaceKey, NumericNode replacementNode) {
        this(replaceKey, replacementNode, null);
    }

    public ReplaceValueInCloseTo(String replaceKey, NumericNode replacementNode, String testName) {
        super(replaceKey, replaceKey, replacementNode, testName);
    }

    @Override
    @Internal
    public String getKeyToFind() {
        return "close_to";
    }

    @Override
    public void transformTest(ObjectNode matchParent) {
        ObjectNode closeToNode = (ObjectNode) matchParent.get(getKeyToFind());
        ObjectNode subNode = (ObjectNode) closeToNode.get(requiredChildKey());
        subNode.remove("value");
        subNode.set("value", getReplacementNode());
    }
}
