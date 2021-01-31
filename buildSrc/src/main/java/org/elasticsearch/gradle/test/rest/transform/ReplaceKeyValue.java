/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.test.rest.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.regex.Pattern;

/**
 * Replace the value of a named object. Must match key, and the regex pattern defined.
 */
public class ReplaceKeyValue implements RestTestTransformByObjectKey {

    private final JsonNode replacementNode;
    private final Pattern valueToMatch;
    private final String keyToFind;

    /**
     * @param keyToFind The object key name to find to evaluate for replacement
     * @param valueToMatch If the key matches and the (JSON) string of the value matches the regex pattern, then replace
     * @param replacementNode The value to replace with if the both the key and value pattern matches.
     */
    public ReplaceKeyValue(String keyToFind, Pattern valueToMatch, JsonNode replacementNode) {
        this.replacementNode = replacementNode;
        this.keyToFind = keyToFind;
        this.valueToMatch = valueToMatch;
    }

    @Override
    public String getKeyToFind() {
        return keyToFind;
    }

    public JsonNode getReplacementNode() {
        return this.replacementNode;
    }

    @Override
    public Pattern valueToMatch() {
        return valueToMatch;
    }

    @Override
    public void transformTest(ObjectNode nodeWithKey) {
        JsonNode value = nodeWithKey.get(getKeyToFind());
        if (value == null) {
            throw new IllegalStateException("Did not actually find " + getReplacementNode() + " node, this is likely a bug");
        }
        nodeWithKey.set(getKeyToFind(), getReplacementNode());
    }
}
