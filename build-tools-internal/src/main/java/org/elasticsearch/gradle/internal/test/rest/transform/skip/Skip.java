/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.skip;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import org.elasticsearch.gradle.internal.test.rest.transform.RestTestTransform;
import org.elasticsearch.gradle.internal.test.rest.transform.RestTestTransformByParentObject;
import org.elasticsearch.gradle.internal.test.rest.transform.RestTestTransformGlobalSetup;
import org.gradle.api.tasks.Input;

import java.util.Iterator;

/**
 * A {@link RestTestTransform} that injects a skip into a REST test.
 */
public class Skip implements RestTestTransformGlobalSetup, RestTestTransformByParentObject {

    private static JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);

    private final String skipReason;
    private final String testName;

    public Skip(String testName, String skipReason) {
        this.skipReason = skipReason;
        this.testName = testName;
    }

    public Skip(String skipReason) {
        this.skipReason = skipReason;
        this.testName = "";
    }

    @Override
    public ObjectNode transformSetup(ObjectNode setupNodeParent) {
        // only transform the global setup if there is no named test
        if (testName.isBlank()) {
            ArrayNode setupNode;
            if (setupNodeParent == null) {
                setupNodeParent = new ObjectNode(jsonNodeFactory);
                setupNode = new ArrayNode(jsonNodeFactory);
                setupNodeParent.set("setup", setupNode);
            }
            setupNode = (ArrayNode) setupNodeParent.get("setup");
            addSkip(setupNode);
        }
        return setupNodeParent;
    }

    private void addSkip(ArrayNode skipParent) {
        Iterator<JsonNode> skipParentIt = skipParent.elements();
        boolean found = false;
        while (skipParentIt.hasNext()) {
            JsonNode arrayEntry = skipParentIt.next();
            if (arrayEntry.isObject()) {
                ObjectNode skipCandidate = (ObjectNode) arrayEntry;
                if (skipCandidate.get("skip") != null) {
                    ObjectNode skipNode = (ObjectNode) skipCandidate.get("skip");
                    skipNode.replace("version", TextNode.valueOf("all"));
                    skipNode.replace("reason", TextNode.valueOf(skipReason));
                    found = true;
                    break;
                }
            }

        }

        if (found == false) {
            ObjectNode skipNode = new ObjectNode(jsonNodeFactory);
            skipParent.insert(0, skipNode);
            ObjectNode skipChild = new ObjectNode(jsonNodeFactory);
            skipChild.set("version", TextNode.valueOf("all"));
            skipChild.set("reason", TextNode.valueOf(skipReason));
            skipNode.set("skip", skipChild);
        }
    }

    @Override
    public void transformTest(ObjectNode parent) {
        if (testName.isBlank() == false) {
            assert parent.get(testName) instanceof ArrayNode;
            addSkip((ArrayNode) parent.get(testName));
        }
    }

    @Override
    public String getKeyToFind() {
        return testName;
    }

    @Input
    public String getSkipReason() {
        return skipReason;
    }

    @Input
    public String getTestName() {
        return testName;
    }
}
