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
import org.elasticsearch.gradle.internal.test.rest.transform.feature.FeatureInjector;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * A {@link RestTestTransform} that injects a skip into a REST test.
 */
public class Skip implements RestTestTransformGlobalSetup, RestTestTransformByParentObject {

    private static JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);

    private final String skipReason;

    public Skip(String skipReason) {
        this.skipReason = skipReason;
    }

    @Override
    public ObjectNode transformSetup(@Nullable ObjectNode setupNodeParent) {
        ArrayNode setupNode;
        if (setupNodeParent == null) {
            setupNodeParent = new ObjectNode(jsonNodeFactory);
            setupNode = new ArrayNode(jsonNodeFactory);
            setupNodeParent.set("setup", setupNode);
        }
        setupNode = (ArrayNode) setupNodeParent.get("setup");
        addSkip(setupNode);
        return setupNodeParent;
    }

    private void addSkip(ArrayNode skipParent) {
        Iterator<JsonNode> skipParentIt = skipParent.elements();
        int i = 0;
        boolean remove = false;
        while (skipParentIt.hasNext()) {
            JsonNode arrayEntry = skipParentIt.next();
            if (arrayEntry.isObject()) {
                ObjectNode skipCandidate = (ObjectNode) arrayEntry;
                if (skipCandidate.get("skip") != null) {
                    remove = true;
                    break;
                }
            }
            i++;
        }
        if (remove) {
            skipParent.remove(i);
        }
        ObjectNode skipNode = new ObjectNode(jsonNodeFactory);
        ObjectNode skipChild = new ObjectNode(jsonNodeFactory);
        skipParent.insert(0, skipNode);
        skipChild.set("version", TextNode.valueOf("all"));
        skipChild.set("reason", TextNode.valueOf(skipReason));
        skipNode.set("skip", skipChild);
    }

    @Override
    public void transformTest(ObjectNode parent) {
        // do nothing (only transform the global setup)
    }

    @Override
    public String getKeyToFind() {
        // do nothing (only transform the global setup)
        return "";
    }
}
