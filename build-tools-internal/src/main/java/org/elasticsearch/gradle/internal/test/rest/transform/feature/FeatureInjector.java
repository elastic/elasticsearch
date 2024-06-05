/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.feature;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import org.elasticsearch.gradle.internal.test.rest.transform.RestTestTransformGlobalSetup;
import org.elasticsearch.gradle.internal.test.rest.transform.RestTestTransformGlobalTeardown;
import org.gradle.api.tasks.Internal;

import java.util.Iterator;

import javax.annotation.Nullable;

/**
 * A parent class for transformations that are backed by a feature. This will inject the necessary "feature" into the
 * global setup and teardown section. See also org.elasticsearch.test.rest.yaml.Features for a list of possible features.
 */
public abstract class FeatureInjector implements RestTestTransformGlobalSetup, RestTestTransformGlobalTeardown {

    private static JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);

    @Override
    public ObjectNode transformSetup(ObjectNode setupNodeParent) {
        // check to ensure that headers feature does not already exist
        if (setupNodeParent != null) {
            ArrayNode setupNode = (ArrayNode) setupNodeParent.get("setup");
            if (hasFeature(setupNode)) {
                return setupNodeParent;
            }
        }
        // transform or insert the headers feature into setup/skip/features
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

    @Override
    public ObjectNode transformTeardown(@Nullable ObjectNode teardownNodeParent) {
        if (teardownNodeParent != null) {
            ArrayNode teardownNode = (ArrayNode) teardownNodeParent.get("teardown");
            // only transform an existing teardown section since a teardown does not inherit from setup but still needs the skip section
            if (teardownNode != null) {
                // check to ensure that headers feature does not already exist
                if (hasFeature(teardownNode)) {
                    return teardownNodeParent;
                }
                addSkip(teardownNode);
                return teardownNodeParent;
            }
        }
        return teardownNodeParent;
    }

    /**
     * The name of the feature to skip. These are defined in org.elasticsearch.test.rest.yaml.Features and found in the tests at
     * as the value of skip.feature. For example this method should return "allowed_warnings" :
     * <pre>
     * skip:
     *       features: allowed_warnings
     * </pre>
     */
    @Internal
    public abstract String getSkipFeatureName();

    private boolean hasFeature(ArrayNode skipParent) {
        JsonNode features = skipParent.at("/0/skip/features");
        if (features != null) {
            if (features.isArray()) {
                ArrayNode featuresArray = (ArrayNode) features;
                Iterator<JsonNode> it = featuresArray.elements();
                while (it.hasNext()) {
                    if (getSkipFeatureName().equals(it.next().asText())) {
                        return true;
                    }
                }
            } else {
                if (getSkipFeatureName().equals(features.asText())) {
                    return true;
                }
            }
        }
        return false;
    }

    private void addSkip(ArrayNode skipParent) {
        Iterator<JsonNode> skipParentIt = skipParent.elements();
        boolean foundSkipNode = false;
        while (skipParentIt.hasNext()) {
            JsonNode arrayEntry = skipParentIt.next();
            if (arrayEntry.isObject()) {
                ObjectNode skipCandidate = (ObjectNode) arrayEntry;
                if (skipCandidate.get("skip") != null) {
                    ObjectNode skipNode = (ObjectNode) skipCandidate.get("skip");
                    foundSkipNode = true;
                    JsonNode featuresNode = skipNode.get("features");
                    if (featuresNode == null) {
                        skipNode.set("features", TextNode.valueOf(getSkipFeatureName()));
                    } else if (featuresNode.isArray()) {
                        ArrayNode featuresNodeArray = (ArrayNode) featuresNode;
                        featuresNodeArray.add(getSkipFeatureName());
                    } else if (featuresNode.isTextual()) {
                        // convert to an array
                        ArrayNode featuresNodeArray = new ArrayNode(jsonNodeFactory);
                        featuresNodeArray.add(featuresNode.asText());
                        featuresNodeArray.add(getSkipFeatureName());
                        // overwrite the features object
                        skipNode.set("features", featuresNodeArray);
                    }
                }
            }
        }
        if (foundSkipNode == false) {
            ObjectNode skipNode = new ObjectNode(jsonNodeFactory);
            ObjectNode featuresNode = new ObjectNode(jsonNodeFactory);
            skipParent.insert(0, skipNode);
            featuresNode.set("features", TextNode.valueOf(getSkipFeatureName()));
            skipNode.set("skip", featuresNode);
        }
    }
}
