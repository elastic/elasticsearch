/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.headers;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.elasticsearch.gradle.internal.test.rest.transform.RestTestTransform;
import org.elasticsearch.gradle.internal.test.rest.transform.RestTestTransformByParentObject;
import org.elasticsearch.gradle.internal.test.rest.transform.feature.FeatureInjector;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A {@link RestTestTransform} that injects HTTP headers into a REST test. This includes adding the necessary values to the "do" section
 * as well as adding headers as a features to the "setup" and "teardown" sections.
 */
public class InjectHeaders extends FeatureInjector implements RestTestTransformByParentObject {

    private static JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);

    private final Map<String, String> headers;
    private final Set<Function<ObjectNode, Boolean>> applyConditions;

    /**
     * @param headers The headers to inject
     * @param applyConditions a set of conditions that has to be satisfied in order to apply headers
     *                        If the Set is empty then headers are always applied.
     */
    public InjectHeaders(Map<String, String> headers, Set<Function<ObjectNode, Boolean>> applyConditions) {
        this.headers = headers;
        this.applyConditions = applyConditions;
    }

    @Override
    public void transformTest(ObjectNode doNodeParent) {
        ObjectNode doNodeValue = (ObjectNode) doNodeParent.get(getKeyToFind());

        if (shouldApplyHeaders(doNodeValue)) {
            ObjectNode headersNode = (ObjectNode) doNodeValue.get("headers");
            if (headersNode == null) {
                headersNode = new ObjectNode(jsonNodeFactory);
            }

            for (Map.Entry<String, String> entry : headers.entrySet()) {
                headersNode.set(entry.getKey(), TextNode.valueOf(entry.getValue()));
            }
            doNodeValue.set("headers", headersNode);
        }
    }

    private boolean shouldApplyHeaders(ObjectNode doNodeValue) {
        return applyConditions.stream().allMatch(f -> f.apply(doNodeValue));
    }

    @Override
    @Internal
    public String getKeyToFind() {
        return "do";
    }

    @Override
    @Internal
    public String getSkipFeatureName() {
        return "headers";
    }

    @Input
    public Map<String, String> getHeaders() {
        return headers;
    }
}
