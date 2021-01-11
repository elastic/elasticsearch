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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;

/**
 * A {@link RestTestTransform} that injects HTTP headers into a REST test. This includes adding the necessary values to the "do" section
 * as well as adding headers as a features to the "setup" section.
 */
public class InjectHeaders implements RestTestTransformByObjectKey, RestTestTransformGlobalSetup {

    private static JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);

    private final Map<String, String> headers;

    /**
     * @param headers The headers to inject
     */
    public InjectHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    @Override
    public void transformTest(ObjectNode doNodeParent) {
        ObjectNode doNodeValue = (ObjectNode) doNodeParent.get(getKeyToFind());
        ObjectNode headersNode = (ObjectNode) doNodeValue.get("headers");
        if (headersNode == null) {
            headersNode = new ObjectNode(jsonNodeFactory);
        }
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            headersNode.set(entry.getKey(), TextNode.valueOf(entry.getValue()));
        }
        doNodeValue.set("headers", headersNode);
    }

    @Override
    public String getKeyToFind() {
        return "do";
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
        Iterator<JsonNode> setupIt = setupNode.elements();
        boolean foundSkipNode = false;
        while (setupIt.hasNext()) {
            JsonNode arrayEntry = setupIt.next();
            if (arrayEntry.isObject()) {
                ObjectNode skipCandidate = (ObjectNode) arrayEntry;
                if (skipCandidate.get("skip") != null) {
                    ObjectNode skipNode = (ObjectNode) skipCandidate.get("skip");
                    foundSkipNode = true;
                    JsonNode featuresNode = skipNode.get("features");
                    if (featuresNode == null) {
                        ArrayNode featuresNodeArray = new ArrayNode(jsonNodeFactory);
                        featuresNodeArray.add("headers");
                        skipNode.set("features", featuresNodeArray);
                    } else if (featuresNode.isArray()) {
                        ArrayNode featuresNodeArray = (ArrayNode) featuresNode;
                        featuresNodeArray.add("headers");

                    } else if (featuresNode.isTextual()) {
                        ArrayNode featuresNodeArray = new ArrayNode(jsonNodeFactory);
                        featuresNodeArray.add(featuresNode.asText());
                        featuresNodeArray.add("headers");
                        // overwrite the features object
                        skipNode.set("features", featuresNodeArray);
                    }
                }
            }
        }
        if (foundSkipNode == false) {
            ObjectNode skipNode = new ObjectNode(jsonNodeFactory);
            ObjectNode featuresNode = new ObjectNode(jsonNodeFactory);
            ArrayNode featuresNodeContent = new ArrayNode(jsonNodeFactory);
            setupNode.add(skipNode);
            skipNode.set("skip", featuresNode);
            featuresNode.set("features", featuresNodeContent);
            featuresNodeContent.add("headers");
        }
        return setupNodeParent;
    }
}
