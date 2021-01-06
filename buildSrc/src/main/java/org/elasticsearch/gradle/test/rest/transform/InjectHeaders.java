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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import javax.annotation.Nullable;
import java.util.Map;

public class InjectHeaders implements ObjectKeyFinder, RestTestSetupTransform {

    private static JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);

    private final Map<String, String> headers;

    public InjectHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    @Override
    public void transformTest(ObjectNode doNode) {
        ObjectNode doNodeValue = (ObjectNode) doNode.get(getKeyToFind());
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
        if (setupNodeParent == null) {
            setupNodeParent = new ObjectNode(jsonNodeFactory);
        }
        ObjectNode setupNode = (ObjectNode) setupNodeParent.get("setup");
        if (setupNode == null) {
            setupNode = new ObjectNode(jsonNodeFactory);
            setupNodeParent.set("setup", setupNode);
        }
        ObjectNode skipNode = (ObjectNode) setupNode.get("skip");
        if (skipNode == null) {
            skipNode = new ObjectNode(jsonNodeFactory);
            setupNode.set("skip", skipNode);
        }
        ArrayNode featuresNode = (ArrayNode) skipNode.get("features");
        if (featuresNode == null) {
            featuresNode = new ArrayNode(jsonNodeFactory);
            skipNode.set("features", featuresNode);
        }
        featuresNode.add("headers");
        return setupNodeParent;
    }
}
