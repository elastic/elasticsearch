/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class NodeAttributesTests extends AbstractXContentTestCase<NodeAttributes> {

    public static NodeAttributes createRandom() {
        int numberOfAttributes = randomIntBetween(1, 10);
        Map<String, String> attributes = new HashMap<>(numberOfAttributes);
        for(int i = 0; i < numberOfAttributes; i++) {
            String val = randomAlphaOfLength(10);
            attributes.put("key-"+i, val);
        }
        return new NodeAttributes(randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            attributes);
    }

    @Override
    protected NodeAttributes createTestInstance() {
        return createRandom();
    }

    @Override
    protected NodeAttributes doParseInstance(XContentParser parser) throws IOException {
        return NodeAttributes.PARSER.parse(parser, null);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
