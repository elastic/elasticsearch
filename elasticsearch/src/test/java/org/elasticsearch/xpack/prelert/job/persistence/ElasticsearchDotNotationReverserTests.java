/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;


public class ElasticsearchDotNotationReverserTests extends ESTestCase {
    public void testResultsMap() throws Exception {
        ElasticsearchDotNotationReverser reverser = createReverser();

        String expected = "{\"complex\":{\"nested\":{\"structure\":{\"first\":\"x\"," +
                "\"second\":\"y\"},\"value\":\"z\"}},\"cpu\":{\"system\":\"5\"," +
                "\"user\":\"10\",\"wait\":\"1\"},\"simple\":\"simon\"}";

        String actual = XContentFactory.jsonBuilder().map(reverser.getResultsMap()).string();
        assertEquals(expected, actual);
    }

    public void testMappingsMap() throws Exception {
        ElasticsearchDotNotationReverser reverser = createReverser();

        String expected = "{\"complex\":{\"properties\":{\"nested\":{\"properties\":" +
                "{\"structure\":{\"properties\":{\"first\":{\"type\":\"keyword\"}," +
                "\"second\":{\"type\":\"keyword\"}},\"type\":\"object\"}," +
                "\"value\":{\"type\":\"keyword\"}},\"type\":\"object\"}}," +
                "\"type\":\"object\"},\"cpu\":{\"properties\":{\"system\":" +
                "{\"type\":\"keyword\"},\"user\":{\"type\":\"keyword\"}," +
                "\"wait\":{\"type\":\"keyword\"}},\"type\":\"object\"}," +
                "\"simple\":{\"type\":\"keyword\"}}";

        String actual = XContentFactory.jsonBuilder().map(reverser.getMappingsMap()).string();
        assertEquals(expected, actual);
    }

    private ElasticsearchDotNotationReverser createReverser() {
        ElasticsearchDotNotationReverser reverser = new ElasticsearchDotNotationReverser();
        // This should get ignored as it's a reserved field name
        reverser.add("bucket_span", "3600");
        reverser.add("simple", "simon");
        reverser.add("cpu.user", "10");
        reverser.add("cpu.system", "5");
        reverser.add("cpu.wait", "1");
        // This should get ignored as one of its segments is a reserved field name
        reverser.add("foo.bucket_span", "3600");
        reverser.add("complex.nested.structure.first", "x");
        reverser.add("complex.nested.structure.second", "y");
        reverser.add("complex.nested.value", "z");
        return reverser;
    }
}
