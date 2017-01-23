/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config.transform;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class TransformSerialisationTests extends ESTestCase {

    public void testDeserialise_singleFieldAsArray() throws JsonProcessingException, IOException {

        String json = "{\"inputs\":\"dns\", \"transform\":\"domain_split\"}";
        XContentParser parser = XContentFactory.xContent(json).createParser(NamedXContentRegistry.EMPTY, json);
        TransformConfig tr = TransformConfig.PARSER.apply(parser, null);

        assertEquals(1, tr.getInputs().size());
        assertEquals("dns", tr.getInputs().get(0));
        assertEquals("domain_split", tr.getTransform());
        assertEquals(2, tr.getOutputs().size());
        assertEquals("subDomain", tr.getOutputs().get(0));
        assertEquals("hrd", tr.getOutputs().get(1));


        json = "{\"inputs\":\"dns\", \"transform\":\"domain_split\", \"outputs\":\"catted\"}";
        parser = XContentFactory.xContent(json).createParser(NamedXContentRegistry.EMPTY, json);
        tr = TransformConfig.PARSER.apply(parser, null);

        assertEquals(1, tr.getInputs().size());
        assertEquals("dns", tr.getInputs().get(0));
        assertEquals("domain_split", tr.getTransform());
        assertEquals(1, tr.getOutputs().size());
        assertEquals("catted", tr.getOutputs().get(0));
    }


    public void testDeserialise_fieldsArray() throws JsonProcessingException, IOException {

        String json = "{\"inputs\":[\"dns\"], \"transform\":\"domain_split\"}";
        XContentParser parser = XContentFactory.xContent(json).createParser(NamedXContentRegistry.EMPTY, json);
        TransformConfig tr = TransformConfig.PARSER.apply(parser, null);

        assertEquals(1, tr.getInputs().size());
        assertEquals("dns", tr.getInputs().get(0));
        assertEquals("domain_split", tr.getTransform());

        json = "{\"inputs\":[\"a\", \"b\", \"c\"], \"transform\":\"concat\", \"outputs\":[\"catted\"]}";
        parser = XContentFactory.xContent(json).createParser(NamedXContentRegistry.EMPTY, json);
        tr = TransformConfig.PARSER.apply(parser, null);

        assertEquals(3, tr.getInputs().size());
        assertEquals("a", tr.getInputs().get(0));
        assertEquals("b", tr.getInputs().get(1));
        assertEquals("c", tr.getInputs().get(2));
        assertEquals("concat", tr.getTransform());
        assertEquals(1, tr.getOutputs().size());
        assertEquals("catted", tr.getOutputs().get(0));
    }
}
