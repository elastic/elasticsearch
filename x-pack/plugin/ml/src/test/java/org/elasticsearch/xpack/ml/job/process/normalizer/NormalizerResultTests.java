/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

public class NormalizerResultTests extends AbstractSerializingTestCase<NormalizerResult> {

    private static final double EPSILON = 0.0000000001;

    public void testDefaultConstructor() {
        NormalizerResult msg = new NormalizerResult();
        assertNull(msg.getLevel());
        assertNull(msg.getPartitionFieldName());
        assertNull(msg.getPartitionFieldValue());
        assertNull(msg.getPersonFieldName());
        assertNull(msg.getPersonFieldValue());
        assertNull(msg.getFunctionName());
        assertNull(msg.getValueFieldName());
        assertEquals(0.0, msg.getProbability(), EPSILON);
        assertEquals(0.0, msg.getNormalizedScore(), EPSILON);
    }

    @Override
    protected NormalizerResult createTestInstance() {
        NormalizerResult msg = new NormalizerResult();
        msg.setLevel("leaf");
        msg.setPartitionFieldName("part");
        msg.setPartitionFieldValue("something");
        msg.setPersonFieldName("person");
        msg.setPersonFieldValue("fred");
        msg.setFunctionName("mean");
        msg.setValueFieldName("value");
        msg.setProbability(0.005);
        msg.setNormalizedScore(98.7);
        return msg;
    }

    @Override
    protected Reader<NormalizerResult> instanceReader() {
        return NormalizerResult::new;
    }

    @Override
    protected NormalizerResult doParseInstance(XContentParser parser) {
        return NormalizerResult.PARSER.apply(parser, null);
    }
}
