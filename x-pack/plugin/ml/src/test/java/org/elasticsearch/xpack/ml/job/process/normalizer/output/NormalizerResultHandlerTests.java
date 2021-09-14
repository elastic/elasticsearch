/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer.output;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.process.normalizer.NormalizerResult;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class NormalizerResultHandlerTests extends ESTestCase {

    private static final double EPSILON = 0.0000001;

    public void testParse() throws IOException {

        String testData = "{\"level\":\"leaf\",\"partition_field_name\":\"part\",\"partition_field_value\":\"v1\","
                + "\"person_field_name\":\"pers\",\"function_name\":\"f\","
                + "\"value_field_name\":\"x\",\"probability\":0.01,\"normalized_score\":88.88}\n"
                + "{\"level\":\"leaf\",\"partition_field_name\":\"part\",\"partition_field_value\":\"v2\","
                + "\"person_field_name\":\"pers\",\"function_name\":\"f\","
                + "\"value_field_name\":\"x\",\"probability\":0.02,\"normalized_score\":44.44}\n"
                + "{\"level\":\"leaf\",\"partition_field_name\":\"part\",\"partition_field_value\":\"v3\","
                + "\"person_field_name\":\"pers\",\"function_name\":\"f\","
                + "\"value_field_name\":\"x\",\"probability\":0.03,\"normalized_score\":22.22}\n";

        InputStream is = new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8));
        NormalizerResultHandler handler = new NormalizerResultHandler(is);
        handler.process();
        List<NormalizerResult> results = handler.getNormalizedResults();
        assertEquals(3, results.size());
        assertEquals(88.88, results.get(0).getNormalizedScore(), EPSILON);
        assertEquals(44.44, results.get(1).getNormalizedScore(), EPSILON);
        assertEquals(22.22, results.get(2).getNormalizedScore(), EPSILON);
    }
}

