/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.transforms;

import static org.elasticsearch.xpack.prelert.transforms.TransformTestUtils.createIndexArray;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.logging.log4j.Logger;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.transforms.Transform.TransformIndex;
import org.elasticsearch.xpack.prelert.transforms.Transform.TransformResult;

public class RegexExtractTests extends ESTestCase {

    public void testTransform() throws TransformException {
        List<TransformIndex> readIndexes = createIndexArray(new TransformIndex(0, 0));
        List<TransformIndex> writeIndexes = createIndexArray(new TransformIndex(2, 0),
                new TransformIndex(2, 1), new TransformIndex(2, 2));

        String regex = "Tag=\"Windfarm ([0-9]+)\\.Turbine ([0-9]+)\\.(.*)\"";

        RegexExtract transform = new RegexExtract(regex, readIndexes, writeIndexes, mock(Logger.class));

        String[] input = {"Tag=\"Windfarm 04.Turbine 06.Temperature\""};
        String[] scratch = {};
        String[] output = new String[3];
        String[][] readWriteArea = {input, scratch, output};

        assertEquals(TransformResult.OK, transform.transform(readWriteArea));
        assertEquals("04", output[0]);
        assertEquals("06", output[1]);
        assertEquals("Temperature", output[2]);
    }
}
