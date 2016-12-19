/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.transforms.date;

import static org.elasticsearch.xpack.prelert.transforms.TransformTestUtils.createIndexArray;

import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.ESTestCase;

import org.elasticsearch.xpack.prelert.transforms.Transform.TransformIndex;
import org.elasticsearch.xpack.prelert.transforms.TransformException;

public class DoubleDateTransformTests extends ESTestCase {

    public void testTransform_GivenTimestampIsNotMilliseconds() throws TransformException {
        List<TransformIndex> readIndexes = createIndexArray(new TransformIndex(0, 0));
        List<TransformIndex> writeIndexes = createIndexArray(new TransformIndex(2, 0));

        DoubleDateTransform transformer = new DoubleDateTransform(false,
                readIndexes, writeIndexes, mock(Logger.class));

        String[] input = {"1000"};
        String[] scratch = {};
        String[] output = new String[1];
        String[][] readWriteArea = {input, scratch, output};

        transformer.transform(readWriteArea);

        assertEquals(1000000, transformer.epochMs());
        assertEquals("1000", output[0]);
    }

    public void testTransform_GivenTimestampIsMilliseconds() throws TransformException {
        List<TransformIndex> readIndexes = createIndexArray(new TransformIndex(0, 0));
        List<TransformIndex> writeIndexes = createIndexArray(new TransformIndex(2, 0));

        DoubleDateTransform transformer = new DoubleDateTransform(true,
                readIndexes, writeIndexes, mock(Logger.class));

        String[] input = {"1000"};
        String[] scratch = {};
        String[] output = new String[1];
        String[][] readWriteArea = {input, scratch, output};

        transformer.transform(readWriteArea);

        assertEquals(1000, transformer.epochMs());
        assertEquals("1", output[0]);
    }

    public void testTransform_GivenTimestampIsNotValidDouble() throws TransformException {
        List<TransformIndex> readIndexes = createIndexArray(new TransformIndex(0, 0));
        List<TransformIndex> writeIndexes = createIndexArray(new TransformIndex(2, 0));

        DoubleDateTransform transformer = new DoubleDateTransform(false,
                readIndexes, writeIndexes, mock(Logger.class));

        String[] input = {"invalid"};
        String[] scratch = {};
        String[] output = new String[1];
        String[][] readWriteArea = {input, scratch, output};

        ParseTimestampException e = ESTestCase.expectThrows(ParseTimestampException.class,
                () -> transformer.transform(readWriteArea));
        assertEquals("Cannot parse timestamp 'invalid' as epoch value", e.getMessage());
    }

    public void testTransform_InputFromScratchArea() throws TransformException {
        List<TransformIndex> readIndexes = createIndexArray(new TransformIndex(1, 0));
        List<TransformIndex> writeIndexes = createIndexArray(new TransformIndex(2, 0));

        DoubleDateTransform transformer = new DoubleDateTransform(false,
                readIndexes, writeIndexes, mock(Logger.class));

        String[] input = {};
        String[] scratch = {"1000"};
        String[] output = new String[1];
        String[][] readWriteArea = {input, scratch, output};

        transformer.transform(readWriteArea);
    }
}
