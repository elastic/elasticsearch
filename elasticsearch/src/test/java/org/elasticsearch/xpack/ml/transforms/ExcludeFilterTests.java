/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.transforms;

import static org.elasticsearch.xpack.ml.transforms.TransformTestUtils.createIndexArray;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.ESTestCase;

import org.elasticsearch.xpack.ml.job.config.Condition;
import org.elasticsearch.xpack.ml.job.config.Operator;
import org.elasticsearch.xpack.ml.transforms.Transform.TransformIndex;
import org.elasticsearch.xpack.ml.transforms.Transform.TransformResult;

public class ExcludeFilterTests extends ESTestCase {

    public void testTransform_matches() throws TransformException {
        List<TransformIndex> readIndexes = createIndexArray(new TransformIndex(0, 0));
        List<TransformIndex> writeIndexes = createIndexArray();

        Condition cond = new Condition(Operator.MATCH, "cat");

        ExcludeFilterRegex transform = new ExcludeFilterRegex(cond, readIndexes, writeIndexes, mock(Logger.class));

        String[] input = {"cat"};
        String[] scratch = {};
        String[] output = {};
        String[][] readWriteArea = {input, scratch, output};

        assertEquals(TransformResult.EXCLUDE, transform.transform(readWriteArea));
    }


    public void testTransform_noMatches() throws TransformException {
        List<TransformIndex> readIndexes = createIndexArray(new TransformIndex(0, 0));
        List<TransformIndex> writeIndexes = createIndexArray();

        Condition cond = new Condition(Operator.MATCH, "boat");

        ExcludeFilterRegex transform = new ExcludeFilterRegex(cond, readIndexes, writeIndexes, mock(Logger.class));

        String[] input = {"cat"};
        String[] scratch = {};
        String[] output = {};
        String[][] readWriteArea = {input, scratch, output};

        assertEquals(TransformResult.OK, transform.transform(readWriteArea));
    }


    public void testTransform_matchesRegex() throws TransformException {
        List<TransformIndex> readIndexes = createIndexArray(new TransformIndex(0, 0));
        List<TransformIndex> writeIndexes = createIndexArray();

        Condition cond = new Condition(Operator.MATCH, "metric[0-9]+");

        ExcludeFilterRegex transform = new ExcludeFilterRegex(cond, readIndexes, writeIndexes, mock(Logger.class));
        String[] input = {"metric01"};
        String[] scratch = {};
        String[] output = new String[3];
        String[][] readWriteArea = {input, scratch, output};

        assertEquals(TransformResult.EXCLUDE, transform.transform(readWriteArea));

        readWriteArea[0] = new String[]{"metric02-A"};
        assertEquals(TransformResult.OK, transform.transform(readWriteArea));
    }



    public void testTransform_matchesMultipleInputs() throws TransformException {
        List<TransformIndex> readIndexes = createIndexArray(new TransformIndex(0, 0),
                new TransformIndex(0, 1),
                new TransformIndex(0, 2));
        List<TransformIndex> writeIndexes = createIndexArray();

        Condition cond = new Condition(Operator.MATCH, "boat");

        ExcludeFilterRegex transform = new ExcludeFilterRegex(cond, readIndexes, writeIndexes, mock(Logger.class));

        String[] input = {"cat", "hat", "boat"};
        String[] scratch = {};
        String[] output = {};
        String[][] readWriteArea = {input, scratch, output};

        assertEquals(TransformResult.EXCLUDE, transform.transform(readWriteArea));
    }



    public void testTransform() throws TransformException {
        List<TransformIndex> readIndexes = createIndexArray(new TransformIndex(0, 0));
        List<TransformIndex> writeIndexes = createIndexArray();

        Condition cond = new Condition(Operator.MATCH, "^(?!latency\\.total).*$");

        ExcludeFilterRegex transform = new ExcludeFilterRegex(cond, readIndexes, writeIndexes, mock(Logger.class));
        String[] input = {"utilization.total"};
        String[] scratch = {};
        String[] output = new String[3];
        String[][] readWriteArea = {input, scratch, output};

        TransformResult tr = transform.transform(readWriteArea);
        assertEquals(TransformResult.EXCLUDE, tr);

        readWriteArea[0] = new String[]{"latency.total"};
        tr = transform.transform(readWriteArea);
        assertEquals(TransformResult.OK, tr);
    }
}
