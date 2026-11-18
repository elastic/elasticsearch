/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.columnar.string;

import org.elasticsearch.test.ESTestCase;

public class PlainWritePlanTests extends ESTestCase {

    public void testEveryCombinationOfWhatTheColumnAsksAndWhatItsValuesCanDo() {
        // NOTE: columns are requested, output, inputsStream, mayBeSorted,
        // then capturesBounds, streamsValues, checksOrder, skipsRepeatedValues.
        // NOTE: no skip index, no output, inputs cannot stream: values are written from values and the order is checked.
        assertPlan(false, false, false, false, false, false, true, false);
        // NOTE: no skip index, no output, inputs cannot stream: ascending inputs change nothing.
        assertPlan(false, false, false, true, false, false, true, false);
        // NOTE: no skip index, no output, inputs stream and are not ascending: values are streamed.
        assertPlan(false, false, true, false, false, true, false, false);
        // NOTE: no skip index, no output, inputs stream but may be ascending: written from values so the order is checked.
        assertPlan(false, false, true, true, false, false, true, false);
        // NOTE: skip index off, output present: the output is ignored, inputs cannot stream, the order is checked.
        assertPlan(false, true, false, false, false, false, true, false);
        // NOTE: skip index off, output present: the output is ignored, ascending inputs change nothing.
        assertPlan(false, true, false, true, false, false, true, false);
        // NOTE: skip index off, output present: the output is ignored, non ascending inputs are streamed.
        assertPlan(false, true, true, false, false, true, false, false);
        // NOTE: skip index off, output present: the output is ignored, ascending inputs are written from values.
        assertPlan(false, true, true, true, false, false, true, false);
        // NOTE: skip index on but no output to write it to: no bounds, inputs cannot stream, the order is checked.
        assertPlan(true, false, false, false, false, false, true, false);
        // NOTE: skip index on but no output to write it to: no bounds, ascending inputs change nothing.
        assertPlan(true, false, false, true, false, false, true, false);
        // NOTE: skip index on but no output to write it to: no bounds, non ascending inputs are streamed.
        assertPlan(true, false, true, false, false, true, false, false);
        // NOTE: skip index on but no output to write it to: no bounds, ascending inputs are written from values.
        assertPlan(true, false, true, true, false, false, true, false);
        // NOTE: bounds are captured: values are needed in hand, and inputs that cannot stream report no repeats to skip.
        assertPlan(true, true, false, false, true, false, false, false);
        // NOTE: bounds are captured: the region proves the order, so ascending inputs need no writer order check or repeat skip.
        assertPlan(true, true, false, true, true, false, false, false);
        // NOTE: bounds are captured: streaming inputs are written from values, but the repeats they report are not read.
        assertPlan(true, true, true, false, true, false, false, true);
        // NOTE: bounds are captured: ascending streaming inputs are written from values, repeats are not read, no writer check.
        assertPlan(true, true, true, true, true, false, false, true);
    }

    private static void assertPlan(
        boolean requested,
        boolean output,
        boolean inputsStream,
        boolean mayBeSorted,
        boolean capturesBounds,
        boolean streamsValues,
        boolean checksOrder,
        boolean skipsRepeatedValues
    ) {
        final String inputs = "requested=" + requested + " output=" + output + " inputsStream=" + inputsStream + " sorted=" + mayBeSorted;
        assertEquals(
            inputs,
            new PlainWritePlan(capturesBounds, streamsValues, checksOrder, skipsRepeatedValues),
            PlainWritePlan.of(requested, output, inputsStream, mayBeSorted)
        );
    }
}
