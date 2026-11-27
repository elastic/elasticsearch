/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal.fieldnames;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

// A prediction is only a hint, so what matters here is that it never returns a name the buffer does not
// actually hold at that point of the walk.
public class FieldNamePredictorTests extends ESTestCase {

    /** Buffer holding {@code prefixPad} filler bytes, then {@code raw}, a closing quote, then {@code suffixPad} filler. */
    private static byte[] buffer(int prefixPad, String raw, int suffixPad) {
        byte[] bytes = raw.getBytes(UTF_8);
        byte[] buf = new byte[prefixPad + bytes.length + 1 + suffixPad];
        Arrays.fill(buf, (byte) 'x');
        System.arraycopy(bytes, 0, buf, prefixPad, bytes.length);
        buf[prefixPad + bytes.length] = '"';
        return buf;
    }

    /**
     * Walks one document through the predictor the way the walker does: begin, then for each name predict
     * and, on a miss, learn. Returns how many names were predicted; every prediction must be the right name.
     */
    private static int walk(FieldNamePredictor predictor, String... names) {
        predictor.beginDocument();
        int hits = 0;
        for (String name : names) {
            byte[] buf = buffer(0, name, 16);
            String predicted = predictor.predict(buf, 0);
            if (predicted == null) {
                predictor.learn(name, buf, 0, name.length());
            } else {
                assertEquals(name, predicted);
                hits++;
            }
        }
        return hits;
    }

    public void testNothingPredictedBeforeLearning() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        predictor.beginDocument();
        assertNull(predictor.predict(buffer(0, "name", 16), 0));
    }

    // Every length around the 8-byte word boundary, up to the longest name remembered, at any offset: the
    // second document predicts every name.
    public void testRepeatedDocumentIsPredicted() {
        for (int len = 0; len <= FieldNamePredictor.MAX_NAME_BYTES; len++) {
            String name = randomAlphaOfLength(len);
            int prefixPad = randomIntBetween(0, 20);
            byte[] buf = buffer(prefixPad, name, randomFrom(8, 9, 64));
            FieldNamePredictor predictor = new FieldNamePredictor();
            predictor.beginDocument();
            assertNull(predictor.predict(buf, prefixPad));
            predictor.learn(name, buf, prefixPad, len);
            predictor.beginDocument();
            assertSame("len=" + len, name, predictor.predict(buf, prefixPad));
        }
        assertEquals(0, walk(new FieldNamePredictor(), "a", "b", "c"));
        FieldNamePredictor predictor = new FieldNamePredictor();
        walk(predictor, "a", "b", "c");
        assertEquals(3, walk(predictor, "a", "b", "c"));
    }

    // Changing any byte, or ending the name earlier or later, defeats the prediction.
    public void testAnyChangedByteDefeatsPrediction() {
        for (int len : new int[] { 1, 5, 7, 8, 9, 16, 17, 40 }) {
            String name = randomAlphaOfLength(len);
            byte[] buf = buffer(0, name, 16);
            FieldNamePredictor predictor = new FieldNamePredictor();
            walk(predictor, name);
            for (int i = 0; i <= len; i++) {
                byte[] changed = buf.clone();
                changed[i] ^= 1;
                predictor.beginDocument();
                assertNull("len=" + len + " byte=" + i, predictor.predict(changed, 0));
            }
            predictor.beginDocument();
            assertNull(predictor.predict(buffer(0, name + "x", 16), 0));
            predictor.beginDocument();
            assertNull(predictor.predict(buffer(0, name.substring(0, len - 1), 16), 0));
        }
    }

    // A name at the very end of the buffer must never be read past it.
    public void testNameAtTheEndOfTheBufferIsSafe() {
        for (int len = 0; len <= FieldNamePredictor.MAX_NAME_BYTES; len++) {
            String name = randomAlphaOfLength(len);
            FieldNamePredictor predictor = new FieldNamePredictor();
            walk(predictor, name);
            for (int suffixPad = 0; suffixPad < 9; suffixPad++) {
                predictor.beginDocument();
                String predicted = predictor.predict(buffer(0, name, suffixPad), 0);
                assertTrue(predicted == null || predicted == name);
            }
            predictor.beginDocument();
            // the closing quote is missing altogether
            assertNull(predictor.predict(Arrays.copyOf(buffer(0, name, 16), len), 0));
        }
    }

    // The point of following names rather than positions: a field that comes and goes costs one wrong guess
    // where it changes, not a wrong guess for every name after it.
    public void testOptionalFieldCostsOneMiss() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        assertEquals(0, walk(predictor, "a", "b", "c", "d", "e"));
        assertEquals(5, walk(predictor, "a", "b", "c", "d", "e"));
        // "b" is absent: "c" is not what followed "a", but "d" and "e" are what followed "c" and "d"
        assertEquals(3, walk(predictor, "a", "c", "d", "e"));
        // "b" is back: "a" now leads to "c", but "c" still follows "b"
        assertEquals(4, walk(predictor, "a", "b", "c", "d", "e"));
        // extra fields at the start and in the middle: "b", "d" and "e" are still found
        assertEquals(3, walk(predictor, "x", "a", "b", "y", "c", "d", "e"));
    }

    // A name that appears more than once in a document is followed by different names, which can only cost
    // wrong guesses, never a wrong name.
    public void testRepeatedNamesAreNeverMispredicted() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        for (int i = 0; i < 5; i++) {
            walk(predictor, "cpu", "id", "x", "mem", "id", "y");
            walk(predictor, "cpu", "id", "y", "mem", "id", "x");
        }
    }

    // Names too long to remember are resolved normally, and the name after one is resolved normally too.
    public void testLongNamesAreNotRemembered() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        String tooLong = randomAlphaOfLength(FieldNamePredictor.MAX_NAME_BYTES + 1);
        walk(predictor, "a", tooLong, "b");
        assertEquals(1, walk(predictor, "a", tooLong, "b"));
    }

    // Documents that reorder names the predictor already knows mostly fail, and a failed prediction costs more
    // than none, so it gives up and stays off until the batch ends.
    public void testReorderedDocumentsGiveUp() {
        String[] forward = { "a", "b", "c", "d", "e", "f" };
        String[] backward = { "f", "e", "d", "c", "b", "a" };
        FieldNamePredictor predictor = new FieldNamePredictor();
        for (int doc = 0; doc < 100; doc++) {
            walk(predictor, doc % 2 == 0 ? forward : backward);
        }
        assertFalse(predictor.isPredicting());
        // even a document it would have predicted in full is not, until the batch ends
        assertEquals(0, walk(predictor, forward));
        predictor.clear();
        assertTrue(predictor.isPredicting());
        walk(predictor, forward);
        assertEquals(forward.length, walk(predictor, forward));
    }

    // A long run of well predicted documents must not postpone giving up once the documents start to reorder.
    public void testGivesUpEvenAfterALongStableRun() {
        String[] forward = { "a", "b", "c", "d", "e", "f" };
        String[] backward = { "f", "e", "d", "c", "b", "a" };
        FieldNamePredictor predictor = new FieldNamePredictor();
        for (int doc = 0; doc < 5000; doc++) {
            walk(predictor, forward);
        }
        assertTrue(predictor.isPredicting());
        for (int doc = 0; doc < 400; doc++) {
            walk(predictor, doc % 2 == 0 ? backward : forward);
        }
        assertFalse(predictor.isPredicting());
    }

    // Documents that bring new names, or that agree with what was learned, never make it give up.
    public void testStableAndGrowingShapesNeverGiveUp() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        for (int doc = 0; doc < 200; doc++) {
            walk(predictor, "a", "b", "c", "d");
            assertTrue(predictor.isPredicting());
        }
        if (randomBoolean()) {
            predictor = new FieldNamePredictor();
        }
        for (int doc = 0; doc < 200; doc++) {
            walk(predictor, "new" + doc + "a", "new" + doc + "b", "new" + doc + "c");
            assertTrue(predictor.isPredicting());
        }
    }

    // A few surprises in every document, as optional fields cause, never make it give up.
    public void testOccasionalSurprisesDoNotGiveUp() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        String[] all = { "a", "b", "c", "d", "e", "f", "g", "h" };
        for (int doc = 0; doc < 5000; doc++) {
            walk(predictor, doc % 2 == 0 ? all : new String[] { "a", "c", "d", "e", "f", "g", "h" });
            assertTrue(predictor.isPredicting());
        }
    }

    public void testClearForgetsEverything() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        assertEquals(0, walk(predictor, "a", "b"));
        assertEquals(2, walk(predictor, "a", "b"));
        predictor.clear();
        assertEquals(0, walk(predictor, "a", "b"));
        assertEquals(2, walk(predictor, "a", "b"));
    }

    // Whatever sequence of documents is walked, a prediction is returned only when the buffer genuinely holds
    // that name followed by a quote (checked by walk).
    public void testPredictionsAreAlwaysCorrect() {
        String[] pool = new String[10];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = randomAlphaOfLengthBetween(0, 20);
        }
        FieldNamePredictor predictor = new FieldNamePredictor();
        for (int doc = 0; doc < 2000; doc++) {
            String[] names = new String[randomIntBetween(0, 8)];
            for (int i = 0; i < names.length; i++) {
                names[i] = randomFrom(pool);
            }
            walk(predictor, names);
            if (rarely()) {
                predictor.clear();
            }
        }
    }
}
