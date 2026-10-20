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

// Unit tests for FieldNamePredictor: a prediction is only a hint, so every test that matters here is
// about it never returning a name the buffer does not actually hold at that position.
public class FieldNamePredictorTests extends ESTestCase {

    /** Buffer holding {@code prefixPad} filler bytes, then {@code name}, a closing quote, then {@code suffixPad} filler. */
    private static byte[] buffer(int prefixPad, String name, int suffixPad) {
        byte[] raw = name.getBytes(UTF_8);
        byte[] buf = new byte[prefixPad + raw.length + 1 + suffixPad];
        Arrays.fill(buf, (byte) 'x');
        System.arraycopy(raw, 0, buf, prefixPad, raw.length);
        buf[prefixPad + raw.length] = '"';
        return buf;
    }

    // Nothing has been learned yet, so nothing is predicted, at any position including past the initial capacity.
    public void testNothingPredictedBeforeLearning() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        byte[] buf = buffer(0, "name", 16);
        for (int position : new int[] { 0, 1, 63, 64, 1023, 5000 }) {
            assertNull(predictor.predict(buf, 0, position));
        }
    }

    // A learned name is predicted for every length around the 8-byte word boundary and well past it,
    // whether or not the buffer has bytes after the closing quote, and at a non-zero offset.
    public void testLearnedNameIsPredictedForAnyLength() {
        for (int len : new int[] { 0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 23, 24, 25, 31, 32, 33, 100 }) {
            String name = randomAlphaOfLength(len);
            for (int suffixPad : new int[] { 0, 1, 7, 8, 64 }) {
                int prefixPad = randomIntBetween(0, 20);
                byte[] buf = buffer(prefixPad, name, suffixPad);
                FieldNamePredictor predictor = new FieldNamePredictor();
                int position = randomIntBetween(0, 200);
                predictor.learn(position, name, buf, prefixPad);
                String predicted = predictor.predict(buf, prefixPad, position);
                if (len < 8 && prefixPad + 8 > buf.length) {
                    // a short name in the last 8 bytes of the buffer is conservatively not predicted:
                    // comparing it as one word would read past the end
                    assertTrue("len=" + len, predicted == null || predicted == name);
                } else {
                    assertSame("len=" + len + " suffixPad=" + suffixPad, name, predicted);
                }
            }
        }
    }

    // A prediction is for one position only.
    public void testPredictionIsPerPosition() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        byte[] buf = buffer(0, "alpha", 16);
        predictor.learn(3, "alpha", buf, 0);
        assertNull(predictor.predict(buf, 0, 2));
        assertNull(predictor.predict(buf, 0, 4));
        assertEquals("alpha", predictor.predict(buf, 0, 3));
    }

    // Changing any single byte of the name, at any index, must defeat the prediction.
    public void testAnyChangedByteDefeatsPrediction() {
        for (int len : new int[] { 1, 3, 7, 8, 9, 16, 17, 33 }) {
            String name = randomAlphaOfLength(len);
            byte[] original = buffer(0, name, 16);
            FieldNamePredictor predictor = new FieldNamePredictor();
            predictor.learn(0, name, original, 0);
            for (int i = 0; i < len; i++) {
                byte[] changed = original.clone();
                changed[i] ^= 0x01;
                assertNull("len=" + len + " changed byte " + i, predictor.predict(changed, 0, 0));
            }
        }
    }

    // A longer name that merely starts with the predicted one must not match: the byte after the
    // predicted name has to be the closing quote.
    public void testPredictedNameThatIsAPrefixOfTheActualNameDoesNotMatch() {
        for (int len : new int[] { 0, 1, 7, 8, 9, 16, 24 }) {
            String name = randomAlphaOfLength(len);
            FieldNamePredictor predictor = new FieldNamePredictor();
            predictor.learn(0, name, buffer(0, name, 16), 0);
            byte[] longer = buffer(0, name + randomAlphaOfLengthBetween(1, 5), 16);
            assertNull("len=" + len, predictor.predict(longer, 0, 0));
        }
    }

    // A shorter name that the predicted one starts with must not match either.
    public void testPredictedNameLongerThanTheActualNameDoesNotMatch() {
        String name = randomAlphaOfLengthBetween(5, 30);
        FieldNamePredictor predictor = new FieldNamePredictor();
        predictor.learn(0, name, buffer(0, name, 16), 0);
        for (int shorter = 0; shorter < name.length(); shorter++) {
            assertNull("shorter=" + shorter, predictor.predict(buffer(0, name.substring(0, shorter), 16), 0, 0));
        }
    }

    // A name sitting in the last bytes of the buffer must never read past its end, even though the
    // short-name path compares a whole 8-byte word.
    public void testNameAtTheEndOfTheBufferNeverReadsOutOfBounds() {
        for (int len = 0; len <= 20; len++) {
            String name = randomAlphaOfLength(len);
            byte[] buf = buffer(0, name, 0);
            FieldNamePredictor predictor = new FieldNamePredictor();
            predictor.learn(0, name, buf, 0);
            // either a hit or a clean miss is fine, an exception is not
            String predicted = predictor.predict(buf, 0, 0);
            assertTrue(predicted == null || predicted.equals(name));
            // and a buffer that ends before the closing quote must miss
            assertNull(predictor.predict(Arrays.copyOf(buf, buf.length - 1), 0, 0));
        }
    }

    // A name containing an escape in the document can never be learned, because its raw bytes are not
    // its decoded bytes; the same raw bytes then must not predict a name either.
    public void testEscapedNamesAreNotLearned() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        // raw bytes: ab\u0063" decoded name: abc
        byte[] escaped = "ab\\u0063\"xxxxxxxx".getBytes(UTF_8);
        predictor.learn(0, "abc", escaped, 0);
        assertNull(predictor.predict(escaped, 0, 0));
        // and an unescaped "abc" is still not predicted, since nothing was learned
        assertNull(predictor.predict(buffer(0, "abc", 16), 0, 0));
    }

    // A decoded name that contains a quote or backslash is not learnable for the same reason.
    public void testNamesWithQuoteOrBackslashAreNotLearned() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        for (String name : new String[] { "a\"b", "a\\b" }) {
            byte[] buf = ("a" + (name.charAt(1) == '"' ? "\\\"" : "\\\\") + "b\"xxxxxxxx").getBytes(UTF_8);
            predictor.learn(0, name, buf, 0);
            assertNull(predictor.predict(buf, 0, 0));
        }
    }

    // Non-ASCII names are compared on their UTF-8 bytes.
    public void testNonAsciiName() {
        String name = "caf\u00e9_\u65e5\u672c\u8a9e";
        byte[] buf = buffer(2, name, 16);
        FieldNamePredictor predictor = new FieldNamePredictor();
        predictor.learn(1, name, buf, 2);
        assertSame(name, predictor.predict(buf, 2, 1));
    }

    // A position that has been predicting correctly keeps its name through a stranger, so shapes that
    // interleave do not make it flap between two names.
    public void testConfidentPredictionSurvivesAMiss() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        byte[] a = buffer(0, "first", 16);
        byte[] b = buffer(0, "second", 16);
        predictor.learn(0, "first", a, 0);
        assertEquals("first", predictor.predict(a, 0, 0)); // confidence now 2
        assertNull(predictor.predict(b, 0, 0));
        predictor.learn(0, "second", b, 0); // costs one point of confidence, does not replace
        assertEquals("first", predictor.predict(a, 0, 0));
        assertNull(predictor.predict(b, 0, 0));
    }

    // With no confidence left, a different name replaces the old one.
    public void testPredictionIsReplacedOnceConfidenceIsExhausted() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        byte[] a = buffer(0, "first", 16);
        byte[] b = buffer(0, "second", 16);
        predictor.learn(0, "first", a, 0); // confidence 1, never confirmed
        predictor.learn(0, "second", b, 0); // costs the single point
        assertEquals("first", predictor.predict(a, 0, 0)); // still first, and now confirmed again
        // a stream that only ever shows "second" at this position wins eventually
        for (int i = 0; i < 10; i++) {
            if (predictor.predict(b, 0, 0) == null) {
                predictor.learn(0, "second", b, 0);
            }
        }
        assertEquals("second", predictor.predict(b, 0, 0));
        assertNull(predictor.predict(a, 0, 0));
    }

    // Positions past the cap are never remembered, and asking about them is safe.
    public void testPositionsPastTheCapAreIgnored() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        byte[] buf = buffer(0, "name", 16);
        predictor.learn(FieldNamePredictor.MAX_POSITIONS, "name", buf, 0);
        predictor.learn(Integer.MAX_VALUE, "name", buf, 0);
        assertNull(predictor.predict(buf, 0, FieldNamePredictor.MAX_POSITIONS));
        assertNull(predictor.predict(buf, 0, Integer.MAX_VALUE));
        predictor.learn(FieldNamePredictor.MAX_POSITIONS - 1, "name", buf, 0);
        assertEquals("name", predictor.predict(buf, 0, FieldNamePredictor.MAX_POSITIONS - 1));
    }

    // The arrays grow on demand: learning at a high position first, then a low one, keeps both.
    public void testGrowthKeepsEarlierEntries() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        byte[] buf = buffer(0, "name", 16);
        predictor.learn(0, "name", buf, 0);
        predictor.learn(700, "name", buf, 0);
        predictor.learn(300, "name", buf, 0);
        for (int position : new int[] { 0, 300, 700 }) {
            assertEquals("name", predictor.predict(buf, 0, position));
        }
        assertNull(predictor.predict(buf, 0, 299));
    }

    public void testClearForgetsEverything() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        byte[] buf = buffer(0, "name", 16);
        for (int position : new int[] { 0, 5, 100 }) {
            predictor.learn(position, "name", buf, 0);
        }
        predictor.clear();
        for (int position : new int[] { 0, 5, 100 }) {
            assertNull(predictor.predict(buf, 0, position));
        }
        // and it can be used again afterwards
        predictor.learn(5, "name", buf, 0);
        assertEquals("name", predictor.predict(buf, 0, 5));
    }

    // Randomised: whatever sequence of learn/predict calls is made, a prediction is returned only when
    // the buffer genuinely holds that name followed by a quote.
    public void testPredictionsAreAlwaysCorrect() {
        String[] pool = new String[12];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = randomAlphaOfLengthBetween(0, 20);
        }
        FieldNamePredictor predictor = new FieldNamePredictor();
        for (int i = 0; i < 5000; i++) {
            String actual = randomFrom(pool);
            int prefixPad = randomIntBetween(0, 9);
            byte[] buf = buffer(prefixPad, actual, randomIntBetween(0, 12));
            int position = randomIntBetween(0, 20);
            String predicted = predictor.predict(buf, prefixPad, position);
            if (predicted != null) {
                assertEquals(actual, predicted);
            } else {
                predictor.learn(position, actual, buf, prefixPad);
            }
            if (rarely()) {
                predictor.clear();
            }
        }
    }

    // ---- Back-off ----

    /**
     * Walks one document through the predictor the way the walker does: begin, then for each name predict and,
     * on a miss, learn. Returns how many names were predicted.
     */
    private static int walk(FieldNamePredictor predictor, String... names) {
        predictor.beginDocument();
        int predicted = 0;
        for (int position = 0; position < names.length; position++) {
            byte[] buf = buffer(0, names[position], 16);
            if (predictor.predict(buf, 0, position) != null) {
                predicted++;
            } else {
                predictor.learn(position, names[position], buf, 0);
            }
        }
        return predicted;
    }

    // The first documents teach the predictor, which is not failing: positions with nothing remembered are not
    // wrong guesses, so a steady shape is never paused and predicts every name from the second document on.
    public void testSteadyShapeIsNeverPaused() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        String[] shape = { "one", "two", "three", "four", "five" };
        assertEquals(0, walk(predictor, shape));
        for (int i = 0; i < 100; i++) {
            assertEquals("document " + i, shape.length, walk(predictor, shape));
        }
    }

    // Shapes that disagree at every position but the first make most guesses wrong. The predictor must then pause,
    // so the documents inside the pause take the old path without predicting anything.
    public void testPredictorPausesWhenMostGuessesAreWrong() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        String[] a = { "type", "id", "ts", "val", "label" };
        String[] b = { "type", "uid", "score", "tags", "region" };
        walk(predictor, a); // learns
        walk(predictor, b); // 1 right, 4 wrong
        // the next FieldNamePredictor.MIN_BACKOFF_DOCUMENTS documents are paused, whatever they look like
        for (int i = 0; i < FieldNamePredictor.MIN_BACKOFF_DOCUMENTS; i++) {
            assertEquals("paused document " + i, 0, walk(predictor, i % 2 == 0 ? a : b));
        }
        // and the pause ends: predictions resume, the first document after it relearns
        walk(predictor, a);
        assertEquals(a.length, walk(predictor, a));
    }

    // Each confirmed poor stretch doubles the pause, up to the cap, so a stream that never settles spends almost
    // none of its documents predicting.
    public void testPauseDoublesWhileShapesKeepDisagreeing() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        String[] a = { "type", "id", "ts", "val", "label" };
        String[] b = { "type", "uid", "score", "tags", "region" };
        String[] c = { "type", "key", "name", "bytes", "ok" };
        String[][] shapes = { a, b, c };
        int documents = 20_000;
        int activeDocuments = 0;
        for (int i = 0; i < documents; i++) {
            predictor.beginDocument();
            // a document is active if the very first position, which always has something remembered after
            // learning, was predicted or wrongly guessed rather than skipped: observe through predict's result
            String[] shape = shapes[i % 3];
            boolean anyPredicted = false;
            for (int position = 0; position < shape.length; position++) {
                byte[] buf = buffer(0, shape[position], 16);
                if (predictor.predict(buf, 0, position) != null) {
                    anyPredicted = true;
                } else {
                    predictor.learn(position, shape[position], buf, 0);
                }
            }
            if (anyPredicted) {
                activeDocuments++;
            }
        }
        // Without back-off position 0 ("type") alone would be predicted in every document, 20000 of them. The pause
        // does not grow without bound: a probe document that happens to repeat the shape learned by the previous
        // probe predicts well and resets the pause to its minimum, so a modest share of documents is still probed.
        assertTrue("predicted in " + activeDocuments + " of " + documents + " documents", activeDocuments < documents / 5);
    }

    // A document that predicts well ends the escalation: after a stable stretch the pause starts small again.
    public void testGoodDocumentResetsTheBackoff() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        String[] a = { "type", "id", "ts", "val", "label" };
        String[] b = { "type", "uid", "score", "tags", "region" };
        // escalate the pause by alternating
        for (int i = 0; i < 400; i++) {
            walk(predictor, i % 2 == 0 ? a : b);
        }
        predictor.clear();
        // after clear the predictor is active and unpaused with the smallest backoff: it learns a, then predicts it
        walk(predictor, a);
        assertEquals(a.length, walk(predictor, a));
        walk(predictor, b); // wrong at 4 positions, pauses for the smallest backoff
        for (int i = 0; i < FieldNamePredictor.MIN_BACKOFF_DOCUMENTS; i++) {
            assertEquals(0, walk(predictor, a));
        }
    }

    // clear() also ends a pause.
    public void testClearEndsAPause() {
        FieldNamePredictor predictor = new FieldNamePredictor();
        String[] a = { "type", "id", "ts", "val", "label" };
        String[] b = { "type", "uid", "score", "tags", "region" };
        walk(predictor, a);
        walk(predictor, b);
        assertEquals(0, walk(predictor, a)); // paused
        predictor.clear();
        walk(predictor, a);
        assertEquals(a.length, walk(predictor, a));
    }
}
