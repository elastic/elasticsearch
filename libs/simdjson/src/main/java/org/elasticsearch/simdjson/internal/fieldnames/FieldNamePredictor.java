/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal.fieldnames;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

/**
 * Remembers which field name followed which, so that documents which repeat a shape resolve every name
 * with a comparison of the name's bytes (a single masked 8-byte word for short names) instead of a scan,
 * hash and table lookup.
 *
 * <p>Every distinct name seen is a node that points at the name that last followed it. A prediction is
 * the successor of the previous name in the walk. Unlike remembering a name per position, a field that
 * is present in one document and absent in the next costs one wrong guess instead of misaligning every
 * name after it.
 *
 * <p>A prediction is only a hint: {@link #predict} returns a name only when the buffer holds exactly the
 * remembered bytes followed by a closing quote. A name with an escape is never remembered, and breaks the
 * chain for the name after it.
 *
 * <p>A failed prediction costs more than not predicting, because the name is then resolved the slow way and
 * learned as well. When documents keep reordering names the predictor already knows, most predictions
 * fail, so it gives up until {@link #clear} (the end of the batch). Predicted names add to a score and
 * mispredicted ones subtract from it, so a stream where most names are predicted, even with a few
 * surprises in every document, never gives up. Names seen for the first time do not count.
 *
 * <p>Not thread-safe.
 */
public final class FieldNamePredictor {

    /** Names beyond this many distinct ones are not remembered. */
    static final int MAX_NODES = 1024;
    /** Longer names are not remembered. */
    static final int MAX_NAME_BYTES = 64;

    /** The score is capped at the start of each document, so that a long run of predictions cannot postpone giving up. */
    static final int MAX_SCORE = 1024;
    /** Below this score the predictor gives up; a little below zero so that learning does not trip it. */
    static final int GIVE_UP_SCORE = -64;

    private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private static final class Node {
        final String name;
        /** The name's bytes, padded to at least 8 so a short name can be compared as one word. */
        final byte[] key;
        final int len;
        /** The name that last followed this one, or {@code null}. */
        Node next;

        Node(String name, byte[] key, int len) {
            // matches() reads a whole word from the key, at 0 and at len - 8, without a bounds check
            assert key.length >= 8 && key.length >= len && len <= MAX_NAME_BYTES;
            this.name = name;
            this.key = key;
            this.len = len;
        }
    }

    private final Map<String, Node> nodes = new HashMap<>();
    /** Stands for the start of a document; its successor is the first name. */
    private final Node root = new Node("", new byte[8], 0);
    /** The node of the name resolved last, or {@code null} if that name could not be remembered. */
    private Node current = root;
    /** The node that was current when the last prediction failed, to link to the name resolved instead. */
    private Node missed;

    /**
     * Names predicted minus names not predicted although known (or never rememberable), since {@link #clear}.
     */
    private int score;
    /** Whether the score was too low at the start of this document; checked per name, so kept apart from the score. */
    private boolean gaveUp;

    /** Marks the start of a document, which is when the predictor decides whether to give up. */
    public void beginDocument() {
        if (score > MAX_SCORE) {
            score = MAX_SCORE;
        }
        gaveUp = score < GIVE_UP_SCORE;
        current = root;
        missed = null;
    }

    /** Whether the predictor has not given up. Primarily for testing. */
    boolean isPredicting() {
        return gaveUp == false;
    }

    /**
     * Returns the name that last followed the previous one if the buffer at {@code start} holds exactly
     * that name followed by a closing quote, otherwise {@code null}.
     *
     * @param start index of the first byte after the field name's opening quote
     */
    public String predict(byte[] buf, int start) {
        if (gaveUp) {
            return null;
        }
        Node from = current;
        if (from != null) {
            Node next = from.next;
            if (next != null && matches(next, buf, start)) {
                current = next;
                score++;
                return next.name;
            }
        }
        missed = from;
        current = null;
        return null;
    }

    private static boolean matches(Node node, byte[] buf, int start) {
        int len = node.len;
        int end = start + len;
        if (end >= buf.length || buf[end] != '"') {
            return false;
        }
        byte[] key = node.key;
        if (len < 8) {
            // One masked word; the read is guarded because a name near the end of the buffer may have
            // fewer than 8 bytes after it.
            if (start + 8 > buf.length) {
                return false;
            }
            long mask = (1L << (len << 3)) - 1;
            return (((long) LONG_LE.get(buf, start) ^ (long) LONG_LE.get(key, 0)) & mask) == 0;
        }
        int i = 0;
        for (; i + 8 <= len; i += 8) {
            if ((long) LONG_LE.get(buf, start + i) != (long) LONG_LE.get(key, i)) {
                return false;
            }
        }
        // Compare the last 8 bytes again rather than looping over the remainder.
        return i == len || (long) LONG_LE.get(buf, end - 8) == (long) LONG_LE.get(key, len - 8);
    }

    /**
     * Remembers that {@code name} followed the previous name, after {@link #predict} failed to anticipate
     * it. The caller guarantees that {@code buf[start, start + len)} holds the name's raw bytes, with no
     * escapes, and that a closing quote follows.
     *
     * @param start index of the first byte after the field name's opening quote
     * @param len   length of the name in bytes
     */
    public void learn(String name, byte[] buf, int start, int len) {
        if (gaveUp) {
            return;
        }
        assert current == null;  // learn must follow a failed predict
        assert isPlainName(buf, start, len);
        if (len > MAX_NAME_BYTES) {
            score--;
            return;
        }
        Node node = nodes.get(name);
        if (node == null) {
            if (nodes.size() >= MAX_NODES) {
                score--;
                return;
            }
            byte[] key = new byte[Math.max(len, 8)];
            System.arraycopy(buf, start, key, 0, len);
            node = new Node(name, key, len);
            nodes.put(name, node);
        } else {
            score--;
        }
        if (missed != null) {
            missed.next = node;
        }
        current = node;
    }

    /** Whether {@code buf[start, start + len)} is a name without escapes, followed by a closing quote. Only for assertions. */
    private static boolean isPlainName(byte[] buf, int start, int len) {
        if (start < 0 || len < 0 || start + len >= buf.length || buf[start + len] != '"') {
            return false;
        }
        for (int i = start; i < start + len; i++) {
            if (buf[i] == '\\') {
                return false;
            }
        }
        return true;
    }

    /** Forgets every prediction, e.g. when the owning table may have adopted different name instances. */
    public void clear() {
        nodes.clear();
        root.next = null;
        current = root;
        missed = null;
        score = 0;
        gaveUp = false;
    }
}
