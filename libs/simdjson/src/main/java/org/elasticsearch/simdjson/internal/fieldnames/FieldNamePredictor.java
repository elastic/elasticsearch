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
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Predicts the next field name of a document from its position in the walk, so that documents
 * that repeat the same shape never have to scan, hash, or look up their field names.
 *
 * <p>Documents written by one producer usually list their fields in the same order every time
 * (ClickBench's 105 flat fields, an OTel log record, a metric sample). The walker numbers every
 * field name it resolves, in document order, restarting at {@code 0} for each document. This class
 * remembers, per number, the name that was last seen there. On the next document the name at
 * position {@code n} is overwhelmingly likely to be the name remembered for position {@code n}, and
 * confirming that costs one comparison of the name's bytes against the buffer, instead of the
 * quote scan, hash, probe and key comparison a table lookup needs.
 *
 * <p>A prediction is only ever a hint, never an answer: {@link #predict} returns a name only when
 * the buffer holds exactly those bytes followed by a closing quote, so a wrong guess falls through
 * to the normal path and cannot change what is parsed. That includes a name containing an escape,
 * which can never match because remembered names are recorded only when their raw bytes equal their
 * decoded bytes.
 *
 * <p>Documents with different shapes interleaved on one thread (a bulk spanning several indices)
 * must not turn this into a cost. Each position carries a small saturating confidence counter: a
 * position that has predicted correctly keeps its name through a few wrong guesses rather than being
 * replaced by the first stranger, so two shapes alternating on one thread settle on one of them
 * instead of re-learning (and re-allocating) on every document. Positions beyond
 * {@link #MAX_POSITIONS} are not predicted, which bounds memory for documents with very many fields.
 *
 * <p>Even that is not enough when shapes alternate document by document (three schemas cycling on one
 * thread): almost every guess is wrong, and a wrong guess costs a failed comparison plus a learn that
 * may allocate, so the predictor would be slower than not having one. So each document is scored when
 * the next begins, counting only guesses that were actually wrong (a position with nothing remembered
 * yet is learning, not failing). If wrong guesses outnumber right ones, prediction and learning are
 * switched off for the next few documents, so those documents take exactly the old path at the cost of
 * one flag check per field name, and the pause doubles each time it is confirmed up to
 * {@link #MAX_BACKOFF_DOCUMENTS}. A document that predicts well resets it.
 *
 * <p>Names handed out are whatever {@link String} instances were passed to {@link #learn}, normally
 * the canonical instances from the field name table. If the table later swaps in a different but
 * {@code equals} instance, predictions keep returning the old one; callers already compare names with
 * {@code equals} (see {@link FieldNameLookup}), so the only effect is a lost identity fast path until
 * the position is relearned.
 *
 * <p><strong>Not thread-safe.</strong> One instance per walker, like the walker itself.
 */
public final class FieldNamePredictor {

    /** Upper bound on the number of positions remembered; later fields are always resolved normally. */
    public static final int MAX_POSITIONS = 1024;

    /** Documents skipped after the first poorly predicted one; doubles while predictions keep failing. */
    static final int MIN_BACKOFF_DOCUMENTS = 8;
    /** Longest pause, in documents, after repeated poor predictions. */
    public static final int MAX_BACKOFF_DOCUMENTS = 512;

    private static final int INITIAL_POSITIONS = 64;
    private static final byte MAX_CONFIDENCE = 3;
    private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    /** Name bytes per position, zero-padded to at least 8 so short names can be compared as one word. */
    private byte[][] keys = new byte[INITIAL_POSITIONS][];
    private int[] lens = new int[INITIAL_POSITIONS];
    private String[] names = new String[INITIAL_POSITIONS];
    private byte[] confidence = new byte[INITIAL_POSITIONS];

    /** False while paused after poorly predicted documents; {@link #predict} and {@link #learn} then do nothing. */
    private boolean active = true;
    private int skipDocuments;
    private int backoff = MIN_BACKOFF_DOCUMENTS;
    /** Right and wrong guesses in the current document; positions with nothing remembered count as neither. */
    private int hits;
    private int wrong;

    /**
     * Marks the start of a document: scores the previous one and decides whether to predict for this one.
     * Must be called once per document, before its first {@link #predict}.
     */
    public void beginDocument() {
        if (active && wrong > hits) {
            skipDocuments = backoff;
            backoff = Math.min(backoff << 1, MAX_BACKOFF_DOCUMENTS);
            // The remembered names were mostly wrong, so let the first document after the pause replace
            // them rather than spend several documents wearing their confidence down.
            Arrays.fill(confidence, (byte) 0);
        } else if (active && hits > 0) {
            backoff = MIN_BACKOFF_DOCUMENTS;
        }
        hits = 0;
        wrong = 0;
        if (skipDocuments > 0) {
            skipDocuments--;
            active = false;
        } else {
            active = true;
        }
    }

    /**
     * Returns the remembered name for {@code position} if the buffer at {@code start} holds exactly
     * that name followed by a closing quote, otherwise {@code null}.
     *
     * @param buf      the document buffer
     * @param start    index of the first byte after the field name's opening quote
     * @param position zero-based index of this field name within the document walk
     */
    public String predict(byte[] buf, int start, int position) {
        if (active == false || position >= names.length) {
            return null;
        }
        String name = names[position];
        if (name == null) {
            return null;
        }
        String predicted = confirm(name, buf, start, position);
        if (predicted == null) {
            wrong++;
        } else {
            hits++;
        }
        return predicted;
    }

    private String confirm(String name, byte[] buf, int start, int position) {
        int len = lens[position];
        int end = start + len;
        if (end >= buf.length || buf[end] != '"') {
            return null;
        }
        byte[] key = keys[position];
        if (len < 8) {
            // Short names are compared as one masked word: the key is zero-padded to 8 bytes, and
            // the buffer read is guarded because a name near the end of the buffer may have fewer
            // than 8 bytes after it.
            if (start + 8 > buf.length) {
                return null;
            }
            long mask = (1L << (len << 3)) - 1;
            if (((long) LONG_LE.get(buf, start) & mask) != ((long) LONG_LE.get(key, 0) & mask)) {
                return null;
            }
        } else {
            int i = 0;
            for (; i + 8 <= len; i += 8) {
                if ((long) LONG_LE.get(buf, start + i) != (long) LONG_LE.get(key, i)) {
                    return null;
                }
            }
            if (i < len && (long) LONG_LE.get(buf, end - 8) != (long) LONG_LE.get(key, len - 8)) {
                return null;
            }
        }
        byte c = confidence[position];
        if (c < MAX_CONFIDENCE) {
            confidence[position] = (byte) (c + 1);
        }
        return name;
    }

    /**
     * Records that the name at {@code position} resolved to {@code name}, after {@link #predict}
     * failed to anticipate it. The prediction at that position is replaced only once it has lost
     * its confidence, see the class comment.
     *
     * <p>Does nothing if the buffer's raw bytes at {@code start} are not the UTF-8 bytes of
     * {@code name} followed by a closing quote, which is how names containing escapes are kept out.
     *
     * @param position zero-based index of this field name within the document walk
     * @param name     the resolved name
     * @param buf      the document buffer
     * @param start    index of the first byte after the field name's opening quote
     */
    public void learn(int position, String name, byte[] buf, int start) {
        if (active == false || position >= MAX_POSITIONS) {
            return;
        }
        if (position < names.length && names[position] != null && confidence[position] > 0) {
            confidence[position]--;
            return;
        }
        byte[] raw = name.getBytes(UTF_8);
        int len = raw.length;
        int end = start + len;
        if (end >= buf.length || buf[end] != '"' || Arrays.equals(buf, start, end, raw, 0, len) == false) {
            return;
        }
        ensureCapacity(position + 1);
        keys[position] = len < 8 ? Arrays.copyOf(raw, 8) : raw;
        lens[position] = len;
        names[position] = name;
        confidence[position] = 1;
    }

    /**
     * Forgets every prediction and any pause, e.g. at a batch boundary or when the owning table's
     * contents have been reset: what follows may look nothing like what came before.
     */
    public void clear() {
        Arrays.fill(names, null);
        Arrays.fill(keys, null);
        Arrays.fill(confidence, (byte) 0);
        active = true;
        skipDocuments = 0;
        backoff = MIN_BACKOFF_DOCUMENTS;
        hits = 0;
        wrong = 0;
    }

    private void ensureCapacity(int needed) {
        if (needed <= names.length) {
            return;
        }
        int size = names.length;
        while (size < needed) {
            size <<= 1;
        }
        size = Math.min(size, MAX_POSITIONS);
        keys = Arrays.copyOf(keys, size);
        lens = Arrays.copyOf(lens, size);
        names = Arrays.copyOf(names, size);
        confidence = Arrays.copyOf(confidence, size);
    }
}
