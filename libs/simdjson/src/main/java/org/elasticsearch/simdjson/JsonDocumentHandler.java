/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import java.math.BigInteger;

/**
 * Callback interface for receiving parsed JSON document events from
 * {@link SimdJsonDirectWalker}. Implementations map these events to a
 * storage format (e.g. columnar row buffers, XContent output).
 *
 * <h2>Event model</h2>
 *
 * <p>Top-level object fields are delivered as named leaf methods
 * ({@link #stringField}, {@link #longField}, etc.). Nested objects are
 * bracketed by {@link #startObject}/{@link #endObject}.
 *
 * <p>Arrays are bracketed by {@link #startArray}/{@link #endArray}.
 * Elements within arrays use the unnamed {@code arrayElem*} methods.
 * Nested objects and arrays within arrays use the same start/end
 * bracketing methods, with field events delivered as named methods
 * (the handler tracks nesting context).
 *
 * <p><strong>Not thread-safe.</strong> Implementations should assume
 * single-threaded access per walker instance.
 */
public interface JsonDocumentHandler {

    // ---- Object field events (named, at any nesting depth) ----

    /** Descends into a named nested object. Pair with {@link #endObject()}. */
    void startObject(String fieldName);

    /** Ascends from a nested object. */
    void endObject();

    /** An empty object ({@code {}}) as a field value. */
    void emptyObject(String fieldName);

    /** A string field. The bytes are UTF-8, already unescaped. */
    void stringField(String fieldName, byte[] buf, int off, int len);

    /**
     * An integer or long field. {@code fitsInt} is true if the value fits in a Java int.
     * The raw source bytes ({@code srcBuf[srcOff..srcOff+srcLen)}) contain the original
     * JSON text of the number, for callers that need the unparsed form.
     */
    void longField(String fieldName, long value, boolean fitsInt, byte[] srcBuf, int srcOff, int srcLen);

    /**
     * An integer field whose value exceeds the range of {@code long}. The raw source bytes
     * ({@code srcBuf[srcOff..srcOff+srcLen)}) contain the original JSON text.
     */
    void bigIntegerField(String fieldName, BigInteger value, byte[] srcBuf, int srcOff, int srcLen);

    /**
     * A float or double field. {@code fitsFloat} is true if {@code (float)value == value}.
     * The raw source bytes ({@code srcBuf[srcOff..srcOff+srcLen)}) contain the original
     * JSON text of the number, for callers that need the unparsed form.
     */
    void doubleField(String fieldName, double value, boolean fitsFloat, byte[] srcBuf, int srcOff, int srcLen);

    /**
     * A boolean field. The raw source bytes ({@code srcBuf[srcOff..srcOff+srcLen)})
     * contain the original JSON text ({@code "true"} or {@code "false"}).
     */
    void booleanField(String fieldName, boolean value, byte[] srcBuf, int srcOff, int srcLen);

    /** A null field. */
    void nullField(String fieldName);

    // ---- Array bracketing ----

    /** Begins an array value for a named field. Pair with {@link #endArray()}. */
    void startArray(String fieldName);

    /** Ends the current array. */
    void endArray();

    // ---- Array element events (unnamed, within startArray/endArray) ----

    /** A string element inside an array. The bytes are UTF-8, already unescaped. */
    void arrayElemString(byte[] buf, int off, int len);

    /** An integer or long element inside an array. */
    void arrayElemLong(long value, boolean fitsInt);

    /** An integer element inside an array whose value exceeds the range of {@code long}. */
    void arrayElemBigInteger(BigInteger value);

    /** A float or double element inside an array. */
    void arrayElemDouble(double value, boolean fitsFloat);

    /** A boolean element inside an array ({@code true} or {@code false}). */
    void arrayElemBoolean(boolean value);

    /** A null element inside an array. */
    void arrayElemNull();

    /** Begins a nested object element within an array. Pair with {@link #arrayElemEndObject()}. */
    void arrayElemStartObject();

    /** Ends a nested object element within an array. */
    void arrayElemEndObject();

    /** Begins a nested array element within an array. Pair with {@link #arrayElemEndArray()}. */
    void arrayElemStartArray();

    /** Ends a nested array element within an array. */
    void arrayElemEndArray();
}
