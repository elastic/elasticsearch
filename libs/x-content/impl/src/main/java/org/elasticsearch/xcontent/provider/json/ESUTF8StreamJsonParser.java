/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.json;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.json.UTF8StreamJsonParser;
import com.fasterxml.jackson.core.sym.ByteQuadsCanonicalizer;

import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.provider.OptimizedTextCapable;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides the method getValueAsText that is a best-effort optimization for UTF8 fields. If the
 * {@link UTF8StreamJsonParser} has already parsed the text, then the caller should fall back to getText.
 * This is sufficient because when we call getText, jackson stores the parsed UTF-16 value for future calls.
 * Once we've already got the parsed UTF-16 value, the optimization isn't necessary.
 */
public class ESUTF8StreamJsonParser extends UTF8StreamJsonParser implements OptimizedTextCapable {
    protected int stringEnd = -1;
    protected int stringLength;
    protected byte[] lastOptimisedValue;

    private final List<Integer> backslashes = new ArrayList<>();

    public ESUTF8StreamJsonParser(
        IOContext ctxt,
        int features,
        InputStream in,
        ObjectCodec codec,
        ByteQuadsCanonicalizer sym,
        byte[] inputBuffer,
        int start,
        int end,
        int bytesPreProcessed,
        boolean bufferRecyclable
    ) {
        super(ctxt, features, in, codec, sym, inputBuffer, start, end, bytesPreProcessed, bufferRecyclable);
    }

    /**
     * Method that will try to get underlying UTF-8 encoded bytes of the current string token.
     * This is only a best-effort attempt; if there is some reason the bytes cannot be retrieved, this method will return null.
     */
    @Override
    public Text getValueAsText() throws IOException {
        // _tokenIncomplete is true when UTF8StreamJsonParser has already processed this value.
        if (_currToken == JsonToken.VALUE_STRING && _tokenIncomplete) {
            if (lastOptimisedValue != null) {
                return new Text(new XContentString.UTF8Bytes(lastOptimisedValue), stringLength);
            }
            if (stringEnd > 0) {
                final int len = stringEnd - 1 - _inputPtr;
                return new Text(new XContentString.UTF8Bytes(_inputBuffer, _inputPtr, len), stringLength);
            }
            return _finishAndReturnText();
        }
        return null;
    }

    protected Text _finishAndReturnText() throws IOException {
        int ptr = _inputPtr;
        if (ptr >= _inputEnd) {
            _loadMoreGuaranteed();
            ptr = _inputPtr;
        }

        int startPtr = ptr;
        final int[] codes = INPUT_CODES_UTF8;
        final int max = _inputEnd;
        final byte[] inputBuffer = _inputBuffer;
        stringLength = 0;
        backslashes.clear();

        loop: while (true) {
            if (ptr >= max) {
                return null;
            }
            int c = inputBuffer[ptr] & 0xFF;
            switch (codes[c]) {
                case 0 -> {
                    ++ptr;
                    ++stringLength;
                }
                case 1 -> {
                    if (c == INT_QUOTE) {
                        // End of the string
                        break loop;
                    }
                    assert c == INT_BACKSLASH;
                    backslashes.add(ptr);
                    ++ptr;
                    if (ptr >= max) {
                        // Backslash at end of file
                        return null;
                    }
                    c = inputBuffer[ptr] & 0xFF;
                    if (c == '"' || c == '/' || c == '\\') {
                        ptr += 1;
                        stringLength += 1;
                    } else {
                        // Any other escaped sequence requires replacing the sequence with
                        // a new character, which we don't support in the optimized path
                        return null;
                    }
                }
                case 2, 3, 4 -> {
                    int bytesToSkip = codes[c];
                    if (ptr + bytesToSkip > max) {
                        return null;
                    }
                    ptr += bytesToSkip;
                    // Code points that require 4 bytes in UTF-8 will use 2 chars in UTF-16.
                    stringLength += (bytesToSkip == 4 ? 2 : 1);
                }
                default -> {
                    return null;
                }
            }
        }

        stringEnd = ptr + 1;
        if (backslashes.isEmpty()) {
            return new Text(new XContentString.UTF8Bytes(inputBuffer, startPtr, ptr - startPtr), stringLength);
        } else {
            byte[] buff = new byte[ptr - startPtr - backslashes.size()];
            int copyPtr = startPtr;
            int destPtr = 0;
            for (Integer backslash : backslashes) {
                int length = backslash - copyPtr;
                System.arraycopy(inputBuffer, copyPtr, buff, destPtr, length);
                destPtr += length;
                copyPtr = backslash + 1;
            }
            System.arraycopy(inputBuffer, copyPtr, buff, destPtr, ptr - copyPtr);
            lastOptimisedValue = buff;
            return new Text(new XContentString.UTF8Bytes(buff), stringLength);
        }
    }

    @Override
    public JsonToken nextToken() throws IOException {
        resetCurrentTokenState();
        return super.nextToken();
    }

    @Override
    public boolean nextFieldName(SerializableString str) throws IOException {
        resetCurrentTokenState();
        return super.nextFieldName(str);
    }

    @Override
    public String nextFieldName() throws IOException {
        resetCurrentTokenState();
        return super.nextFieldName();
    }

    /**
     * Resets the current token state before moving to the next. It resets the _inputPtr and the
     * _tokenIncomplete only if {@link UTF8StreamJsonParser#getText()} or {@link UTF8StreamJsonParser#getValueAsString()}
     * hasn't run yet.
     */
    private void resetCurrentTokenState() {
        if (_currToken == JsonToken.VALUE_STRING && _tokenIncomplete && stringEnd > 0) {
            _inputPtr = stringEnd;
            _tokenIncomplete = false;
        }
        lastOptimisedValue = null;
        stringEnd = -1;
    }
}
