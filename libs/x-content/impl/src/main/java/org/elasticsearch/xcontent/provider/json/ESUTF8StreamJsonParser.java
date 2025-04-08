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
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.json.UTF8StreamJsonParser;
import com.fasterxml.jackson.core.sym.ByteQuadsCanonicalizer;

import org.elasticsearch.xcontent.ESBytesRef;

import java.io.IOException;
import java.io.InputStream;

public class ESUTF8StreamJsonParser extends UTF8StreamJsonParser {
    public ESUTF8StreamJsonParser(IOContext ctxt, int features, InputStream in, ObjectCodec codec, ByteQuadsCanonicalizer sym, byte[] inputBuffer, int start, int end, int bytesPreProcessed, boolean bufferRecyclable) {
        super(ctxt, features, in, codec, sym, inputBuffer, start, end, bytesPreProcessed, bufferRecyclable);
    }

    public ESBytesRef getValueAsByteRef() throws IOException  {
        if (_currToken == JsonToken.VALUE_STRING && _tokenIncomplete) {
            var value = _finishAndReturnByteRef();
            if(value != null) {
                _tokenIncomplete = false;
            }
            return value;
        }
        return null;
    }

    protected ESBytesRef _finishAndReturnByteRef() throws IOException {
        int ptr = _inputPtr;
        if(ptr >= _inputEnd) {
            _loadMoreGuaranteed();
            ptr = _inputPtr;
        }

        int startPtr = ptr;
        final int[] codes = INPUT_CODES_UTF8;
        final int max = _inputEnd;
        final byte[] inputBuffer = _inputBuffer;
        while(ptr < max) {
            int c = inputBuffer[ptr] & 0xFF;
            if(codes[c] != 0) {
                if(c == INT_QUOTE) {
                    _inputPtr = ptr + 1;
                    return new ESBytesRef(inputBuffer, startPtr, ptr);
                }
                return null;
            }
            ++ptr;
        }
        return null;
    }
}
