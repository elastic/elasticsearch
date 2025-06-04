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

import java.io.IOException;
import java.io.InputStream;

public class ESUTF8StreamJsonParser extends UTF8StreamJsonParser {
    protected int stringEnd = -1;

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
     * Currently, this is only implemented for ascii-only strings that do not contain escaped characters.
     */
    public Text getValueAsText() throws IOException {
        if (_currToken == JsonToken.VALUE_STRING && _tokenIncomplete) {
            if (stringEnd > 0) {
                final int len = stringEnd - 1 - _inputPtr;
                // For now, we can use `len` for `stringLength` because we only support ascii-encoded unescaped strings,
                // which means each character uses exactly 1 byte.
                return new Text(new XContentString.UTF8Bytes(_inputBuffer, _inputPtr, len), len);
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
        while (ptr < max) {
            int c = inputBuffer[ptr] & 0xFF;
            if (codes[c] != 0) {
                if (c == INT_QUOTE) {
                    stringEnd = ptr + 1;
                    final int len = ptr - startPtr;
                    // For now, we can use `len` for `stringLength` because we only support ascii-encoded unescaped strings,
                    // which means each character uses exactly 1 byte.
                    return new Text(new XContentString.UTF8Bytes(inputBuffer, startPtr, len), len);
                }
                return null;
            }
            ++ptr;
        }
        return null;
    }

    @Override
    public JsonToken nextToken() throws IOException {
        if (_currToken == JsonToken.VALUE_STRING && _tokenIncomplete && stringEnd > 0) {
            _inputPtr = stringEnd;
            _tokenIncomplete = false;
        }
        stringEnd = -1;
        return super.nextToken();
    }

    @Override
    public boolean nextFieldName(SerializableString str) throws IOException {
        if (_currToken == JsonToken.VALUE_STRING && _tokenIncomplete && stringEnd > 0) {
            _inputPtr = stringEnd;
            _tokenIncomplete = false;
        }
        stringEnd = -1;
        return super.nextFieldName(str);
    }

    @Override
    public String nextFieldName() throws IOException {
        if (_currToken == JsonToken.VALUE_STRING && _tokenIncomplete && stringEnd > 0) {
            _inputPtr = stringEnd;
            _tokenIncomplete = false;
        }
        stringEnd = -1;
        return super.nextFieldName();
    }
}
