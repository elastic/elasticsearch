/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/FasterXML/jackson-dataformats-binary which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.xcontent.provider.cbor;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.sym.ByteQuadsCanonicalizer;
import com.fasterxml.jackson.dataformat.cbor.CBORConstants;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.provider.OptimizedTextCapable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;

/**
 * Contains code adapted from {@link CBORParser} licensed under the Apache License 2.0.
 */
public class ESCborParser extends CBORParser implements OptimizedTextCapable {
    public ESCborParser(
        IOContext ctxt,
        int parserFeatures,
        int cborFeatures,
        ObjectCodec codec,
        ByteQuadsCanonicalizer sym,
        InputStream in,
        byte[] inputBuffer,
        int start,
        int end,
        boolean bufferRecyclable
    ) {
        super(ctxt, parserFeatures, cborFeatures, codec, sym, in, inputBuffer, start, end, bufferRecyclable);
    }

    @Override
    public Text getValueAsText() throws IOException {
        JsonToken t = _currToken;
        if (_tokenIncomplete) {
            if (t == JsonToken.VALUE_STRING) {
                return _finishAndReturnText(_typeByte);
            }
        }
        return null;
    }

    private Text _finishAndReturnText(int ch) throws IOException {
        final int type = ((ch >> 5) & 0x7);
        ch &= 0x1F;

        // sanity check
        if (type != CBORConstants.MAJOR_TYPE_TEXT) {
            // should never happen so
            _throwInternal();
        }
        int previousPointer = _inputPtr;

        // String value, decode
        final int len = _decodeExplicitLength(ch);
        if (len == 0) {
            return new Text(new XContentString.UTF8Bytes(new byte[0], 0, 0), 0);
        }
        if (len < 0) {
            // optimized text is not supported for chunked strings
            return null;
        }
        final int available = _inputEnd - _inputPtr;
        if (available >= len) {
            Text text = new Text(new XContentString.UTF8Bytes(_inputBuffer, _inputPtr, len));
            _inputPtr = previousPointer;
            return text;
        }
        // this is expected to be used in the context where the input stream is not available
        return null;
    }

    /**
     * Method used to decode explicit length of a variable-length value
     * (or, for indefinite/chunked, indicate that one is not known).
     * Note that long (64-bit) length is only allowed if it fits in
     * 32-bit signed int, for now; expectation being that longer values
     * are always encoded as chunks.
     */
    private int _decodeExplicitLength(int lowBits) throws IOException {
        // common case, indefinite length; relies on marker
        if (lowBits == 31) {
            return -1;
        }
        if (lowBits <= 23) {
            return lowBits;
        }
        switch (lowBits - 24) {
            case 0:
                return _decode8Bits();
            case 1:
                return _decode16Bits();
            case 2:
                return _decode32Bits();
            case 3:
                long l = _decode64Bits();
                if (l < 0 || l > MAX_INT_L) {
                    throw _constructError("Illegal length for " + currentToken() + ": " + l);
                }
                return (int) l;
        }
        throw _constructError(
            String.format(
                Locale.ROOT,
                "Invalid 5-bit length indicator for `JsonToken.%s`: 0x%02X; only 0x00-0x17, 0x1F allowed",
                currentToken(),
                lowBits
            )
        );
    }

    private int _decode8Bits() throws IOException {
        if (_inputPtr >= _inputEnd) {
            loadMoreGuaranteed();
        }
        return _inputBuffer[_inputPtr++] & 0xFF;
    }

    private int _decode16Bits() throws IOException {
        int ptr = _inputPtr;
        if ((ptr + 1) >= _inputEnd) {
            return _slow16();
        }
        final byte[] b = _inputBuffer;
        int v = ((b[ptr] & 0xFF) << 8) + (b[ptr + 1] & 0xFF);
        _inputPtr = ptr + 2;
        return v;
    }

    private int _slow16() throws IOException {
        if (_inputPtr >= _inputEnd) {
            loadMoreGuaranteed();
        }
        int v = (_inputBuffer[_inputPtr++] & 0xFF);
        if (_inputPtr >= _inputEnd) {
            loadMoreGuaranteed();
        }
        return (v << 8) + (_inputBuffer[_inputPtr++] & 0xFF);
    }

    private int _decode32Bits() throws IOException {
        int ptr = _inputPtr;
        if ((ptr + 3) >= _inputEnd) {
            return _slow32();
        }
        final byte[] b = _inputBuffer;
        int v = (b[ptr++] << 24) + ((b[ptr++] & 0xFF) << 16) + ((b[ptr++] & 0xFF) << 8) + (b[ptr++] & 0xFF);
        _inputPtr = ptr;
        return v;
    }

    private int _slow32() throws IOException {
        if (_inputPtr >= _inputEnd) {
            loadMoreGuaranteed();
        }
        int v = _inputBuffer[_inputPtr++]; // sign will disappear anyway
        if (_inputPtr >= _inputEnd) {
            loadMoreGuaranteed();
        }
        v = (v << 8) + (_inputBuffer[_inputPtr++] & 0xFF);
        if (_inputPtr >= _inputEnd) {
            loadMoreGuaranteed();
        }
        v = (v << 8) + (_inputBuffer[_inputPtr++] & 0xFF);
        if (_inputPtr >= _inputEnd) {
            loadMoreGuaranteed();
        }
        return (v << 8) + (_inputBuffer[_inputPtr++] & 0xFF);
    }

    private long _decode64Bits() throws IOException {
        int ptr = _inputPtr;
        if ((ptr + 7) >= _inputEnd) {
            return _slow64();
        }
        final byte[] b = _inputBuffer;
        int i1 = (b[ptr++] << 24) + ((b[ptr++] & 0xFF) << 16) + ((b[ptr++] & 0xFF) << 8) + (b[ptr++] & 0xFF);
        int i2 = (b[ptr++] << 24) + ((b[ptr++] & 0xFF) << 16) + ((b[ptr++] & 0xFF) << 8) + (b[ptr++] & 0xFF);
        _inputPtr = ptr;
        return _long(i1, i2);
    }

    private long _slow64() throws IOException {
        return _long(_decode32Bits(), _decode32Bits());
    }

    private static long _long(int i1, int i2) {
        long l1 = i1;
        long l2 = i2;
        l2 = (l2 << 32) >>> 32;
        return (l1 << 32) + l2;
    }
}
