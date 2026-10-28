/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.json;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.json.ByteSourceJsonBootstrapper;
import com.fasterxml.jackson.core.sym.ByteQuadsCanonicalizer;

import java.io.IOException;

public class ESJsonFactory extends JsonFactory {
    ESJsonFactory(JsonFactoryBuilder b) {
        super(b);
    }

    @Override
    protected JsonParser _createParser(byte[] data, int offset, int len, IOContext ctxt) throws IOException {
        if (len > 0
            && Feature.CHARSET_DETECTION.enabledIn(_factoryFeatures)
            && Feature.CANONICALIZE_FIELD_NAMES.enabledIn(_factoryFeatures)) {
            var bootstrap = new ByteSourceJsonBootstrapper(ctxt, data, offset, len);
            var encoding = bootstrap.detectEncoding();
            if (encoding == JsonEncoding.UTF8) {
                boolean invalidBom = false;
                int ptr = offset;
                // Skip over the BOM if present
                if ((data[ptr] & 0xFF) == 0xEF) {
                    if (len < 3) {
                        invalidBom = true;
                    } else if ((data[ptr + 1] & 0xFF) != 0xBB) {
                        invalidBom = true;
                    } else if ((data[ptr + 2] & 0xFF) != 0xBF) {
                        invalidBom = true;
                    } else {
                        ptr += 3;
                    }
                }
                if (invalidBom == false) {
                    ByteQuadsCanonicalizer can = _byteSymbolCanonicalizer.makeChild(_factoryFeatures);
                    return new ESUTF8StreamJsonParser(
                        ctxt,
                        _parserFeatures,
                        null,
                        _objectCodec,
                        can,
                        data,
                        ptr,
                        offset + len,
                        ptr - offset,
                        false
                    );
                }
            }
        }
        return new ByteSourceJsonBootstrapper(ctxt, data, offset, len).constructParser(
            _parserFeatures,
            _objectCodec,
            _byteSymbolCanonicalizer,
            _rootCharSymbols,
            _factoryFeatures
        );
    }
}
