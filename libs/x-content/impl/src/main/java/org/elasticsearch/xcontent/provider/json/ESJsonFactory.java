/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.io.IOContext;

import java.io.IOException;
import java.io.InputStream;

public class ESJsonFactory extends JsonFactory {
    ESJsonFactory(JsonFactoryBuilder b) {
        super(b);
    }

    @Override
    protected JsonParser _createParser(InputStream in, IOContext ctxt) throws IOException {
        try {
            return new ESByteSourceJsonBootstrapper(ctxt, in).constructParser(
                _parserFeatures,
                _objectCodec,
                _byteSymbolCanonicalizer,
                _rootCharSymbols,
                _factoryFeatures
            );
        } catch (IOException | RuntimeException e) {
            // 10-Jun-2022, tatu: For [core#763] may need to close InputStream here
            if (ctxt.isResourceManaged()) {
                try {
                    in.close();
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                }
            }
            ctxt.close();
            throw e;
        }
    }

    @Override
    protected JsonParser _createParser(byte[] data, int offset, int len, IOContext ctxt) throws IOException {
        return new ESByteSourceJsonBootstrapper(ctxt, data, offset, len).constructParser(
            _parserFeatures,
            _objectCodec,
            _byteSymbolCanonicalizer,
            _rootCharSymbols,
            _factoryFeatures
        );
    }
}
