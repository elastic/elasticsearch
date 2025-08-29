/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.cbor;

import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.sym.ByteQuadsCanonicalizer;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

public class ESCborFactory extends CBORFactory {

    public static ESCborFactoryBuilder builder() {
        return new ESCborFactoryBuilder();
    }

    @Override
    protected CBORParser _createParser(byte[] data, int offset, int len, IOContext ctxt) {
        ByteQuadsCanonicalizer can = _byteSymbolCanonicalizer.makeChildOrPlaceholder(_factoryFeatures);
        return new ESCborParser(ctxt, _parserFeatures, _formatParserFeatures, _objectCodec, can, null, data, offset, offset + len, false);
    }
}
