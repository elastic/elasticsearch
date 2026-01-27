/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.content;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

class CborFactory {

    private static final CBORFactory cborFactory;

    static {
        cborFactory = new CBORFactory();
        cborFactory.configure(CBORFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false); // this trips on many mappings now...
        // Do not automatically close unclosed objects/arrays in com.fasterxml.jackson.dataformat.cbor.CBORGenerator#close() method
        cborFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
        cborFactory.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true);
    }

    public static JsonGenerator generator(OutputStream out) throws IOException {
        return cborFactory.createGenerator(out, JsonEncoding.UTF8);
    }

    public static JsonParser parser(InputStream in) throws IOException {
        return cborFactory.createParser(in);
    }
}
