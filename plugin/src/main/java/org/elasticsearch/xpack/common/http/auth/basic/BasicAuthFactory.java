/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.common.http.auth.basic;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.common.http.auth.HttpAuthFactory;
import org.elasticsearch.xpack.security.crypto.CryptoService;

import java.io.IOException;

public class BasicAuthFactory extends HttpAuthFactory<BasicAuth, ApplicableBasicAuth> {

    private final CryptoService cryptoService;

    public BasicAuthFactory(@Nullable CryptoService cryptoService) {
        this.cryptoService = cryptoService;
    }

    public String type() {
        return BasicAuth.TYPE;
    }

    public BasicAuth parse(XContentParser parser) throws IOException {
        return BasicAuth.parse(parser);
    }

    @Override
    public ApplicableBasicAuth createApplicable(BasicAuth auth) {
        return new ApplicableBasicAuth(auth, cryptoService);
    }
}
