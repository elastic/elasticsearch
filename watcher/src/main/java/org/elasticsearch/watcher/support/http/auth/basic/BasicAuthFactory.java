/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http.auth.basic;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.support.http.auth.HttpAuthFactory;
import org.elasticsearch.watcher.support.secret.SecretService;

import java.io.IOException;

/**
 *
 */
public class BasicAuthFactory extends HttpAuthFactory<BasicAuth, ApplicableBasicAuth> {

    private final SecretService secretService;

    @Inject
    public BasicAuthFactory(SecretService secretService) {
        this.secretService = secretService;
    }

    public String type() {
        return BasicAuth.TYPE;
    }

    public BasicAuth parse(XContentParser parser) throws IOException {
        return BasicAuth.parse(parser);
    }

    @Override
    public ApplicableBasicAuth createApplicable(BasicAuth auth) {
        return new ApplicableBasicAuth(auth, secretService);
    }
}
