/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.common.http.auth;

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public abstract class HttpAuthFactory<Auth extends HttpAuth, AAuth extends ApplicableHttpAuth<Auth>> {

    public abstract String type();

    public abstract Auth parse(XContentParser parser) throws IOException;

    public abstract AAuth createApplicable(Auth auth);

    public AAuth parseApplicable(XContentParser parser) throws IOException {
        Auth auth = parse(parser);
        return createApplicable(auth);
    }

}
