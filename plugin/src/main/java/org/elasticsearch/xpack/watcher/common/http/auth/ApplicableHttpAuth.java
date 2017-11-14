/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.common.http.auth;

import org.apache.http.auth.AuthScope;
import org.apache.http.client.CredentialsProvider;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.HttpURLConnection;

public abstract class ApplicableHttpAuth<Auth extends HttpAuth> implements ToXContentObject {

    protected final Auth auth;

    public ApplicableHttpAuth(Auth auth) {
        this.auth = auth;
    }

    public final String type() {
        return auth.type();
    }

    public abstract void apply(HttpURLConnection connection);

    public abstract void apply(CredentialsProvider credsProvider, AuthScope authScope);

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return auth.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ApplicableHttpAuth<?> that = (ApplicableHttpAuth<?>) o;

        return auth.equals(that.auth);
    }

    @Override
    public int hashCode() {
        return auth.hashCode();
    }
}
