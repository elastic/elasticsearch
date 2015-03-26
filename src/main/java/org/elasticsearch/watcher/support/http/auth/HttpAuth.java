/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http.auth;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.net.HttpURLConnection;

public abstract class HttpAuth implements ToXContent {

    public abstract String type();

    public abstract void update(HttpURLConnection connection);

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(type());
        builder = innerToXContent(builder, params);
        return builder.endObject();
    }

    public abstract XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException;

    public static interface Parser<Auth extends HttpAuth> {

        String type();

        Auth parse(XContentParser parser) throws IOException;

    }

}
