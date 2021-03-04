/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.wildfly.transport;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;

import javax.enterprise.inject.Produces;
import java.nio.file.Path;

@SuppressWarnings("unused")
public final class RestHighLevelClientProducer {

    @Produces
    public RestHighLevelClient createRestHighLevelClient() {
        String httpUri = System.getProperty("elasticsearch.uri");

        return new RestHighLevelClient(RestClient.builder(HttpHost.create(httpUri)));
    }

    @SuppressForbidden(reason = "get path not configured in environment")
    private Path getPath(final String elasticsearchProperties) {
        return PathUtils.get(elasticsearchProperties);
    }
}
