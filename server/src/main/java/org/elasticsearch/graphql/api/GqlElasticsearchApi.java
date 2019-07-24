/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.graphql.api;

import static org.elasticsearch.graphql.api.GqlApiUtils.logResult;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;

import org.elasticsearch.graphql.api.resolver.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class GqlElasticsearchApi implements GqlApi {
    private static final Logger logger = LogManager.getLogger(GqlElasticsearchApi.class);

    NodeClient client;

    public GqlElasticsearchApi(NodeClient client) {
        this.client = client;
    }

    @Override
    public CompletableFuture<Map<String, Object>> getInfo() throws Exception {
        logger.info("getInfo");
        return ResolverGetInfo.exec(client)
            .thenApply(logResult(logger, "getInfo"));
    }

    @Override
    public CompletableFuture<List<Object>> getIndexInfos() throws Exception {
        logger.info("getIndexInfos");
        return ResolverGetIndexInfos.exec(client)
            .thenApply(logResult(logger, "getIndexInfos"));
    }

    @Override
    public CompletableFuture<Map<String, Object>> getIndex(String indexName) throws Exception {
        logger.info("getIndex [indexName = {}]", indexName);
        return ResolverGetIndex.exec(client, indexName)
            .thenApply(logResult(logger, "getIndex"));
    }

    @Override
    public CompletableFuture<Map<String, Object>> getDocument(String indexName, String documentId) throws Exception {
        logger.info("getDocument [indexName = {}, documentId]", indexName, documentId);
        return ResolverGetDocument.exec(client, indexName, documentId)
            .thenApply(logResult(logger, "getDocument"));
    }


    @Override
    public CompletableFuture<Map<String, Object>> getNode(String nodeIdOrName) throws Exception {
        logger.info("getNode [nodeIdOrName = {}]", nodeIdOrName);
        return ResolverGetNode.exec(client, nodeIdOrName)
            .thenApply(logResult(logger, "getNode"));
    }
}
