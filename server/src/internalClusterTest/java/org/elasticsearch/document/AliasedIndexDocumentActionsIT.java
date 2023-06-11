/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.document;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;

public class AliasedIndexDocumentActionsIT extends DocumentActionsIT {

    @Override
    protected void createIndex() {
        logger.info("Creating index [test1] with alias [test]");
        try {
            client().admin().indices().prepareDelete("test1").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        logger.info("--> creating index test");
        indicesAdmin().create(new CreateIndexRequest("test1").simpleMapping("name", "type=keyword,store=true").alias(new Alias("test")))
            .actionGet();
    }

    @Override
    protected String getConcreteIndexName() {
        return "test1";
    }
}
