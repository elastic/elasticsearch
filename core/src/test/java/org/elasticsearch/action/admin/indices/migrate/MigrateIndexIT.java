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

package org.elasticsearch.action.admin.indices.migrate;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.index.MigrateIndexTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MigrateIndexIT extends MigrateIndexTestCase {
    // Also see the superclass for test methods
    public void testNonEmptyIndexFails() throws InterruptedException, ExecutionException {
        int docCount = between(1, 1000);
        List<IndexRequestBuilder> docs = new ArrayList<>(docCount);
        for (int i = 0; i < docCount; i++) {
            docs.add(client().prepareIndex("test_0", "test").setSource("foo", "bar", "i", i));
        }
        indexRandom(true, docs);
        client().admin().indices().prepareAliases().addAlias("test_0", "test").get();

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> client().admin().indices()
                .prepareMigrateIndex("test_0", "test_1").setAliases("test").get());
        assertEquals(
                "Without the reindex module Elasticsearch can only migrate from empty indexes and [test_0] does not appear to be empty.",
                e.getMessage());
    }
}
