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

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import static java.util.Collections.emptySet;
import static org.mockito.Mockito.mock;

/**
 * Tests the "pre-flight" checks that {@link TransportMigrateIndexAction} takes before attempting to coalesce the request.
 */
public class TransportMigrateIndexActionPreflightTests extends ESTestCase {
    private ThreadPool threadPool;
    private TransportMigrateIndexAction action;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        action = new TransportMigrateIndexAction(Settings.EMPTY, mock(TransportService.class), null, threadPool,
                new ActionFilters(emptySet()), new IndexNameExpressionResolver(Settings.EMPTY), null);
    }

    @After
    public void after() {
        threadPool.shutdown();
    }

    public void testIndexDoesNotExist() {
        assertTrue(action.preflightChecks(new CreateIndexRequest("test"), MetaData.builder().build()));
    }

    public void testIndexExistsAndHasAlias() {
        assertFalse(action.preflightChecks(
                new CreateIndexRequest("test")
                    .alias(new Alias("alias")),
                MetaData.builder()
                        .put(index("test").putAlias(AliasMetaData.builder("alias")))
                        .build()
        ));
    }

    public void testIndexExistsNoAliasRequired() {
        assertFalse(action.preflightChecks(
                new CreateIndexRequest("test"),
                MetaData.builder()
                        .put(index("test"))
                        .build()
        ));
    }

    public void testIndexExistsAndHasAliasWhichHasRouting() {
        assertFalse(action.preflightChecks(
                new CreateIndexRequest("test")
                    .alias(new Alias("alias").searchRouting("asdf").indexRouting("qwer")),
                MetaData.builder()
                        .put(index("test").putAlias(AliasMetaData.builder("alias").searchRouting("asdf").indexRouting("qwer")))
                        .build()
        ));
    }

    public void testIndexIsAnAlias() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.preflightChecks(
                new CreateIndexRequest("test"),
                MetaData.builder()
                        .put(index("test_index").putAlias(AliasMetaData.builder("test")))
                        .build()
        ));
        assertEquals("[test] doesn't exist but an alias of the same name does", e.getMessage());
    }

    public void testIndexExistsButDoesntHaveTheAlias() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.preflightChecks(
                new CreateIndexRequest("test").alias(new Alias("alias")),
                MetaData.builder()
                        .put(index("test"))
                        .build()
        ));
        assertEquals("[test] already exists but doesn't have the [alias] alias", e.getMessage());
    }

    public void testIndexExistsButDoesntHaveAnAlias() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.preflightChecks(
                new CreateIndexRequest("test")
                    .alias(new Alias("alias"))
                    .alias(new Alias("alias2")),
                MetaData.builder()
                        .put(index("test").putAlias(AliasMetaData.builder("alias")))
                        .build()
        ));
        assertEquals("[test] already exists but doesn't have the [alias2] alias", e.getMessage());
    }

    public void testIndexButAliasDoesNotHaveSearchRouting() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.preflightChecks(
                new CreateIndexRequest("test")
                    .alias(new Alias("alias").searchRouting("asdf")),
                MetaData.builder()
                        .put(index("test").putAlias(AliasMetaData.builder("alias")))
                        .build()
        ));
        assertEquals("[test] already exists and has the [alias] alias but the search routing doesn't match. Expected [asdf] but got [null]",
                e.getMessage());
    }

    public void testIndexButAliasDoesNotHaveIndexRouting() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.preflightChecks(
                new CreateIndexRequest("test")
                    .alias(new Alias("alias").indexRouting("asdf")),
                MetaData.builder()
                        .put(index("test").putAlias(AliasMetaData.builder("alias")))
                        .build()
        ));
        assertEquals("[test] already exists and has the [alias] alias but the index routing doesn't match. Expected [asdf] but got [null]",
                e.getMessage());
    }

    private IndexMetaData.Builder index(String name) {
        return IndexMetaData.builder(name)
                .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(1);
    }
}
