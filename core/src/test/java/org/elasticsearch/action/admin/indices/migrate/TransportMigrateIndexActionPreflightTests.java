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
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static java.util.Collections.emptySet;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
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
                new ActionFilters(emptySet()), new IndexNameExpressionResolver(Settings.EMPTY), null, null);
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

    public void testIndexExistsAndHasAliasWithHasFilter() throws IOException {
        QueryBuilder filter = termQuery("test", "foo");
        String expectedFilter = filter.toString(); // json
        // actual is encoded as whatever, probably not json
        XContentBuilder builder = XContentBuilder.builder(randomFrom(XContentType.values()).xContent());
        builder.prettyPrint();
        filter.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertFalse(action.preflightChecks(
                new CreateIndexRequest("test")
                    .alias(new Alias("alias").filter(expectedFilter)),
                MetaData.builder()
                    .put(index("test").putAlias(AliasMetaData.builder("alias").filter(new CompressedXContent(builder.bytes()))))
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

    public void testIndexExistsButAliasDoesNotHaveSearchRouting() {
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

    public void testIndexExistsButAliasDoesNotHaveIndexRouting() {
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

    public void testIndexExistsButAliasDoesNotHaveMatchingFilters() {
        String filter = termQuery("test", "text").toString();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.preflightChecks(
                new CreateIndexRequest("test")
                    .alias(new Alias("alias").filter(filter)),
                MetaData.builder()
                        .put(index("test").putAlias(AliasMetaData.builder("alias").filter("{\"test\":\"bar\"}")))
                        .build()
        ));
        assertEquals("[test] already exists and has the [alias] alias but the filter doesn't match. Expected "
                + "[" + filter + "] but got [{\"test\":\"bar\"}]",
                e.getMessage());
    }

    public void testIndexExistsButAliasDoesNotHaveMatchingFiltersDifferentXContent() throws IOException {
        String expectedFilter = termQuery("test", "foo").toString(); // json
        QueryBuilder actualFilter = termQuery("test", "bar"); // encoded as whatever, probably not json
        XContentBuilder builder = XContentBuilder.builder(randomFrom(XContentType.values()).xContent());
        actualFilter.toXContent(builder, ToXContent.EMPTY_PARAMS);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.preflightChecks(
                new CreateIndexRequest("test")
                    .alias(new Alias("alias").filter(expectedFilter)),
                MetaData.builder()
                        .put(index("test").putAlias(AliasMetaData.builder("alias").filter(new CompressedXContent(builder.bytes()))))
                        .build()
        ));
        assertEquals("[test] already exists and has the [alias] alias but the filter doesn't match. Expected "
                + "[" + expectedFilter + "] but got [" + XContentHelper.convertToJson(builder.bytes(), true) + "]",
                e.getMessage());
    }

    public void testIndexExistsButAliasHasFiltersWhenNotRequired() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.preflightChecks(
                new CreateIndexRequest("test")
                    .alias(new Alias("alias")),
                MetaData.builder()
                        .put(index("test").putAlias(AliasMetaData.builder("alias").filter("{\"test\":\"foo\"}")))
                        .build()
        ));
        assertEquals("[test] already exists and has the [alias] alias but the filter doesn't match. Expected "
                + "[null] but got [{\"test\":\"foo\"}]",
                e.getMessage());
    }

    public void testIndexExistsButAliasDoesNotHaveFiltersWhenRequired() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.preflightChecks(
                new CreateIndexRequest("test")
                    .alias(new Alias("alias").filter("{\"test\":\"foo\"}")),
                MetaData.builder()
                        .put(index("test").putAlias(AliasMetaData.builder("alias")))
                        .build()
        ));
        assertEquals("[test] already exists and has the [alias] alias but the filter doesn't match. Expected "
                + "[{\"test\":\"foo\"}] but got [null]",
                e.getMessage());
    }

    private IndexMetaData.Builder index(String name) {
        return IndexMetaData.builder(name)
                .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(1);
    }
}
