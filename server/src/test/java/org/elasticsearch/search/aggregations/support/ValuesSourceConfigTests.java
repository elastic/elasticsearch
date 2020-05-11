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

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.ESSingleNodeTestCase;

// TODO: This whole set of tests needs to be rethought.
public class ValuesSourceConfigTests extends ESSingleNodeTestCase {

    public void testKeyword() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type",
                "bytes", "type=keyword");
        client().prepareIndex("index").setId("1")
                .setSource("bytes", "abc")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                    context, null, "bytes", null, null, null, null, CoreValuesSourceType.BYTES, null);
            ValuesSource.Bytes valuesSource = (ValuesSource.Bytes) config.toValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(new BytesRef("abc"), values.nextValue());
        }
    }

    public void testEmptyKeyword() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type",
                "bytes", "type=keyword");
        client().prepareIndex("index").setId("1")
                .setSource()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                    context, null, "bytes", null, null, null, null, CoreValuesSourceType.BYTES, null);
            ValuesSource.Bytes valuesSource = (ValuesSource.Bytes) config.toValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
            assertFalse(values.advanceExact(0));

            config = ValuesSourceConfig.resolve(
                    context, null, "bytes", null, "abc", null, null, CoreValuesSourceType.BYTES, null);
            valuesSource = (ValuesSource.Bytes) config.toValuesSource();
            values = valuesSource.bytesValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(new BytesRef("abc"), values.nextValue());
        }
    }

    public void testUnmappedKeyword() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type");
        client().prepareIndex("index").setId("1")
                .setSource()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);
            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                    context, ValueType.STRING, "bytes", null, null, null, null, CoreValuesSourceType.BYTES, null);
            ValuesSource.Bytes valuesSource = (ValuesSource.Bytes) config.toValuesSource();
            assertNull(valuesSource);

            config = ValuesSourceConfig.resolve(
                    context, ValueType.STRING, "bytes", null, "abc", null, null, CoreValuesSourceType.BYTES, null);
            valuesSource = (ValuesSource.Bytes) config.toValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(new BytesRef("abc"), values.nextValue());
        }
    }

    public void testLong() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type",
                "long", "type=long");
        client().prepareIndex("index").setId("1")
                .setSource("long", 42)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                    context, null, "long", null, null, null, null, CoreValuesSourceType.BYTES, null);
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.toValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(42, values.nextValue());
        }
    }

    public void testEmptyLong() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type",
                "long", "type=long");
        client().prepareIndex("index").setId("1")
                .setSource()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                    context, null, "long", null, null, null, null, CoreValuesSourceType.BYTES, null);
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.toValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertFalse(values.advanceExact(0));

            config = ValuesSourceConfig.resolve(
                    context, null, "long", null, 42, null, null, CoreValuesSourceType.BYTES, null);
            valuesSource = (ValuesSource.Numeric) config.toValuesSource();
            values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(42, values.nextValue());
        }
    }

    public void testUnmappedLong() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type");
        client().prepareIndex("index").setId("1")
                .setSource()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                    context, ValueType.NUMBER, "long", null, null, null, null, CoreValuesSourceType.BYTES, null);
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.toValuesSource();
            assertNull(valuesSource);

            config = ValuesSourceConfig.resolve(
                    context, ValueType.NUMBER, "long", null, 42, null, null, CoreValuesSourceType.BYTES, null);
            valuesSource = (ValuesSource.Numeric) config.toValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(42, values.nextValue());
        }
    }

    public void testBoolean() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type",
                "bool", "type=boolean");
        client().prepareIndex("index").setId("1")
                .setSource("bool", true)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                    context, null, "bool", null, null, null, null, CoreValuesSourceType.BYTES, null);
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.toValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(1, values.nextValue());
        }
    }

    public void testEmptyBoolean() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type",
                "bool", "type=boolean");
        client().prepareIndex("index").setId("1")
                .setSource()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                    context, null, "bool", null, null, null, null, CoreValuesSourceType.BYTES, null);
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.toValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertFalse(values.advanceExact(0));

            config = ValuesSourceConfig.resolve(
                    context, null, "bool", null, true, null, null, CoreValuesSourceType.BYTES, null);
            valuesSource = (ValuesSource.Numeric) config.toValuesSource();
            values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(1, values.nextValue());
        }
    }

    public void testUnmappedBoolean() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type");
        client().prepareIndex("index").setId("1")
                .setSource()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                    context, ValueType.BOOLEAN, "bool", null, null, null, null, CoreValuesSourceType.BYTES, null);
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.toValuesSource();
            assertNull(valuesSource);

            config = ValuesSourceConfig.resolve(
                    context, ValueType.BOOLEAN, "bool", null, true, null, null, CoreValuesSourceType.BYTES, null);
            valuesSource = (ValuesSource.Numeric) config.toValuesSource();
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(1, values.nextValue());
        }
    }

    public void testTypeFieldDeprecation() {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type");
        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                context, null, TypeFieldMapper.NAME, null, null, null, null, CoreValuesSourceType.BYTES, null);
            assertWarnings(QueryShardContext.TYPES_DEPRECATION_MESSAGE);
        }
    }

    public void testFieldAlias() throws Exception {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type",
            "field", "type=keyword", "alias", "type=alias,path=field");
        client().prepareIndex("index").setId("1")
            .setSource("field", "value")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);
            ValuesSourceConfig config = ValuesSourceConfig.resolve(
                context, ValueType.STRING, "alias", null, null, null, null, CoreValuesSourceType.BYTES, null);
            ValuesSource.Bytes valuesSource = (ValuesSource.Bytes) config.toValuesSource();

            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(new BytesRef("value"), values.nextValue());
        }
    }
}
