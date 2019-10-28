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
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.MultiGeoValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

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

            ValuesSourceConfig<ValuesSource.Bytes> config = ValuesSourceConfig.resolve(
                    context, null, "bytes", null, null, null, null);
            ValuesSource.Bytes valuesSource = config.toValuesSource(context);
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

            ValuesSourceConfig<ValuesSource.Bytes> config = ValuesSourceConfig.resolve(
                    context, null, "bytes", null, null, null, null);
            ValuesSource.Bytes valuesSource = config.toValuesSource(context);
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
            assertFalse(values.advanceExact(0));

            config = ValuesSourceConfig.resolve(
                    context, null, "bytes", null, "abc", null, null);
            valuesSource = config.toValuesSource(context);
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
            ValuesSourceConfig<ValuesSource.Bytes> config = ValuesSourceConfig.resolve(
                    context, ValueType.STRING, "bytes", null, null, null, null);
            ValuesSource.Bytes valuesSource = config.toValuesSource(context);
            assertNull(valuesSource);

            config = ValuesSourceConfig.resolve(
                    context, ValueType.STRING, "bytes", null, "abc", null, null);
            valuesSource = config.toValuesSource(context);
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

            ValuesSourceConfig<ValuesSource.Numeric> config = ValuesSourceConfig.resolve(
                    context, null, "long", null, null, null, null);
            ValuesSource.Numeric valuesSource = config.toValuesSource(context);
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

            ValuesSourceConfig<ValuesSource.Numeric> config = ValuesSourceConfig.resolve(
                    context, null, "long", null, null, null, null);
            ValuesSource.Numeric valuesSource = config.toValuesSource(context);
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertFalse(values.advanceExact(0));

            config = ValuesSourceConfig.resolve(
                    context, null, "long", null, 42, null, null);
            valuesSource = config.toValuesSource(context);
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

            ValuesSourceConfig<ValuesSource.Numeric> config = ValuesSourceConfig.resolve(
                    context, ValueType.NUMBER, "long", null, null, null, null);
            ValuesSource.Numeric valuesSource = config.toValuesSource(context);
            assertNull(valuesSource);

            config = ValuesSourceConfig.resolve(
                    context, ValueType.NUMBER, "long", null, 42, null, null);
            valuesSource = config.toValuesSource(context);
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

            ValuesSourceConfig<ValuesSource.Numeric> config = ValuesSourceConfig.resolve(
                    context, null, "bool", null, null, null, null);
            ValuesSource.Numeric valuesSource = config.toValuesSource(context);
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

            ValuesSourceConfig<ValuesSource.Numeric> config = ValuesSourceConfig.resolve(
                    context, null, "bool", null, null, null, null);
            ValuesSource.Numeric valuesSource = config.toValuesSource(context);
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertFalse(values.advanceExact(0));

            config = ValuesSourceConfig.resolve(
                    context, null, "bool", null, true, null, null);
            valuesSource = config.toValuesSource(context);
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

            ValuesSourceConfig<ValuesSource.Numeric> config = ValuesSourceConfig.resolve(
                    context, ValueType.BOOLEAN, "bool", null, null, null, null);
            ValuesSource.Numeric valuesSource = config.toValuesSource(context);
            assertNull(valuesSource);

            config = ValuesSourceConfig.resolve(
                    context, ValueType.BOOLEAN, "bool", null, true, null, null);
            valuesSource = config.toValuesSource(context);
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(1, values.nextValue());
        }
    }

    public void testGeoPoint() throws IOException {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type",
            "geo_point", "type=geo_point");
        client().prepareIndex("index")
            .setSource("geo_point", "-10.0,10.0")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig<ValuesSource.GeoPoint> config = ValuesSourceConfig.resolve(
                context, null, "geo_point", null, null, null, null);
            ValuesSource.GeoPoint valuesSource = config.toValuesSource(context);
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            MultiGeoValues values = valuesSource.geoValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            MultiGeoValues.GeoValue value = values.nextValue();
            assertThat(value.lat(), closeTo(-10, GeoUtils.TOLERANCE));
            assertThat(value.lon(), closeTo(10, GeoUtils.TOLERANCE));
        }
    }

    public void testEmptyGeoPoint() throws IOException {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type",
            "geo_point", "type=geo_point");
        client().prepareIndex("index")
            .setSource()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig<ValuesSource.GeoPoint> config = ValuesSourceConfig.resolve(
                context, null, "geo_point", null, null, null, null);
            ValuesSource.GeoPoint valuesSource = config.toValuesSource(context);
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            MultiGeoValues values = valuesSource.geoValues(ctx);
            assertFalse(values.advanceExact(0));

            config = ValuesSourceConfig.resolve(
                context, null, "geo_point", null, "0,0", null, null);
            valuesSource = config.toValuesSource(context);
            ctx = searcher.getIndexReader().leaves().get(0);
            values = valuesSource.geoValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            MultiGeoValues.GeoValue value = values.nextValue();
            assertThat(value.lat(), closeTo(0, GeoUtils.TOLERANCE));
            assertThat(value.lon(), closeTo(0, GeoUtils.TOLERANCE));
        }
    }

    public void testUnmappedGeoPoint() throws IOException {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type");
        client().prepareIndex("index")
            .setSource()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig<ValuesSource.GeoPoint> config = ValuesSourceConfig.resolve(
                context, ValueType.GEOPOINT, "geo_point", null, null, null, null);
            ValuesSource.GeoPoint valuesSource = config.toValuesSource(context);
            assertNull(valuesSource);

            config = ValuesSourceConfig.resolve(
                context, ValueType.GEOPOINT, "geo_point", null, "0,0", null, null);
            valuesSource = config.toValuesSource(context);
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            MultiGeoValues values = valuesSource.geoValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            MultiGeoValues.GeoValue value = values.nextValue();
            assertThat(value.lat(), closeTo(0, GeoUtils.TOLERANCE));
            assertThat(value.lon(), closeTo(0, GeoUtils.TOLERANCE));
        }
    }

    public void testGeoShape() throws IOException {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type",
            "geo_shape", "type=geo_shape");
        client().prepareIndex("index")
            .setSource("geo_shape", "POINT (-10 10)")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig<ValuesSource.GeoShape> config = ValuesSourceConfig.resolve(
                context, null, "geo_shape", null, null, null, null);
            ValuesSource.GeoShape valuesSource = config.toValuesSource(context);
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            MultiGeoValues values = valuesSource.geoValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            // TODO (talevy): assert value once BoundingBox is defined
//            MultiGeoValues.GeoValue value = values.nextValue();
//            assertThat(value.minX(), closeTo(-10, GeoUtils.TOLERANCE));
//            assertThat(value.minY(), closeTo(10, GeoUtils.TOLERANCE));
        }
    }

    public void testEmptyGeoShape() throws IOException {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type",
            "geo_shape", "type=geo_shape");
        client().prepareIndex("index")
            .setSource()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig<ValuesSource.GeoShape> config = ValuesSourceConfig.resolve(
                context, null, "geo_shape", null, null, null, null);
            ValuesSource.GeoShape valuesSource = config.toValuesSource(context);
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            MultiGeoValues values = valuesSource.geoValues(ctx);
            assertFalse(values.advanceExact(0));

            config = ValuesSourceConfig.resolve(
                context, null, "geo_shape", null, "POINT (0 0)", null, null);
            valuesSource = config.toValuesSource(context);
            ctx = searcher.getIndexReader().leaves().get(0);
            values = valuesSource.geoValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            // TODO (talevy): assert value once BoundingBox is defined
//            MultiGeoValues.GeoValue value = values.nextValue();
//            assertThat(value.minX(), closeTo(-10, GeoUtils.TOLERANCE));
//            assertThat(value.minY(), closeTo(10, GeoUtils.TOLERANCE));

            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> ValuesSourceConfig.resolve(context, ValueType.GEO, "geo_shapes", null, "invalid",
                    null, null).toValuesSource(context));
            assertThat(exception.getMessage(), equalTo("Unknown geometry type: invalid"));
        }
    }

    public void testUnmappedGeoShape() throws IOException {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type");
        client().prepareIndex("index")
            .setSource()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig<ValuesSource.GeoShape> config = ValuesSourceConfig.resolve(
                context, ValueType.GEOSHAPE, "geo_shape", null, null, null, null);
            ValuesSource.GeoShape valuesSource = config.toValuesSource(context);
            assertNull(valuesSource);

            config = ValuesSourceConfig.resolve(
                context, ValueType.GEOSHAPE, "geo_shape", null, "POINT (0 0)", null, null);
            valuesSource = config.toValuesSource(context);
            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            MultiGeoValues values = valuesSource.geoValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            // TODO (talevy): assert value once BoundingBox is defined
//            MultiGeoValues.GeoValue value = values.nextValue();
//            assertThat(value.minX(), closeTo(-10, GeoUtils.TOLERANCE));
//            assertThat(value.minY(), closeTo(10, GeoUtils.TOLERANCE));
        }
    }

    public void testTypeFieldDeprecation() {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type");
        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            ValuesSourceConfig<ValuesSource.Bytes> config = ValuesSourceConfig.resolve(
                context, null, TypeFieldMapper.NAME, null, null, null, null);
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
            ValuesSourceConfig<ValuesSource.Bytes> config = ValuesSourceConfig.resolve(
                context, ValueType.STRING, "alias", null, null, null, null);
            ValuesSource.Bytes valuesSource = config.toValuesSource(context);

            LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
            SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(new BytesRef("value"), values.nextValue());
        }
    }
}
