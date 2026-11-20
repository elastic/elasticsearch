/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.DummyBlockLoaderContext;
import org.elasticsearch.index.mapper.GeoShapeQueryable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.spatial.GeometryDocValueFormat;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;

/**
 * The doc values of {@code geo_shape} and {@code shape} fields don't record their format, which follows the version the
 * index was created on, so every reader of them must be created for that format: the doc-values query, the fielddata
 * behind aggregations and the ES|QL block loaders, including grid functions fused into the loader. The same shapes
 * indexed before and after {@link IndexVersions#GEOMETRY_DOC_VALUES_V2} must therefore read identically. Points matter
 * most, since their V2 layout differs from the legacy one even in the header.
 */
public class ShapeDocValuesIndexVersionTests extends MapperServiceTestCase {

    /**
     * A shape with a box it intersects, and a box it doesn't, which for all but the point lies within its extent so
     * that only reading the shape itself tells them apart. The shapes are far enough apart that each box intersects at
     * most one of them.
     */
    private record Shape(String wkt, Rectangle hit, Rectangle miss) {}

    private static final List<Shape> SHAPES = List.of(
        new Shape("POLYGON ((0 0, 10 0, 0 10, 0 0))", new Rectangle(1, 2, 2, 1), new Rectangle(8, 9, 9, 8)),
        new Shape("POINT (50 50)", new Rectangle(49, 51, 51, 49), new Rectangle(60, 61, 61, 60)),
        new Shape("MULTIPOINT (100 -20, 110 -30)", new Rectangle(109, 111, -29, -31), new Rectangle(104, 106, -24, -26)),
        new Shape("LINESTRING (-50 -50, -40 -40)", new Rectangle(-46, -44, -44, -46), new Rectangle(-49, -48, -41, -42))
    );

    @Override
    protected Collection<Plugin> getPlugins() {
        var plugin = new LocalStateSpatialPlugin();
        plugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                return List.of();
            }
        });
        return List.of(plugin);
    }

    public void testGeoShape() throws IOException {
        assertReadersFollowIndexVersion("geo_shape");
    }

    public void testShape() throws IOException {
        assertReadersFollowIndexVersion("shape");
    }

    private void assertReadersFollowIndexVersion(String type) throws IOException {
        List<String> legacy = readDocValues(
            type,
            IndexVersionUtils.randomPreviousCompatibleWriteVersion(IndexVersions.GEOMETRY_DOC_VALUES_V2),
            GeometryDocValueFormat.LEGACY
        );
        List<String> v2 = readDocValues(
            type,
            IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.GEOMETRY_DOC_VALUES_V2),
            GeometryDocValueFormat.V2
        );
        assertThat(v2, equalTo(legacy));
    }

    /**
     * Indexes {@link #SHAPES} into a doc-values-only field, checks that queries match the right shapes, and returns what
     * the fielddata and the bounds and centroid block loaders read for each shape, in document order.
     */
    private List<String> readDocValues(String type, IndexVersion version, GeometryDocValueFormat expectedFormat) throws IOException {
        MapperService mapperService = createMapperService(version, fieldMapping(b -> b.field("type", type).field("index", false)));
        var fieldType = (AbstractShapeGeometryFieldType<?>) mapperService.fieldType("field");
        assertThat("index version " + version, fieldType.docValueFormat(), equalTo(expectedFormat));
        List<String> values = new ArrayList<>();
        withLuceneIndex(mapperService, iw -> {
            for (Shape shape : SHAPES) {
                iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field("field", shape.wkt()))).rootDoc());
            }
            iw.forceMerge(1);
        }, reader -> {
            IndexSearcher searcher = newSearcher(reader);
            SearchExecutionContext context = createSearchExecutionContext(mapperService, searcher);
            for (Shape shape : SHAPES) {
                assertThat(shape.wkt(), searcher.count(intersects(fieldType, context, shape.hit())), equalTo(1));
                assertThat(shape.wkt(), searcher.count(intersects(fieldType, context, shape.miss())), equalTo(0));
            }

            LeafReaderContext ctx = getOnlyLeafReader(reader).getContext();
            int numDocs = ctx.reader().maxDoc();
            assertThat(numDocs, equalTo(SHAPES.size()));
            var fieldData = (LeafShapeFieldData<?>) fieldType.fielddataBuilder(FieldDataContext.noRuntimeFields("index", "test"))
                .build(null, null)
                .load(ctx);
            ShapeValues<?> shapeValues = fieldData.getShapeValues();
            TestBlock bounds = readBlock(mapperService, fieldType, MappedFieldType.FieldExtractPreference.EXTRACT_SPATIAL_BOUNDS, ctx);
            TestBlock centroids = readBlock(mapperService, fieldType, MappedFieldType.FieldExtractPreference.EXTRACT_SPATIAL_CENTROID, ctx);
            for (int doc = 0; doc < numDocs; doc++) {
                assertTrue(shapeValues.advanceExact(doc));
                ShapeValues.ShapeValue value = shapeValues.value();
                ShapeValues.BoundingBox box = value.boundingBox();
                values.add(
                    value.dimensionalShapeType()
                        + " weight="
                        + value.weight()
                        + " centroid="
                        + value.getX()
                        + ","
                        + value.getY()
                        + " box="
                        + List.of(box.minX(), box.maxX(), box.minY(), box.maxY())
                        + " bounds block="
                        + bounds.get(doc)
                        + " centroid block="
                        + centroids.get(doc)
                );
            }

            if (type.equals("geo_shape")) {
                assertGridLoaderGetsFormat(mapperService, fieldType, ctx, expectedFormat);
            }
        });
        return values;
    }

    private static Query intersects(AbstractShapeGeometryFieldType<?> fieldType, SearchExecutionContext context, Rectangle box) {
        if (fieldType instanceof GeoShapeQueryable geo) {
            return geo.geoShapeQuery(context, "field", ShapeRelation.INTERSECTS, box);
        }
        return ((ShapeFieldMapper.ShapeFieldType) fieldType).shapeQuery(box, "field", ShapeRelation.INTERSECTS, context);
    }

    private TestBlock readBlock(
        MapperService mapperService,
        MappedFieldType fieldType,
        MappedFieldType.FieldExtractPreference preference,
        LeafReaderContext ctx
    ) throws IOException {
        BlockLoader loader = fieldType.blockLoader(new DummyBlockLoaderContext.MapperServiceBlockLoaderContext(mapperService) {
            @Override
            public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
                return preference;
            }
        });
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        try (BlockLoader.ColumnAtATimeReader reader = loader.columnAtATimeReader(ctx).apply(breaker)) {
            int numDocs = ctx.reader().maxDoc();
            int[] docs = new int[numDocs];
            for (int i = 0; i < numDocs; i++) {
                docs[i] = i;
            }
            return (TestBlock) reader.read(TestBlock.factory(), TestBlock.docs(docs), 0, false);
        }
    }

    /** The grid tiler is supplied by ES|QL, so here a stand-in only records the format the loader gives it. */
    private void assertGridLoaderGetsFormat(
        MapperService mapperService,
        MappedFieldType fieldType,
        LeafReaderContext ctx,
        GeometryDocValueFormat expectedFormat
    ) throws IOException {
        List<GeometryDocValueFormat> formats = new ArrayList<>();
        var config = new BlockLoaderFunctionConfig.GeoGrid(
            BlockLoaderFunctionConfig.Function.ST_GEOHASH,
            3,
            null,
            () -> (lon, lat) -> 0,
            (warnings, format) -> {
                formats.add(format);
                return encoded -> new long[0];
            }
        );
        BlockLoader loader = fieldType.blockLoader(new DummyBlockLoaderContext.MapperServiceBlockLoaderContext(mapperService) {
            @Override
            public BlockLoaderFunctionConfig blockLoaderFunctionConfig() {
                return config;
            }
        });
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        try (
            BlockLoader.ColumnAtATimeReader reader = loader.columnAtATimeReader(ctx).apply(breaker);
            TestBlock block = (TestBlock) reader.read(TestBlock.factory(), TestBlock.docs(0), 0, false)
        ) {
            assertThat(block.size(), equalTo(1));
        }
        assertThat(formats, hasSize(1));
        assertThat(formats, everyItem(equalTo(expectedFormat)));
    }
}
