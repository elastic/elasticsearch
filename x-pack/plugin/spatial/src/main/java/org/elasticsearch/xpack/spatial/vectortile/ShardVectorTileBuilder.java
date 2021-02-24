/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.vectortile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractLatLonPointIndexFieldData;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeQueryable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.AbstractLatLonShapeIndexFieldData;
import org.elasticsearch.xpack.spatial.vectortile.collector.PointDocValuesVectorTileCollector;
import org.elasticsearch.xpack.spatial.vectortile.collector.ShapeDocValuesVectorTileCollector;
import org.elasticsearch.xpack.spatial.vectortile.collector.VectorTileCollector;
import org.elasticsearch.xpack.spatial.vectortile.collector.WKTMapBoxCollector;
import org.locationtech.jts.geom.Envelope;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;

class ShardVectorTileBuilder {
    private static final Logger logger = LogManager.getLogger(ShardVectorTileBuilder.class);

    private final IndexShard indexShard;
    private final Engine.Searcher searcher;
    private final SearchExecutionContext searchExecutionContext;
    private final GeoShapeQueryable geoField;
    private final boolean hasDocValues;
    private final MappedFieldType sourceField;
    private final IndexFieldData<?> indexFieldData;
    private final int z;
    private final int x;
    private final int y;
    private final String field;

    ShardVectorTileBuilder(IndexService indexService,
                           ShardId shardId,
                           String field,
                           int z,
                           int x,
                           int y) {
        this.indexShard = indexService.getShard(shardId.id());
        this.searcher = indexShard.acquireSearcher("vectortile");
        this.field = field;
        Closeable toClose = searcher;
        try {
            this.searchExecutionContext = indexService.newSearchExecutionContext(
                indexShard.shardId().id(),
                0,
                searcher,
                () -> 0L,
                null,
                Collections.emptyMap()
            );
            MappedFieldType fieldType = searchExecutionContext.getFieldType(field);
            this.hasDocValues = fieldType.hasDocValues();
            this.sourceField = searchExecutionContext.getFieldType(SourceFieldMapper.NAME);
            this.indexFieldData = fieldType.hasDocValues() ? searchExecutionContext.getForField(fieldType) : null;
            this.z = z;
            this.x = x;
            this.y = y;
            verifyGeoField(fieldType);
            this.geoField = (GeoShapeQueryable) fieldType;
            toClose = null;
        } finally {
            IOUtils.closeWhileHandlingException(toClose);
        }
    }


    private void verifyGeoField(MappedFieldType fieldType) {
        if (fieldType == null) {
            throw new IllegalArgumentException("fieldType is null");
        }
        if (fieldType instanceof GeoShapeQueryable == false) {
            throw new IllegalArgumentException("Wrong type for the geo field, " +
                "expected [geo_point,geo_shape], got [" + fieldType.name() + "]");
        }
        if (fieldType.isSearchable() == false) {
            throw new IllegalArgumentException("The geo field [" + fieldType.name() + "]  is not searchable");
        }
    }

    public byte[] execute() throws IOException {
        try (searcher) {
            Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
            Envelope tileEnvelope = VectorTileUtils.getJTSTileBounds(x, y, z);
            Query tileQuery = geoField.geoShapeQuery(rectangle, field, ShapeRelation.INTERSECTS, searchExecutionContext);
            // TODO: We can use other implementations for point and doc values to speed up the construction of a tile
            // TODO: How can we track memory usage when building a tile?
            VectorTileCollector collector = getCollector(tileEnvelope, rectangle);
            searcher.search(tileQuery, collector);
            return collector.getVectorTile();
        }
    }

    private VectorTileCollector getCollector(Envelope tileEnvelope, Rectangle rectangle) {
        if (hasDocValues) {
            if (geoField instanceof GeoPointFieldMapper.GeoPointFieldType) {
                AbstractLatLonPointIndexFieldData.LatLonPointIndexFieldData points =
                    (AbstractLatLonPointIndexFieldData.LatLonPointIndexFieldData) indexFieldData;
                return new PointDocValuesVectorTileCollector(points, tileEnvelope, field);
            } else {
                AbstractLatLonShapeIndexFieldData.LatLonShapeIndexFieldData shapes =
                    (AbstractLatLonShapeIndexFieldData.LatLonShapeIndexFieldData) indexFieldData;
                org.apache.lucene.geo.Rectangle rectangle1 =
                    new org.apache.lucene.geo.Rectangle(rectangle.getMinLat(), rectangle.getMaxLat(),
                        rectangle.getMinLon(), rectangle.getMaxLon());
                return  new ShapeDocValuesVectorTileCollector(shapes, tileEnvelope, rectangle1, field, sourceField);
            }
        } else {
            return new WKTMapBoxCollector(sourceField, tileEnvelope, field);
        }
    }
}
