/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.lucene.spatial.BinaryShapeDocValuesField;
import org.elasticsearch.lucene.spatial.CartesianShapeIndexer;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.MapXContentParser;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.datageneration.ShapeDataSourceHandler;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ShapeFieldBlockLoaderTests extends BlockLoaderTestCase {
    public ShapeFieldBlockLoaderTests(Params params) {
        super("shape", List.of(new ShapeDataSourceHandler(params.indexMode())), params);
    }

    @Override
    protected boolean supportsMultiField() {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        // Columnar indices keep the original WKB in their own doc value, so they don't reconstruct from the triangle tree
        boolean docValuesMode = params.preference() == MappedFieldType.FieldExtractPreference.DOC_VALUES
            && params.indexMode().isStrictColumnar() == false
            && (Boolean) fieldMapping.getOrDefault("doc_values", true);

        if (docValuesMode) {
            return expectedFromDocValues(value);
        }

        if (value instanceof List<?> == false) {
            return convert(value);
        }

        // TODO FieldExtractPreference.EXTRACT_SPATIAL_BOUNDS is currently not covered, it needs special logic
        // As a result we always load from source (stored or fallback synthetic) and they should work the same.
        var resultList = ((List<Object>) value).stream().map(this::convert).filter(Objects::nonNull).toList();
        return maybeFoldList(resultList);
    }

    /**
     * Compute expected value for DOC_VALUES mode by round-tripping through the doc-values format.
     * Doc-values combine multiple field values into a single binary value, reconstructing the
     * original geometry with quantized coordinates.
     */
    @SuppressWarnings("unchecked")
    private Object expectedFromDocValues(Object value) {
        List<Object> values = value instanceof List<?> ? (List<Object>) value : Collections.singletonList(value);
        List<Geometry> geometries = values.stream().map(this::parseGeometry).filter(Objects::nonNull).toList();
        if (geometries.isEmpty()) {
            return null;
        }
        try {
            CartesianShapeIndexer indexer = new CartesianShapeIndexer("test");
            BinaryShapeDocValuesField field = new BinaryShapeDocValuesField("test", CoordinateEncoder.CARTESIAN);
            for (Geometry geometry : geometries) {
                field.add(indexer.indexShape(geometry), geometry);
            }
            GeometryDocValueReader reader = new GeometryDocValueReader();
            reader.reset(field.binaryValue());
            Geometry reconstructed = reader.getGeometry(CoordinateEncoder.CARTESIAN);
            if (reconstructed == null) {
                return null;
            }
            return toWKB(reconstructed);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Geometry parseGeometry(Object value) {
        try {
            if (value instanceof String s) {
                return fromWKT(s);
            }
            if (value instanceof Map<?, ?> m) {
                return fromGeoJson(m);
            }
        } catch (Exception e) {
            // Malformed value
        }
        return null;
    }

    private Object convert(Object value) {
        if (value instanceof String s) {
            return toWKB(fromWKT(s));
        }

        if (value instanceof Map<?, ?> m) {
            return toWKB(fromGeoJson(m));
        }

        // Malformed values are excluded
        return null;
    }

    private Geometry fromWKT(String s) {
        try {
            return WellKnownText.fromWKT(StandardValidator.instance(true), false, s);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private Geometry fromGeoJson(Map<?, ?> map) {
        try {
            var parser = new MapXContentParser(
                xContentRegistry(),
                LoggingDeprecationHandler.INSTANCE,
                (Map<String, Object>) map,
                XContentType.JSON
            );
            parser.nextToken();

            return GeoJson.fromXContent(StandardValidator.instance(true), false, true, parser);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private BytesRef toWKB(Geometry geometry) {
        return new BytesRef(WellKnownBinary.toWKB(geometry, ByteOrder.LITTLE_ENDIAN));
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        var plugin = new LocalStateSpatialPlugin();
        plugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                return List.of();
            }
        });

        return Collections.singletonList(plugin);
    }
}
