/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.MapXContentParser;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.datageneration.GeoShapeDataSourceHandler;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GeoShapeFieldBlockLoaderTests extends BlockLoaderTestCase {
    public GeoShapeFieldBlockLoaderTests(Params params) {
        super("geo_shape", List.of(new GeoShapeDataSourceHandler()), params);
    }

    @Override
    public void testBlockLoaderOfMultiField() throws IOException {
        // Multi fields are noop for geo_shape.
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        if (value instanceof List<?> == false) {
            return convert(value);
        }

        // TODO FieldExtractPreference.EXTRACT_SPATIAL_BOUNDS is currently not covered, it needs special logic
        // As a result we always load from source (stored or fallback synthetic) and they should work the same.
        var resultList = ((List<Object>) value).stream().map(this::convert).filter(Objects::nonNull).toList();
        return maybeFoldList(resultList);
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
            var geometry = WellKnownText.fromWKT(GeographyValidator.instance(true), false, s);
            return normalize(geometry);
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

            var geometry = GeoJson.fromXContent(GeographyValidator.instance(true), false, true, parser);
            return normalize(geometry);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Geometry normalize(Geometry geometry) {
        if (GeometryNormalizer.needsNormalize(Orientation.RIGHT, geometry)) {
            return GeometryNormalizer.apply(Orientation.RIGHT, geometry);
        }

        return geometry;
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
