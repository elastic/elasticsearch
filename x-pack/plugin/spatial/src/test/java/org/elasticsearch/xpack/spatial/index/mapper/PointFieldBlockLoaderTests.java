/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.XYDocValuesField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.datageneration.PointDataSourceHandler;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class PointFieldBlockLoaderTests extends BlockLoaderTestCase {
    public PointFieldBlockLoaderTests(Params params) {
        super("point", List.of(new PointDataSourceHandler()), params);
    }

    @Override
    protected boolean supportsMultiField() {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        var nullValue = switch (fieldMapping.get("null_value")) {
            case Map<?, ?> m -> convert(m, null);
            case null -> null;
            default -> throw new IllegalStateException("Unexpected null_value format");
        };

        // Columnar reads from XY doc values only: values come back sorted by encoding and at float precision - as the
        // encoded longs for DOC_VALUES, or their WKB for any other preference.
        if (params.indexMode().isStrictColumnar()) {
            boolean asWkb = params.preference() != MappedFieldType.FieldExtractPreference.DOC_VALUES;
            var resultList = (value instanceof List<?> list ? list.stream() : Stream.of(value)).map(v -> convert(v, nullValue))
                .filter(Objects::nonNull)
                .map(this::encode)
                .sorted()
                .map(encoded -> asWkb ? (Object) toWKB(decode(encoded)) : (Object) encoded)
                .toList();
            return maybeFoldList(resultList);
        }

        if (params.preference() == MappedFieldType.FieldExtractPreference.DOC_VALUES && hasDocValues(fieldMapping, true)) {
            if (value instanceof List<?> == false) {
                return encode(convert(value, nullValue));
            }

            var resultList = ((List<Object>) value).stream()
                .map(v -> convert(v, nullValue))
                .filter(Objects::nonNull)
                .map(this::encode)
                .sorted()
                .toList();
            return maybeFoldList(resultList);
        }

        if (value instanceof List<?> == false) {
            return toWKB(convert(value, nullValue));
        }

        // Non-columnar loads from source (stored or fallback synthetic), which is exact.
        var resultList = ((List<Object>) value).stream().map(v -> convert(v, nullValue)).filter(Objects::nonNull).map(this::toWKB).toList();
        return maybeFoldList(resultList);
    }

    @Override
    protected Object getFieldValue(Map<String, Object> document, String fieldName) {
        var extracted = new ArrayList<>();
        processLevel(document, fieldName, extracted);

        if (extracted.size() == 1) {
            return extracted.get(0);
        }

        return extracted;
    }

    @SuppressWarnings("unchecked")
    private void processLevel(Map<String, Object> level, String field, ArrayList<Object> extracted) {
        if (field.contains(".") == false) {
            var value = level.get(field);
            processLeafLevel(value, extracted);
            return;
        }

        var nameInLevel = field.split("\\.")[0];
        var entry = level.get(nameInLevel);
        if (entry instanceof Map<?, ?> m) {
            processLevel((Map<String, Object>) m, field.substring(field.indexOf('.') + 1), extracted);
        }
        if (entry instanceof List<?> l) {
            for (var object : l) {
                processLevel((Map<String, Object>) object, field.substring(field.indexOf('.') + 1), extracted);
            }
        }
    }

    private void processLeafLevel(Object value, ArrayList<Object> extracted) {
        if (value instanceof List<?> l) {
            if (l.size() > 0 && l.get(0) instanceof Double) {
                // this must be a single point in array form
                // we'll put it into a different form here to make our lives a bit easier while implementing `expected`
                extracted.add(Map.of("type", "point", "coordinates", l));
            } else {
                // this is actually an array of points but there could still be points in array form inside
                for (var arrayValue : l) {
                    processLeafLevel(arrayValue, extracted);
                }
            }
        } else {
            extracted.add(value);
        }
    }

    @SuppressWarnings("unchecked")
    private CartesianPoint convert(Object value, CartesianPoint nullValue) {
        if (value == null) {
            return nullValue;
        }

        var point = new CartesianPoint();

        if (value instanceof String s) {
            try {
                point.resetFromString(s, true);
                return point;
            } catch (Exception e) {
                return null;
            }
        }

        if (value instanceof Map<?, ?> m) {
            if (m.get("type") != null) {
                var coordinates = (List<Double>) m.get("coordinates");
                point.reset(coordinates.get(0), coordinates.get(1));
            } else {
                point.reset((Double) m.get("x"), (Double) m.get("y"));
            }

            return point;
        }
        if (value instanceof List<?> l) {
            point.reset((Double) l.get(0), (Double) l.get(1));
            return point;
        }

        // Malformed values are excluded
        return null;
    }

    private Long encode(CartesianPoint point) {
        if (point == null) {
            return null;
        }
        return new XYDocValuesField("f", (float) point.getX(), (float) point.getY()).numericValue().longValue();
    }

    private BytesRef toWKB(CartesianPoint cartesianPoint) {
        if (cartesianPoint == null) {
            return null;
        }
        return new BytesRef(WellKnownBinary.toWKB(new Point(cartesianPoint.getX(), cartesianPoint.getY()), ByteOrder.LITTLE_ENDIAN));
    }

    // Reused across decode() calls: each result is consumed immediately by the caller, so it is safe to share.
    private final CartesianPoint scratchPoint = new CartesianPoint();

    // Decode an encoded XY doc-value long back into a point, exactly as the columnar block loader does.
    private CartesianPoint decode(long encoded) {
        scratchPoint.resetFromEncoded(encoded);
        return scratchPoint;
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
