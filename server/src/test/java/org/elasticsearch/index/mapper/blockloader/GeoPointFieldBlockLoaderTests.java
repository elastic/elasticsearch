/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GeoPointFieldBlockLoaderTests extends BlockLoaderTestCase {
    public GeoPointFieldBlockLoaderTests(BlockLoaderTestCase.Params params) {
        super("geo_point", params);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object expected(Map<String, Object> fieldMapping, Object values, TestContext testContext) {
        var nullValue = switch (fieldMapping.get("null_value")) {
            case String s -> convert(s, null, false);
            case null -> null;
            default -> throw new IllegalStateException("Unexpected null_value format");
        };

        if (params.preference() == MappedFieldType.FieldExtractPreference.DOC_VALUES && hasDocValues(fieldMapping, true)) {
            if (values instanceof List<?> == false) {
                var point = convert(values, nullValue, testContext.isMultifield());
                return point != null ? point.getEncoded() : null;
            }

            var resultList = ((List<Object>) values).stream()
                .map(v -> convert(v, nullValue, testContext.isMultifield()))
                .filter(Objects::nonNull)
                .map(GeoPoint::getEncoded)
                .sorted()
                .toList();
            return maybeFoldList(resultList);
        }

        // stored source is used
        if (params.syntheticSource() == false) {
            return exactValuesFromSource(values, nullValue, false);
        }

        // Usually implementation of block loader from source adjusts values read from source
        // so that they look the same as doc_values would (like reducing precision).
        // geo_point does not do that and because of that we need to handle all these cases below.
        // If we are reading from stored source or fallback synthetic source we get the same exact data as source.
        // But if we are using "normal" synthetic source we get lesser precision data from doc_values.
        // That is unless "synthetic_source_keep" forces fallback synthetic source again.

        if (testContext.forceFallbackSyntheticSource()) {
            return exactValuesFromSource(values, nullValue, false);
        }

        String syntheticSourceKeep = (String) fieldMapping.getOrDefault("synthetic_source_keep", "none");
        if (syntheticSourceKeep.equals("all")) {
            return exactValuesFromSource(values, nullValue, false);
        }

        // synthetic source and doc_values are present
        if (hasDocValues(fieldMapping, true)) {
            if (values instanceof List<?> == false) {
                return toWKB(normalize(convert(values, nullValue, false)));
            }

            var resultList = ((List<Object>) values).stream()
                .map(v -> convert(v, nullValue, false))
                .filter(Objects::nonNull)
                .sorted(Comparator.comparingLong(GeoPoint::getEncoded))
                .map(p -> toWKB(normalize(p)))
                .toList();
            return maybeFoldList(resultList);
        }

        // synthetic source but no doc_values so using fallback synthetic source
        return exactValuesFromSource(values, nullValue, false);
    }

    @SuppressWarnings("unchecked")
    private Object exactValuesFromSource(Object value, GeoPoint nullValue, boolean needsMultifieldAdjustment) {
        if (value instanceof List<?> == false) {
            return toWKB(convert(value, nullValue, needsMultifieldAdjustment));
        }

        var resultList = ((List<Object>) value).stream()
            .map(v -> convert(v, nullValue, needsMultifieldAdjustment))
            .filter(Objects::nonNull)
            .map(this::toWKB)
            .toList();
        return maybeFoldList(resultList);
    }

    @SuppressWarnings("unchecked")
    private GeoPoint convert(Object value, GeoPoint nullValue, boolean needsMultifieldAdjustment) {
        if (value == null) {
            if (nullValue == null) {
                return null;
            }
            return possiblyAdjustMultifieldValue(nullValue, needsMultifieldAdjustment);
        }

        if (value instanceof String s) {
            try {
                return possiblyAdjustMultifieldValue(new GeoPoint(s), needsMultifieldAdjustment);
            } catch (Exception e) {
                return null;
            }
        }

        if (value instanceof Map<?, ?> m) {
            if (m.get("type") != null) {
                var coordinates = (List<Double>) m.get("coordinates");
                // Order is GeoJSON is lon,lat
                return possiblyAdjustMultifieldValue(new GeoPoint(coordinates.get(1), coordinates.get(0)), needsMultifieldAdjustment);
            } else {
                return possiblyAdjustMultifieldValue(new GeoPoint((Double) m.get("lat"), (Double) m.get("lon")), needsMultifieldAdjustment);
            }
        }

        // Malformed values are excluded
        return null;
    }

    private GeoPoint possiblyAdjustMultifieldValue(GeoPoint point, boolean isMultifield) {
        // geo_point multifields are parsed from a geohash representation of the original point (GeoPointFieldMapper#index)
        // and it's not exact.
        // So if this is a multifield we need another adjustment here.
        // Note that this does not apply to block loader from source because in this case we parse raw original values.
        // Same thing happens with synthetic source since it is generated from the parent field data that didn't go through multi field
        // parsing logic.
        if (isMultifield) {
            return point.resetFromString(point.geohash());
        }

        return point;
    }

    private GeoPoint normalize(GeoPoint point) {
        if (point == null) {
            return null;
        }
        return point.resetFromEncoded(point.getEncoded());
    }

    private BytesRef toWKB(GeoPoint point) {
        if (point == null) {
            return null;
        }

        return new BytesRef(WellKnownBinary.toWKB(new Point(point.getX(), point.getY()), ByteOrder.LITTLE_ENDIAN));
    }
}
