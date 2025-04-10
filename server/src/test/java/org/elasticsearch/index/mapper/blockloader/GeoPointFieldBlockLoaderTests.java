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
import java.util.ArrayList;
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
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        var extractedFieldValues = (ExtractedFieldValues) value;
        var values = extractedFieldValues.values();

        var nullValue = switch (fieldMapping.get("null_value")) {
            case String s -> convert(s, null);
            case null -> null;
            default -> throw new IllegalStateException("Unexpected null_value format");
        };

        if (params.preference() == MappedFieldType.FieldExtractPreference.DOC_VALUES && hasDocValues(fieldMapping, true)) {
            if (values instanceof List<?> == false) {
                var point = convert(values, nullValue);
                return point != null ? point.getEncoded() : null;
            }

            var resultList = ((List<Object>) values).stream()
                .map(v -> convert(v, nullValue))
                .filter(Objects::nonNull)
                .map(GeoPoint::getEncoded)
                .sorted()
                .toList();
            return maybeFoldList(resultList);
        }

        if (params.syntheticSource() == false) {
            return exactValuesFromSource(values, nullValue);
        }

        // Usually implementation of block loader from source adjusts values read from source
        // so that they look the same as doc_values would (like reducing precision).
        // geo_point does not do that and because of that we need to handle all these cases below.
        // If we are reading from stored source or fallback synthetic source we get the same exact data as source.
        // But if we are using "normal" synthetic source we get lesser precision data from doc_values.
        // That is unless "synthetic_source_keep" forces fallback synthetic source again.

        if (testContext.forceFallbackSyntheticSource()) {
            return exactValuesFromSource(values, nullValue);
        }

        String syntheticSourceKeep = (String) fieldMapping.getOrDefault("synthetic_source_keep", "none");
        if (syntheticSourceKeep.equals("all")) {
            return exactValuesFromSource(values, nullValue);
        }
        if (syntheticSourceKeep.equals("arrays") && extractedFieldValues.documentHasObjectArrays()) {
            return exactValuesFromSource(values, nullValue);
        }

        // synthetic source and doc_values are present
        if (hasDocValues(fieldMapping, true)) {
            if (values instanceof List<?> == false) {
                return toWKB(normalize(convert(values, nullValue)));
            }

            var resultList = ((List<Object>) values).stream()
                .map(v -> convert(v, nullValue))
                .filter(Objects::nonNull)
                .sorted(Comparator.comparingLong(GeoPoint::getEncoded))
                .map(p -> toWKB(normalize(p)))
                .toList();
            return maybeFoldList(resultList);
        }

        // synthetic source but no doc_values so using fallback synthetic source
        return exactValuesFromSource(values, nullValue);
    }

    @SuppressWarnings("unchecked")
    private Object exactValuesFromSource(Object value, GeoPoint nullValue) {
        if (value instanceof List<?> == false) {
            return toWKB(convert(value, nullValue));
        }

        var resultList = ((List<Object>) value).stream().map(v -> convert(v, nullValue)).filter(Objects::nonNull).map(this::toWKB).toList();
        return maybeFoldList(resultList);
    }

    private record ExtractedFieldValues(Object values, boolean documentHasObjectArrays) {}

    @Override
    protected Object getFieldValue(Map<String, Object> document, String fieldName) {
        var extracted = new ArrayList<>();
        var documentHasObjectArrays = processLevel(document, fieldName, extracted, false);

        if (extracted.size() == 1) {
            return new ExtractedFieldValues(extracted.get(0), documentHasObjectArrays);
        }

        return new ExtractedFieldValues(extracted, documentHasObjectArrays);
    }

    @SuppressWarnings("unchecked")
    private boolean processLevel(Map<String, Object> level, String field, ArrayList<Object> extracted, boolean documentHasObjectArrays) {
        if (field.contains(".") == false) {
            var value = level.get(field);
            processLeafLevel(value, extracted);
            return documentHasObjectArrays;
        }

        var nameInLevel = field.split("\\.")[0];
        var entry = level.get(nameInLevel);
        if (entry instanceof Map<?, ?> m) {
            return processLevel((Map<String, Object>) m, field.substring(field.indexOf('.') + 1), extracted, documentHasObjectArrays);
        }
        if (entry instanceof List<?> l) {
            for (var object : l) {
                processLevel((Map<String, Object>) object, field.substring(field.indexOf('.') + 1), extracted, true);
            }
            return true;
        }

        assert false : "unexpected document structure";
        return false;
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
    private GeoPoint convert(Object value, GeoPoint nullValue) {
        if (value == null) {
            return nullValue;
        }

        if (value instanceof String s) {
            try {
                return new GeoPoint(s);
            } catch (Exception e) {
                return null;
            }
        }

        if (value instanceof Map<?, ?> m) {
            if (m.get("type") != null) {
                var coordinates = (List<Double>) m.get("coordinates");
                // Order is GeoJSON is lon,lat
                return new GeoPoint(coordinates.get(1), coordinates.get(0));
            } else {
                return new GeoPoint((Double) m.get("lat"), (Double) m.get("lon"));
            }
        }

        // Malformed values are excluded
        return null;
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
