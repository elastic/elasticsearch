/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.fields;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a field to be extracted by the datafeed.
 * It encapsulates the extraction logic.
 */
public abstract class ExtractedField {

    public enum ExtractionMethod {
        SOURCE, DOC_VALUE, SCRIPT_FIELD
    }

    /** The name of the field as configured in the job */
    protected final String alias;

    /** The name of the field we extract */
    protected final String name;

    private final Set<String> types;

    private final ExtractionMethod extractionMethod;

    protected ExtractedField(String alias, String name, Set<String> types, ExtractionMethod extractionMethod) {
        this.alias = Objects.requireNonNull(alias);
        this.name = Objects.requireNonNull(name);
        this.types = Objects.requireNonNull(types);
        this.extractionMethod = Objects.requireNonNull(extractionMethod);
    }

    public String getAlias() {
        return alias;
    }

    public String getName() {
        return name;
    }

    public Set<String> getTypes() {
        return types;
    }

    public ExtractionMethod getExtractionMethod() {
        return extractionMethod;
    }

    public abstract Object[] value(SearchHit hit);

    public abstract boolean supportsFromSource();

    public String getDocValueFormat() {
        return null;
    }

    public static ExtractedField newTimeField(String name, Set<String> types, ExtractionMethod extractionMethod) {
        if (extractionMethod == ExtractionMethod.SOURCE) {
            throw new IllegalArgumentException("time field cannot be extracted from source");
        }
        return new TimeField(name, types, extractionMethod);
    }

    public static ExtractedField newGeoShapeField(String alias, String name) {
        return new GeoShapeField(alias, name, Collections.singleton("geo_shape"));
    }

    public static ExtractedField newGeoPointField(String alias, String name) {
        return new GeoPointField(alias, name, Collections.singleton("geo_point"));
    }

    public static ExtractedField newField(String name, Set<String> types, ExtractionMethod extractionMethod) {
        return newField(name, name, types, extractionMethod);
    }

    public static ExtractedField newField(String alias, String name, Set<String> types, ExtractionMethod extractionMethod) {
        switch (extractionMethod) {
            case DOC_VALUE:
            case SCRIPT_FIELD:
                return new FromFields(alias, name, types, extractionMethod);
            case SOURCE:
                return new FromSource(alias, name, types);
            default:
                throw new IllegalArgumentException("Invalid extraction method [" + extractionMethod + "]");
        }
    }

    public ExtractedField newFromSource() {
        if (supportsFromSource()) {
            return new FromSource(alias, name, types);
        }
        throw new IllegalStateException("Field (alias [" + alias + "], name [" + name + "]) should be extracted via ["
            + extractionMethod + "] and cannot be extracted from source");
    }

    private static class FromFields extends ExtractedField {

        FromFields(String alias, String name, Set<String> types, ExtractionMethod extractionMethod) {
            super(alias, name, types, extractionMethod);
        }

        @Override
        public Object[] value(SearchHit hit) {
            DocumentField keyValue = hit.field(name);
            if (keyValue != null) {
                List<Object> values = keyValue.getValues();
                return values.toArray(new Object[0]);
            }
            return new Object[0];
        }

        @Override
        public boolean supportsFromSource() {
            return getExtractionMethod() == ExtractionMethod.DOC_VALUE;
        }
    }

    private static class GeoShapeField extends FromSource {
        private static final WellKnownText wkt = new WellKnownText(true, new StandardValidator(true));

        GeoShapeField(String alias, String name, Set<String> types) {
            super(alias, name, types);
        }

        @Override
        public Object[] value(SearchHit hit) {
            Object[] value = super.value(hit);
            if (value.length != 1) {
                throw new IllegalStateException("Unexpected values for a geo_shape field: " + Arrays.toString(value));
            }
            if (value[0] instanceof String) {
                value[0] = handleString((String) value[0]);
            } else if (value[0] instanceof Map<?, ?>) {
                @SuppressWarnings("unchecked")
                Map<String, Object> geoObject = (Map<String, Object>) value[0];
                value[0] = handleObject(geoObject);
            } else {
                throw new IllegalStateException("Unexpected value type for a geo_shape field: " + value[0].getClass());
            }
            return value;
        }

        private String handleString(String geoString) {
            try {
                if (geoString.startsWith("POINT")) { // Entry is of the form "POINT (-77.03653 38.897676)"
                    Geometry geometry = wkt.fromWKT(geoString);
                    if (geometry.type() != ShapeType.POINT) {
                        throw new IllegalArgumentException("Unexpected non-point geo_shape type: " + geometry.type().name());
                    }
                    Point pt = ((Point)geometry);
                    return pt.getY() + "," + pt.getX();
                } else {
                    throw new IllegalArgumentException("Unexpected value for a geo_shape field: " + geoString);
                }
            } catch (IOException | ParseException ex) {
                throw new IllegalArgumentException("Unexpected value for a geo_shape field: " + geoString);
            }
        }

        private String handleObject(Map<String, Object> geoObject) {
            String geoType = (String) geoObject.get("type");
            if (geoType != null && "point".equals(geoType.toLowerCase(Locale.ROOT))) {
                @SuppressWarnings("unchecked")
                List<Double> coordinates = (List<Double>) geoObject.get("coordinates");
                if (coordinates == null || coordinates.size() != 2) {
                    throw new IllegalArgumentException("Invalid coordinates for geo_shape point: " + geoObject);
                }
                return coordinates.get(1) + "," + coordinates.get(0);
            } else {
                throw new IllegalArgumentException("Unexpected value for a geo_shape field: " + geoObject);
            }
        }

    }

    private static class GeoPointField extends FromFields {

        GeoPointField(String alias, String name, Set<String> types) {
            super(alias, name, types, ExtractionMethod.DOC_VALUE);
        }

        @Override
        public Object[] value(SearchHit hit) {
            Object[] value = super.value(hit);
            if (value.length != 1) {
                throw new IllegalStateException("Unexpected values for a geo_point field: " + Arrays.toString(value));
            }
            if (value[0] instanceof String) {
                value[0] = handleString((String) value[0]);
            } else {
                throw new IllegalStateException("Unexpected value type for a geo_point field: " + value[0].getClass());
            }
            return value;
        }

        private String handleString(String geoString) {
            if (geoString.contains(",")) { // Entry is of the form "38.897676, -77.03653"
                return geoString.replace(" ", "");
            } else {
                throw new IllegalArgumentException("Unexpected value for a geo_point field: " + geoString);
            }
        }

        @Override
        public boolean supportsFromSource() {
            return false;
        }
    }

    private static class TimeField extends FromFields {

        private static final String EPOCH_MILLIS_FORMAT = "epoch_millis";

        TimeField(String name, Set<String> types, ExtractionMethod extractionMethod) {
            super(name, name, types, extractionMethod);
        }

        @Override
        public Object[] value(SearchHit hit) {
            Object[] value = super.value(hit);
            if (value.length != 1) {
                return value;
            }
            if (value[0] instanceof String) { // doc_value field with the epoch_millis format
                value[0] = Long.parseLong((String) value[0]);
            } else if (value[0] instanceof Long == false) { // pre-6.0 field
                throw new IllegalStateException("Unexpected value for a time field: " + value[0].getClass());
            }
            return value;
        }

        @Override
        public String getDocValueFormat() {
            return EPOCH_MILLIS_FORMAT;
        }

        @Override
        public boolean supportsFromSource() {
            return false;
        }
    }

    private static class FromSource extends ExtractedField {

        private String[] namePath;

        FromSource(String alias, String name, Set<String> types) {
            super(alias, name, types, ExtractionMethod.SOURCE);
            namePath = name.split("\\.");
        }

        @Override
        public Object[] value(SearchHit hit) {
            Map<String, Object> source = hit.getSourceAsMap();
            int level = 0;
            while (source != null && level < namePath.length - 1) {
                source = getNextLevel(source, namePath[level]);
                level++;
            }
            if (source != null) {
                Object values = source.get(namePath[level]);
                if (values != null) {
                    if (values instanceof List<?>) {
                        @SuppressWarnings("unchecked")
                        List<Object> asList = (List<Object>) values;
                        return asList.toArray(new Object[0]);
                    } else {
                        return new Object[]{values};
                    }
                }
            }
            return new Object[0];
        }

        @Override
        public boolean supportsFromSource() {
            return true;
        }

        @SuppressWarnings("unchecked")
        private static Map<String, Object> getNextLevel(Map<String, Object> source, String key) {
            Object nextLevel = source.get(key);
            if (nextLevel instanceof Map<?, ?>) {
                return (Map<String, Object>) source.get(key);
            }
            return null;
        }
    }
}
