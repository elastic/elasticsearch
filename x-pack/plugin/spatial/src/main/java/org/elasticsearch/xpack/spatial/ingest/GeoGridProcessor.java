/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.ingest;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeometryParserFormat;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.h3.H3;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.spatial.common.H3CartesianUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.geo.GeometryParserFormat.GEOJSON;
import static org.elasticsearch.common.geo.GeometryParserFormat.WKT;

/**
 *  The geo-grid-processor converts a geohash, geotile or geohex tile cell definition into an indexable geometry describing the tile shape.
 */
public final class GeoGridProcessor extends AbstractProcessor {
    public static final String TYPE = "geo_grid";

    private final FieldConfig config;
    private final boolean ignoreMissing;
    private final GeometryParserFormat targetFormat;
    private final TileFieldType tileFieldType;

    GeoGridProcessor(
        String tag,
        String description,
        FieldConfig config,
        boolean ignoreMissing,
        GeometryParserFormat targetFormat,
        TileFieldType tileFieldType
    ) {
        super(tag, description);
        this.config = config;
        this.ignoreMissing = ignoreMissing;
        this.targetFormat = targetFormat;
        this.tileFieldType = tileFieldType;
    }

    public static class FieldConfig {
        private final String field;
        private final String targetField;
        private final String parentField;
        private final String childrenField;
        private final String nonChildrenField;
        private final String precisionField;

        FieldConfig(
            String field,
            String targetField,
            String parentField,
            String childrenField,
            String nonChildrenField,
            String precisionField
        ) {
            this.field = field;
            this.targetField = targetField;
            this.parentField = parentField;
            this.childrenField = childrenField;
            this.nonChildrenField = nonChildrenField;
            this.precisionField = precisionField;
        }

        public String field(String key) {
            return switch (key) {
                case "field" -> field;
                case "target_field" -> targetField;
                case "parent_field" -> parentField;
                case "children_field" -> childrenField;
                case "non_children_field" -> nonChildrenField;
                case "precision_field" -> precisionField;
                default -> throw new IllegalArgumentException("Invalid geo_grid processor field type [" + key + "]");
            };
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        Object obj = ingestDocument.getFieldValue(config.field, Object.class, ignoreMissing);

        if (obj == null && ignoreMissing) {
            return ingestDocument;
        } else if (obj == null) {
            throw new IllegalArgumentException("field [" + config.field + "] is null, cannot process it.");
        }
        if (obj instanceof String == false) {
            throw new IllegalArgumentException("field [" + config.field + "] must be a String tile address");
        }

        try {
            final TileHandler handler = switch (tileFieldType) {
                case GEOTILE -> new GeotileHandler(obj.toString());
                case GEOHASH -> new GeohashHandler(obj.toString());
                case GEOHEX -> new GeohexHandler(obj.toString());
            };
            final Geometry geometry = handler.makeGeometry();
            XContentBuilder newValueBuilder = XContentFactory.jsonBuilder().startObject().field("val");
            targetFormat.toXContent(geometry, newValueBuilder, ToXContent.EMPTY_PARAMS);
            newValueBuilder.endObject();
            Map<String, Object> newObj = XContentHelper.convertToMap(BytesReference.bytes(newValueBuilder), true, XContentType.JSON).v2();
            ingestDocument.setFieldValue(config.targetField, newObj.get("val"));
            if (config.parentField != null && config.parentField.length() > 0) {
                final String parent = handler.getParent();
                if (parent != null && parent.length() > 0) {
                    ingestDocument.setFieldValue(config.parentField, parent);
                }
            }
            if (config.childrenField != null && config.childrenField.length() > 0) {
                final List<String> children = handler.makeChildren();
                if (children.size() > 0) {
                    ingestDocument.setFieldValue(config.childrenField, children);
                }
            }
            if (config.nonChildrenField != null && config.nonChildrenField.length() > 0) {
                final List<String> nonChildren = handler.makeNonChildren();
                if (nonChildren.size() > 0) {
                    ingestDocument.setFieldValue(config.nonChildrenField, nonChildren);
                }
            }
            if (config.precisionField != null && config.precisionField.length() > 0) {
                final int precision = handler.getPrecision();
                if (precision >= 0) {
                    ingestDocument.setFieldValue(config.precisionField, precision);
                }
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid tile definition", e);
        }

        return ingestDocument;
    }

    /**
     * This class handles the different tile types, rectangular or hexagonal.
     * Each of geohash, geotile and geohex have different ways of describing geometries, and other attributes.
     */
    abstract static class TileHandler {
        abstract Geometry makeGeometry();

        abstract List<String> makeChildren();

        // Most tilers have no intersecting non-children
        List<String> makeNonChildren() {
            return Collections.emptyList();
        }

        abstract int getPrecision();

        abstract String getParent();
    }

    static class GeotileHandler extends TileHandler {
        private final String address;
        private final int zoom;
        private final int x;
        private final int y;

        GeotileHandler(String address) {
            this.address = address;
            final int[] hashAsInts = GeoTileUtils.parseHash(address);
            this.zoom = hashAsInts[0];
            this.x = hashAsInts[1];
            this.y = hashAsInts[2];
        }

        @Override
        Geometry makeGeometry() {
            return GeoTileUtils.toBoundingBox(address);
        }

        @Override
        List<String> makeChildren() {
            int z = zoom + 1;
            if (z > GeoTileUtils.MAX_ZOOM) {
                return Collections.emptyList();
            }
            int xo = x * 2;
            int yo = y * 2;
            return Arrays.asList(hash(z, xo, yo), hash(z, xo + 1, yo), hash(z, xo, yo + 1), hash(z, xo + 1, yo + 1));
        }

        private static String hash(int zoom, int x, int y) {
            return zoom + "/" + x + "/" + y;
        }

        @Override
        String getParent() {
            return zoom == 0 ? "" : hash(zoom - 1, x / 2, y / 2);
        }

        @Override
        int getPrecision() {
            return zoom;
        }
    }

    static class GeohashHandler extends TileHandler {
        private final String hash;

        GeohashHandler(String hash) {
            this.hash = hash;
        }

        @Override
        Geometry makeGeometry() {
            return Geohash.toBoundingBox(hash);
        }

        @Override
        String getParent() {
            return hash.length() == 0 ? "" : hash.substring(0, hash.length() - 1);
        }

        @Override
        List<String> makeChildren() {
            return Arrays.asList(Geohash.getSubGeohashes(hash));
        }

        @Override
        int getPrecision() {
            return hash.length();
        }
    }

    static class GeohexHandler extends TileHandler {
        private final long h3;

        GeohexHandler(String spec) {
            this.h3 = H3.stringToH3(spec);
        }

        @Override
        Geometry makeGeometry() {
            return makePolygonFromH3(h3);
        }

        @Override
        String getParent() {
            int res = H3.getResolution(h3);
            if (res == 0) {
                return "";
            }
            long parent = H3.h3ToParent(h3);
            return H3.h3ToString(parent);
        }

        @Override
        List<String> makeChildren() {
            return asStringList(H3.h3ToChildren(h3));
        }

        @Override
        List<String> makeNonChildren() {
            return asStringList(H3.h3ToNoChildrenIntersecting(h3));
        }

        @Override
        int getPrecision() {
            return H3.getResolution(h3);
        }

        private static Geometry makePolygonFromH3(long h3) {
            return H3CartesianUtil.getNormalizeGeometry(h3);
        }

        private static List<String> asStringList(long[] cells) {
            ArrayList<String> addresses = new ArrayList<>(cells.length);
            for (long cell : cells) {
                addresses.add(H3.h3ToString(cell));
            }
            return addresses;
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String field(String key) {
        return config.field(key);
    }

    GeometryParserFormat targetFormat() {
        return targetFormat;
    }

    TileFieldType tileType() {
        return tileFieldType;
    }

    public static final class Factory implements Processor.Factory {

        public GeoGridProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", field);
            String parentField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "parent_field", "");
            String childrenField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "children_field", "");
            String nonChildrenField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "non_children_field", "");
            String precisionField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "precision_field", "");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            TileFieldType tileFieldType = TileFieldType.parse(
                ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "tile_type"),
                processorTag
            );
            GeometryParserFormat targetFormat = getTargetFormat(processorTag, config);
            FieldConfig fields = new FieldConfig(field, targetField, parentField, childrenField, nonChildrenField, precisionField);
            return new GeoGridProcessor(processorTag, description, fields, ignoreMissing, targetFormat, tileFieldType);
        }

        private GeometryParserFormat getTargetFormat(String processorTag, Map<String, Object> config) {
            String propertyName = "target_format";
            String targetFormat = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, propertyName, GEOJSON.name());
            return switch (targetFormat.toLowerCase(Locale.ROOT).trim()) {
                case "geojson" -> GEOJSON;
                case "wkt" -> WKT;
                default -> throw ConfigurationUtils.newConfigurationException(
                    TYPE,
                    processorTag,
                    propertyName,
                    "illegal value [" + targetFormat + "], valid values are [WKT, GEOJSON]"
                );
            };
        }
    }

    enum TileFieldType {
        GEOHASH,
        GEOTILE,
        GEOHEX;

        public static TileFieldType parse(String value, String tag) {
            EnumSet<TileFieldType> validValues = EnumSet.allOf(TileFieldType.class);
            try {
                return valueOf(value.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw ConfigurationUtils.newConfigurationException(
                    TYPE,
                    tag,
                    "tile_type",
                    "illegal value [" + value + "], valid values are " + Arrays.toString(validValues.toArray())
                );
            }
        }
    }
}
