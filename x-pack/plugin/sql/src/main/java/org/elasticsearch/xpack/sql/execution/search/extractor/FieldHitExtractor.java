/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.ql.execution.search.extractor.AbstractFieldHitExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.common.io.SqlStreamInput;
import org.elasticsearch.xpack.sql.expression.literal.geo.GeoShape;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.VERSION;
import static org.elasticsearch.xpack.sql.type.SqlDataTypeConverter.convert;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.GEO_POINT;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.GEO_SHAPE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.SHAPE;

/**
 * Extractor for ES fields. Works for both 'normal' fields but also nested ones (which require hitName to be set).
 * The latter is used as metadata in assembling the results in the tabular response.
 */
public class FieldHitExtractor extends AbstractFieldHitExtractor {

    /**
     * Stands for {@code field}. We try to use short names for {@link HitExtractor}s
     * to save a few bytes when when we send them back to the user.
     */
    static final String NAME = "f";

    public FieldHitExtractor(String name, DataType dataType, ZoneId zoneId, MultiValueSupport multiValueSupport) {
        super(name, dataType, zoneId, multiValueSupport);
    }

    public FieldHitExtractor(String name, DataType dataType, ZoneId zoneId) {
        super(name, dataType, zoneId);
    }

    public FieldHitExtractor(String name, DataType dataType, ZoneId zoneId, String hitName, MultiValueSupport multiValueSupport) {
        super(name, dataType, zoneId, hitName, multiValueSupport);
    }

    public FieldHitExtractor(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected DataType loadTypeFromName(String typeName) {
        return SqlDataTypes.fromTypeName(typeName);
    }

    @Override
    protected ZoneId readZoneId(StreamInput in) throws IOException {
        return SqlStreamInput.asSqlStream(in).zoneId();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    // let's make sure first that we are not dealing with an geo_point represented as an array
    @Override
    protected boolean isPrimitive(List<?> list) {
        return isGeoPointArray(list);
    }

    private boolean isGeoPointArray(List<?> list) {
        if (dataType() != GEO_POINT) {
            return false;
        }
        // we expect the point in [lon lat] or [lon lat alt] formats
        if (list.size() > 3 || list.size() < 1) {
            return false;
        }
        return list.get(0) instanceof Number;
    }

    @Override
    protected Object unwrapCustomValue(Object values) {
        DataType dataType = dataType();

        if (dataType == GEO_POINT) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) values;
                GeoPoint geoPoint = GeoUtils.parseGeoPoint(map.get("coordinates"), true);
                return new GeoShape(geoPoint.lon(), geoPoint.lat());
            } catch (ElasticsearchParseException ex) {
                throw new SqlIllegalArgumentException("Cannot parse geo_point value [{}] (returned by [{}])", values, fieldName());
            }
        }
        if (dataType == GEO_SHAPE) {
            try {
                return new GeoShape(values);
            } catch (IOException | XContentParseException ex) {
                throw new SqlIllegalArgumentException("Cannot read geo_shape value [{}] (returned by [{}])", values, fieldName());
            }
        }
        if (dataType == SHAPE) {
            try {
                return new GeoShape(values);
            } catch (IOException ex) {
                throw new SqlIllegalArgumentException("Cannot read shape value [{}] (returned by [{}])", values, fieldName());
            }
        }
        if (values instanceof Map) {
            throw new SqlIllegalArgumentException("Objects (returned by [{}]) are not supported", fieldName());
        }
        if (dataType == DATETIME) {
            if (values instanceof String) {
                return DateUtils.asDateTimeWithNanos(values.toString()).withZoneSameInstant(zoneId());
            }
        }
        if (dataType == UNSIGNED_LONG) {
            // Unsigned longs can be returned either as such (for values exceeding long range) or as longs. Value conversion is needed
            // since its later processing will be type dependent. (ex.: negation of UL is only "safe" for 0 values)
            return convert(values, UNSIGNED_LONG);
        }
        if (dataType == VERSION) {
            return convert(values, VERSION);
        }

        return null;
    }
}
