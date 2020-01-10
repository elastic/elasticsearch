/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.ql.execution.search.extractor.AbstractFieldHitExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.expression.function.scalar.geo.GeoShape;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.common.io.SqlStreamInput;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

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

    public FieldHitExtractor(String name, DataType dataType, ZoneId zoneId, boolean useDocValue, boolean arrayLeniency) {
        super(name, dataType, zoneId, useDocValue, arrayLeniency);
    }

    public FieldHitExtractor(String name, DataType dataType, ZoneId zoneId, boolean useDocValue) {
        super(name, dataType, zoneId, useDocValue);
    }

    public FieldHitExtractor(String name, String fullFieldName, DataType dataType, ZoneId zoneId, boolean useDocValue, String hitName,
            boolean arrayLeniency) {
        super(name, fullFieldName, dataType, zoneId, useDocValue, hitName, arrayLeniency);
    }

    public FieldHitExtractor(StreamInput in) throws IOException {
        super(in);
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
        if (dataType() != DataType.GEO_POINT) {
            return false;
        }
        // we expect the point in [lon lat] or [lon lat alt] formats
        if (list.size() > 3 || list.size() < 1) {
            return false;
        }
        return list.get(0) instanceof Number;
    }

    @Override
    protected Object extractFromSource(Map<String, Object> map) {
        return super.extractFromSource(map);
    }

    @Override
    protected Object unwrapCustomValue(Object values) {
        DataType dataType = dataType();

        if (dataType == DataType.GEO_POINT) {
            try {
                GeoPoint geoPoint = GeoUtils.parseGeoPoint(values, true);
                return new GeoShape(geoPoint.lon(), geoPoint.lat());
            } catch (ElasticsearchParseException ex) {
                throw new SqlIllegalArgumentException("Cannot parse geo_point value [{}] (returned by [{}])", values, fieldName());
            }
        }
        if (dataType == DataType.GEO_SHAPE) {
            try {
                return new GeoShape(values);
            } catch (IOException ex) {
                throw new SqlIllegalArgumentException("Cannot read geo_shape value [{}] (returned by [{}])", values, fieldName());
            }
        }
        if (dataType == DataType.SHAPE) {
            try {
                return new GeoShape(values);
            } catch (IOException ex) {
                throw new SqlIllegalArgumentException("Cannot read shape value [{}] (returned by [{}])", values, fieldName());
            }
        }
        if (values instanceof Map) {
            throw new SqlIllegalArgumentException("Objects (returned by [{}]) are not supported", fieldName());
        }
        if (dataType == DataType.DATETIME) {
            if (values instanceof String) {
                return DateUtils.asDateTime(Long.parseLong(values.toString()), zoneId());
            }
        }

        return null;
    }

    @Override
    protected DataType dataType() {
        return super.dataType();
    }
}