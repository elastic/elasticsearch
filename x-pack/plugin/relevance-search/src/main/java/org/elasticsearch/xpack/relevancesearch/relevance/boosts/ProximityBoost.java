/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance.boosts;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.text.MessageFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Objects;

public class ProximityBoost extends ScriptScoreBoost {
    private final GeometryValidator geoValidator = GeographyValidator.instance(true);
    private final DateMathParser dateParser = DateFormatter.forPattern("date_optional_time||epoch_millis").toDateMathParser();

    private final String center;
    private final String function;
    private final Float factor;

    public static final String TYPE = "proximity";

    public enum CenterType {
        Numeric,
        Geo,
        Date
    }

    private CenterType centerType = null;

    public ProximityBoost(String center, String function, Float factor) {
        super(TYPE, "add");
        this.center = center;
        this.function = function;
        this.factor = factor;
        this.setCenterType();
    }

    public void setCenterType() {
        centerType = null;
        if (isNumber()) {
            centerType = CenterType.Numeric;
            return;
        }
        if (isDate()) {
            centerType = CenterType.Date;
            return;
        }
        if (isGeo()) {
            centerType = CenterType.Geo;
            return;
        }
        throw new IllegalArgumentException("Invalid center: [" + center + "]");
    }

    public boolean isGeo() {
        try {
            String[] latLon = center.split(",", 2);
            NumberFormat.getInstance().parse(latLon[0].trim());
            NumberFormat.getInstance().parse(latLon[1].trim());
            return true;
        } catch (Exception e) {
            // do nothing
        }
        try {
            Geohash.decodeLatitude(center);
            Geohash.decodeLongitude(center);
            return true;
        } catch (Exception e) {
            // do nothing
        }
        try {
            WellKnownText.fromWKT(geoValidator, true, center);
            return true;
        } catch (Exception e) {
            // do nothing
        }
        return false;
    }

    public boolean isNumber() {
        try {
            Double.valueOf(center);
            return true;
        } catch (Exception x) {
            // do nothing
        }
        try {
            Integer.valueOf(center);
            return true;
        } catch (Exception x) {
            // do nothing
        }
        return false;
    }

    public boolean isDate() {
        try {
            dateParser.parse(center, () -> 0);
            return true;
        } catch (ElasticsearchParseException e) {
            return false;
        }
    }

    public String getCenter() {
        return center;
    }

    public Number getNumericCenter() throws ParseException {
        return NumberFormat.getInstance().parse(center);
    }

    public String getDateCenter() {
        if (Objects.equals(center, "now")) {
            return dateParser.parse(center, () -> 0).toString();
        }
        return center;
    }

    public String getFunction() {
        return function;
    }

    public Float getFactor() {
        return factor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProximityBoost that = (ProximityBoost) o;
        return (this.function.equals(that.getFunction()) && this.center.equals(that.getCenter()) && this.factor.equals(that.getFactor()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, center, function, factor);
    }

    @Override
    public String getSource(String field) {
        try {
            switch (this.centerType) {
                case Numeric -> {
                    return getNumericSource(field);
                }
                case Geo -> {
                    return getGeoSource(field);
                }
                case Date -> {
                    return getDateSource(field);
                }
            }
        } catch (Exception x) {
            // do nothing
        }
        return null;
    }

    private String getNumericSource(String field) throws ParseException {
        Double scale = Math.max(Math.abs(getNumericCenter().doubleValue() / 2f), 1f);
        String jsFunc = "decayNumericLinear";
        switch (this.function) {
            case "linear" -> jsFunc = "decayNumericLinear";
            case "exponential" -> jsFunc = "decayNumericExp";
            case "gaussian" -> jsFunc = "decayNumericGauss";
        }
        return MessageFormat.format(
            "{0} * {1}({2}, {3}, 0, 0.5, {4})",
            getFactor(),
            jsFunc,
            center, // origin
            scale, // scale
            safeValue(field)
        );
    }

    private String getGeoSource(String field) {
        String jsFunc = "decayGeoLinear";
        switch (this.function) {
            case "linear" -> jsFunc = "decayGeoLinear";
            case "exponential" -> jsFunc = "decayGeoExp";
            case "gaussian" -> jsFunc = "decayGeoGauss";
        }
        return MessageFormat.format(
            "{0} * {1}(''{2}'', ''{3}'', 0, 0.5, {4})",
            getFactor(),
            jsFunc,
            center, // origin
            "1km", // scale
            safeValue(field)
        );
    }

    private String getDateSource(String field) {
        String jsFunc = "decayDateLinear";
        switch (this.function) {
            case "linear" -> jsFunc = "decayDateLinear";
            case "exponential" -> jsFunc = "decayDateExp";
            case "gaussian" -> jsFunc = "decayDateGauss";
        }
        return MessageFormat.format(
            "{0} * {1}(''{2}'', ''{3}'', 0, 0.5, {4})",
            getFactor(),
            jsFunc,
            getDateCenter(), // origin
            "1d", // scale
            safeValue(field)
        );
    }
}
