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

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.Objects;

public class ProximityBoost extends ScriptScoreBoost {
    private final GeometryValidator geoValidator = GeographyValidator.instance(true);
    private final DateMathParser dateParser = DateFormatter.forPattern("date_optional_time||epoch_millis").toDateMathParser();

    private final String center;
    private final FunctionType function;
    private final Float factor;

    public static final String TYPE = "proximity";

    public enum JsDecayFunction {
        decayNumericLinear,
        decayNumericExp,
        decayNumericGauss,
        decayGeoLinear,
        decayGeoExp,
        decayGeoGauss,
        decayDateLinear,
        decayDateExp,
        decayDateGauss
    }

    public enum CenterType {
        Numeric,
        Geo,
        Date
    }

    public enum FunctionType {
        linear,
        exponential,
        gaussian
    }

    private CenterType centerType = null;

    public ProximityBoost(String center, String function, Float factor) {
        super(TYPE, OperationType.add.toString());
        this.center = center;
        this.function = FunctionType.valueOf(function);
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
        }
    }

    private NumberFormat getNumberFormat() {
        return NumberFormat.getInstance(Locale.ROOT);
    }

    public boolean isGeo() {
        try {
            String[] latLon = center.split(",", 2);
            getNumberFormat().parse(latLon[0].trim());
            getNumberFormat().parse(latLon[1].trim());
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
        } catch (Exception e) {
            // do nothing
        }
        try {
            Integer.valueOf(center);
            return true;
        } catch (Exception e) {
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
        return getNumberFormat().parse(center);
    }

    public String getDateCenter() {
        if (Objects.equals(center, "now")) {
            return dateParser.parse(center, () -> 0).toString();
        }
        return center;
    }

    public FunctionType getFunction() {
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
        return (this.type.equals(that.getType())
            && this.function.equals(that.getFunction())
            && this.center.equals(that.getCenter())
            && this.factor.equals(that.getFactor()));
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
        } catch (Exception e) {
            return null;
        }
        return null;
    }

    private String getNumericSource(String field) throws ParseException {
        Double scale = Math.max(Math.abs(getNumericCenter().doubleValue() / 2f), 1f);
        JsDecayFunction jsFunc = JsDecayFunction.decayNumericLinear;
        switch (this.function) {
            case linear -> jsFunc = JsDecayFunction.decayNumericLinear;
            case exponential -> jsFunc = JsDecayFunction.decayNumericExp;
            case gaussian -> jsFunc = JsDecayFunction.decayNumericGauss;
        }
        String funcCall = format(
            "{0}({1}, {2}, 0.0, 0.5, doc[''{3}''].value)",
            jsFunc,
            center, // origin
            scale,
            field
        );
        return format("{0} * ((doc[''{1}''].size() > 0) ? {2} : {3})", getFactor(), field, funcCall, constantFactor());
    }

    private String getGeoSource(String field) {
        JsDecayFunction jsFunc = JsDecayFunction.decayGeoLinear;
        switch (this.function) {
            case linear -> jsFunc = JsDecayFunction.decayGeoLinear;
            case exponential -> jsFunc = JsDecayFunction.decayGeoExp;
            case gaussian -> jsFunc = JsDecayFunction.decayGeoGauss;
        }
        String funcCall = format(
            "{0}(''{1}'', ''{2}'', ''0km'', 0.5, doc[''{3}''].value)",
            jsFunc,
            center, // origin
            "1km",
            field
        );
        return format("{0} * ((doc[''{1}''].size() > 0) ? {2} : {3})", getFactor(), field, funcCall, constantFactor());
    }

    private String getDateSource(String field) {
        JsDecayFunction jsFunc = JsDecayFunction.decayDateLinear;
        switch (this.function) {
            case linear -> jsFunc = JsDecayFunction.decayDateLinear;
            case exponential -> jsFunc = JsDecayFunction.decayDateExp;
            case gaussian -> jsFunc = JsDecayFunction.decayDateGauss;
        }
        String funcCall = format(
            "{0}(''{1}'', ''{2}'', ''0'', 0.5, doc[''{3}''].value)",
            jsFunc,
            getDateCenter(), // origin
            "1d",
            field
        );
        return format("{0} * ((doc[''{1}''].size() > 0) ? {2} : {3})", getFactor(), field, funcCall, constantFactor());
    }
}
