/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance.boosts;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.Objects;

public class ProximityBoost extends AbstractScriptScoreBoost {
    private final DateMathParser dateParser = DateFormatter.forPattern("date_optional_time||epoch_millis").toDateMathParser();

    private final String center;
    private final FunctionType function;
    private final Float factor;

    public static final String TYPE = "proximity";

    private CenterType centerType = null;

    public ProximityBoost(String center, String function, Float factor) {
        super(TYPE, OperationType.ADD.toString());
        this.center = center;
        this.function = FunctionType.valueOf(function.toUpperCase(Locale.ROOT));
        this.factor = factor;
        this.setCenterType();
    }

    public void setCenterType() {
        centerType = null;
        if (isNumber()) {
            centerType = CenterType.NUMERIC;
        } else if (isDate()) {
            centerType = CenterType.DATE;
        } else if (isGeo()) {
            centerType = CenterType.GEO;
        }
    }

    boolean isGeo() {
        try {
            GeoUtils.parseFromString(center);
            return true;
        } catch (ElasticsearchParseException e) {
            return false;
        }
    }

    boolean isNumber() {
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

    private NumberFormat getNumberFormat() {
        return NumberFormat.getInstance(Locale.ROOT);
    }

    boolean isDate() {
        try {
            dateParser.parse(center, () -> 0);
            return true;
        } catch (ElasticsearchParseException e) {
            return false;
        }
    }

    @Override
    public String getSource(String field) {
        try {
            switch (this.centerType) {
                case NUMERIC -> {
                    return getNumericSource(field);
                }
                case GEO -> {
                    return getGeoSource(field);
                }
                case DATE -> {
                    return getDateSource(field);
                }
            }
        } catch (ParseException e) {
            return null;
        }
        return null;
    }

    private String getNumericSource(String field) throws ParseException {
        Double scale = Math.max(Math.abs(getNumericCenter().doubleValue() / 2f), 1f);
        String funcCall = format(
            "{0}({1}, {2}, 0.0, 0.5, doc[''{3}''].value)",
            getFunctionName(),
            center, // origin
            scale,
            field
        );
        return format("{0} * ((doc[''{1}''].size() > 0) ? {2} : {3})", getFactor(), field, funcCall, constantFactor());
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

    private String getGeoSource(String field) {
        String funcCall = format(
            "{0}(''{1}'', ''{2}'', ''0km'', 0.5, doc[''{3}''].value)",
            getFunctionName(),
            center, // origin
            "1km",
            field
        );
        return format("{0} * ((doc[''{1}''].size() > 0) ? {2} : {3})", getFactor(), field, funcCall, constantFactor());
    }

    private String getDateSource(String field) {
        String funcCall = format(
            "{0}(''{1}'', ''{2}'', ''0'', 0.5, doc[''{3}''].value)",
            getFunctionName(),
            getDateCenter(), // origin
            "1d",
            field
        );
        return format("{0} * ((doc[''{1}''].size() > 0) ? {2} : {3})", getFactor(), field, funcCall, constantFactor());
    }

    private String getFunctionName() {
        return "decay" + centerType.getName() + function.getName();
    }

    public enum CenterType {
        NUMERIC("Numeric"),
        GEO("Geo"),
        DATE("Date");

        private final String name;

        CenterType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public enum FunctionType {
        LINEAR("Linear"),
        EXPONENTIAL("Exp"),
        GAUSSIAN("Gauss");

        private final String name;

        FunctionType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
