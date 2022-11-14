/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.query;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class H3PolygonScaleRecommender {
    private final double scaleFactor;
    private final double min;
    private final double max;
    private final Map<String, Double> recommendations;
    public static final H3PolygonScaleRecommender SPHERICAL = new H3PolygonScaleRecommender(1.17, sphericalExceptions());
    public static final H3PolygonScaleRecommender PLANAR = new H3PolygonScaleRecommender(1.30, planarExceptions());

    private H3PolygonScaleRecommender(double scaleFactor, Map<String, Double> recommendations) {
        this.scaleFactor = scaleFactor;
        this.recommendations = recommendations;
        this.min = reduce(scaleFactor, recommendations.values(), Math::min);
        this.max = reduce(scaleFactor, recommendations.values(), Math::max);
    }

    private static double reduce(double scaleFactor, Collection<Double> values, BiFunction<Double, Double, Double> selector) {
        for (double value : values) {
            scaleFactor = selector.apply(scaleFactor, value);
        }
        return scaleFactor;
    }

    public double recommend(String address) {
        return recommendations.getOrDefault(address, scaleFactor);
    }

    public double scaleFactor() {
        return scaleFactor;
    }

    public double min() {
        return min;
    }

    public double max() {
        return max;
    }

    private static Map<String, Double> sphericalExceptions() {
        HashMap<String, Double> exceptions = new HashMap<>();
        exceptions.put("851c1daffffffff", 1.181);
        exceptions.put("83ea03fffffffff", 1.180);
        exceptions.put("83a606fffffffff", 1.180);
        exceptions.put("83c38efffffffff", 1.178);
        exceptions.put("8330aafffffffff", 1.178);
        exceptions.put("8319b2fffffffff", 1.172);
        return exceptions;
    }

    private static Map<String, Double> planarExceptions() {
        HashMap<String, Double> exceptions = new HashMap<>();
        exceptions.put("81037ffffffffff", 2.530);
        exceptions.put("8007fffffffffff", 1.839);
        exceptions.put("81f3bffffffffff", 1.440);
        exceptions.put("80edfffffffffff", 1.418);
        exceptions.put("8009fffffffffff", 1.409);
        exceptions.put("80ebfffffffffff", 1.395);
        exceptions.put("800dfffffffffff", 1.383);
        exceptions.put("80e9fffffffffff", 1.378);
        exceptions.put("81f2fffffffffff", 1.366);
        exceptions.put("8103bffffffffff", 1.365);
        exceptions.put("80e7fffffffffff", 1.355);
        exceptions.put("800bfffffffffff", 1.346);
        exceptions.put("81003ffffffffff", 1.343);
        exceptions.put("81f03ffffffffff", 1.335);
        exceptions.put("80e5fffffffffff", 1.334);
        exceptions.put("81f23ffffffffff", 1.328);
        exceptions.put("800ffffffffffff", 1.324);
        exceptions.put("81efbffffffffff", 1.322);
        exceptions.put("81053ffffffffff", 1.322);
        exceptions.put("81023ffffffffff", 1.321);
        exceptions.put("8015fffffffffff", 1.300);
        return exceptions;
    }
}
