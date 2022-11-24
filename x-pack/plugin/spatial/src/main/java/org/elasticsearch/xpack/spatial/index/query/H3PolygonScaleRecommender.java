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
    private final Map<String, Inflation> recommendations;
    private static final CannotInflate CANNOT_INFLATE = new CannotInflate();
    public static final H3PolygonScaleRecommender SPHERICAL = new H3PolygonScaleRecommender(1.17, sphericalExceptions());
    public static final H3PolygonScaleRecommender PLANAR = new H3PolygonScaleRecommender(1.30, planarExceptions());
    private final InflationFactor scratchFactor = new InflationFactor(1.0);

    private H3PolygonScaleRecommender(double scaleFactor, Map<String, Inflation> recommendations) {
        this.scaleFactor = scaleFactor;
        this.recommendations = recommendations;
        this.min = reduce(scaleFactor, recommendations.values(), Math::min);
        this.max = reduce(scaleFactor, recommendations.values(), Math::max);
    }

    private static double reduce(double scaleFactor, Collection<Inflation> values, BiFunction<Double, Double, Double> selector) {
        for (Inflation value : values) {
            if (value.canInflate()) {
                scaleFactor = selector.apply(scaleFactor, value.scaleFactor());
            }
        }
        return scaleFactor;
    }

    public Inflation recommend(String address) {
        return recommendations.getOrDefault(address, scratchFactor.withFactor(scaleFactor));
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

    private static Map<String, Inflation> sphericalExceptions() {
        HashMap<String, Inflation> exceptions = new HashMap<>();
        exceptions.put("851c1daffffffff", new InflationFactor(1.181));
        exceptions.put("83ea03fffffffff", new InflationFactor(1.180));
        exceptions.put("83a606fffffffff", new InflationFactor(1.180));
        exceptions.put("83c38efffffffff", new InflationFactor(1.178));
        exceptions.put("8330aafffffffff", new InflationFactor(1.178));
        exceptions.put("8319b2fffffffff", new InflationFactor(1.172));
        return exceptions;
    }

    private static Map<String, Inflation> planarExceptions() {
        HashMap<String, Inflation> exceptions = new HashMap<>();
        exceptions.put("8001fffffffffff", CANNOT_INFLATE);
        exceptions.put("8003fffffffffff", CANNOT_INFLATE);
        exceptions.put("80effffffffffff", CANNOT_INFLATE);
        exceptions.put("80f1fffffffffff", CANNOT_INFLATE); // Failed to find truncation
        exceptions.put("80f3fffffffffff", CANNOT_INFLATE); // Failed to find truncation
        exceptions.put("81037ffffffffff", CANNOT_INFLATE); // new InflationFactor(2.530)
        exceptions.put("8007fffffffffff", CANNOT_INFLATE); // new InflationFactor(1.839)
        exceptions.put("81f3bffffffffff", new InflationFactor(1.440));
        exceptions.put("80edfffffffffff", new InflationFactor(1.418));
        exceptions.put("8009fffffffffff", new InflationFactor(1.409));
        exceptions.put("80ebfffffffffff", new InflationFactor(1.395));
        exceptions.put("800dfffffffffff", new InflationFactor(1.383));
        exceptions.put("80e9fffffffffff", new InflationFactor(1.378));
        exceptions.put("81f2fffffffffff", new InflationFactor(1.366));
        exceptions.put("8103bffffffffff", new InflationFactor(1.365));
        exceptions.put("80e7fffffffffff", new InflationFactor(1.355));
        exceptions.put("800bfffffffffff", new InflationFactor(1.346));
        exceptions.put("81003ffffffffff", new InflationFactor(1.343));
        exceptions.put("81f03ffffffffff", new InflationFactor(1.335));
        exceptions.put("80e5fffffffffff", new InflationFactor(1.334));
        exceptions.put("81f23ffffffffff", new InflationFactor(1.328));
        exceptions.put("800ffffffffffff", new InflationFactor(1.324));
        exceptions.put("81efbffffffffff", new InflationFactor(1.322));
        exceptions.put("81053ffffffffff", new InflationFactor(1.322));
        exceptions.put("81023ffffffffff", new InflationFactor(1.321));
        exceptions.put("8015fffffffffff", new InflationFactor(1.300));
        return exceptions;
    }

    public interface Inflation {
        boolean canInflate();

        double scaleFactor();
    }

    private static class InflationFactor implements Inflation {
        public double scaleFactor;

        private InflationFactor(double scaleFactor) {
            this.scaleFactor = scaleFactor;
        }

        @Override
        public boolean canInflate() {
            return true;
        }

        @Override
        public double scaleFactor() {
            return scaleFactor;
        }

        private InflationFactor withFactor(double scaleFactor) {
            this.scaleFactor = scaleFactor;
            return this;
        }
    }

    private static class CannotInflate implements Inflation {

        @Override
        public boolean canInflate() {
            return false;
        }

        @Override
        public double scaleFactor() {
            throw new IllegalStateException("Cannot get inflation factor for cell that cannot be inflated");
        }
    }
}
