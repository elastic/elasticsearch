/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geometry;

import org.elasticsearch.geometry.utils.CartesianValidator;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteOrder;

public class GeometryValidatorTests extends ESTestCase {

    public static class NoopValidator implements GeometryValidator {

        @Override
        public void validate(Geometry geometry) {

        }
    }

    public static class OneValidator extends GeographyValidator {
        /**
         * Minimum longitude value.
         */
        private static final double MIN_LON_INCL = -1D;

        /**
         * Maximum longitude value.
         */
        private static final double MAX_LON_INCL = 1D;

        /**
         * Minimum latitude value.
         */
        private static final double MIN_LAT_INCL = -1D;

        /**
         * Maximum latitude value.
         */
        private static final double MAX_LAT_INCL = 1D;

        /**
         * Minimum altitude value.
         */
        private static final double MIN_ALT_INCL = -1D;

        /**
         * Maximum altitude value.
         */
        private static final double MAX_ALT_INCL = 1D;

        public OneValidator() {
            super(true);
        }

        @Override
        protected void checkLatitude(double latitude) {
            if (Double.isNaN(latitude) || latitude < MIN_LAT_INCL || latitude > MAX_LAT_INCL) {
                throw new IllegalArgumentException(
                    "invalid latitude " + latitude + "; must be between " + MIN_LAT_INCL + " and " + MAX_LAT_INCL
                );
            }
        }

        @Override
        protected void checkLongitude(double longitude) {
            if (Double.isNaN(longitude) || longitude < MIN_LON_INCL || longitude > MAX_LON_INCL) {
                throw new IllegalArgumentException(
                    "invalid longitude " + longitude + "; must be between " + MIN_LON_INCL + " and " + MAX_LON_INCL
                );
            }
        }

        @Override
        protected void checkAltitude(double zValue) {
            if (Double.isNaN(zValue) == false && (zValue < MIN_ALT_INCL || zValue > MAX_ALT_INCL)) {
                throw new IllegalArgumentException(
                    "invalid altitude " + zValue + "; must be between " + MIN_ALT_INCL + " and " + MAX_ALT_INCL
                );
            }
        }
    }

    public void testNoopValidator() throws Exception {
        GeometryValidator validator = new NoopValidator();
        WellKnownText.fromWKT(validator, true, "CIRCLE (10000 20000 30000)");
        WellKnownText.fromWKT(validator, true, "POINT (10000 20000)");
        WellKnownText.fromWKT(validator, true, "LINESTRING (10000 20000, 0 0)");
        WellKnownText.fromWKT(validator, true, "POLYGON ((300 100, 400 200, 500 300, 300 100), (50 150, 250 150, 200 100))");
        WellKnownText.fromWKT(validator, true, "MULTIPOINT (10000 20000, 20000 30000)");
    }

    public void testOneValidator() throws Exception {
        GeometryValidator validator = new OneValidator();
        WellKnownText.fromWKT(validator, true, "POINT (0 1)");
        WellKnownText.fromWKT(validator, true, "POINT (0 1 0.5)");
        IllegalArgumentException ex;
        ex = expectThrows(IllegalArgumentException.class, () -> WellKnownText.fromWKT(validator, true, "CIRCLE (1 2 3)"));
        assertEquals("invalid latitude 2.0; must be between -1.0 and 1.0", ex.getMessage());
        ex = expectThrows(IllegalArgumentException.class, () -> WellKnownText.fromWKT(validator, true, "POINT (2 1)"));
        assertEquals("invalid longitude 2.0; must be between -1.0 and 1.0", ex.getMessage());
        ex = expectThrows(IllegalArgumentException.class, () -> WellKnownText.fromWKT(validator, true, "LINESTRING (1 -1 0, 0 0 2)"));
        assertEquals("invalid altitude 2.0; must be between -1.0 and 1.0", ex.getMessage());
        ex = expectThrows(
            IllegalArgumentException.class,
            () -> WellKnownText.fromWKT(validator, true, "POLYGON ((0.3 0.1, 0.4 0.2, 5 0.3, 0.3 0.1))")
        );
        assertEquals("invalid longitude 5.0; must be between -1.0 and 1.0", ex.getMessage());
        ex = expectThrows(
            IllegalArgumentException.class,
            () -> WellKnownText.fromWKT(validator, true, "POLYGON ((0.3 0.1, 0.4 0.2, 0.5 0.3, 0.3 0.1), (0.5 1.5, 2.5 1.5, 2.0 1.0))")
        );
        assertEquals("invalid latitude 1.5; must be between -1.0 and 1.0", ex.getMessage());
        ex = expectThrows(IllegalArgumentException.class, () -> WellKnownText.fromWKT(validator, true, "MULTIPOINT (0 1, -2 1)"));
        assertEquals("invalid longitude -2.0; must be between -1.0 and 1.0", ex.getMessage());
    }

    public void testGeographyValidatorAllowsAntimeridianCrossingBBox() throws Exception {
        // minX > maxX is a legitimate encoding of an envelope crossing the antimeridian; it must not be rejected.
        GeometryValidator validator = GeographyValidator.instance(true);
        WellKnownText.fromWKT(validator, false, "BBOX (170.0, -170.0, 10.0, -10.0)");
        WellKnownBinary.fromWKT("BBOX (170.0, -170.0, 10.0, -10.0)", ByteOrder.LITTLE_ENDIAN, false, validator);
    }

    public void testCartesianValidatorRejectsInvertedXBBox() throws Exception {
        GeometryValidator validator = CartesianValidator.INSTANCE;
        // A valid envelope is unaffected.
        WellKnownText.fromWKT(validator, false, "BBOX (0.0, 12.0, 60.0, 30.0)");
        WellKnownBinary.fromWKT("BBOX (0.0, 12.0, 60.0, 30.0)", ByteOrder.LITTLE_ENDIAN, false, validator);

        // maxX < minX has no antimeridian-crossing meaning for cartesian coordinates and must be rejected,
        // both via the Geometry-based path (old WKT2WKB implementation) and the direct WKT->WKB path (new).
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> WellKnownText.fromWKT(validator, false, "BBOX (12.0, 0.0, 60.0, 30.0)")
        );
        assertEquals("max x cannot be less than min x", ex.getMessage());
        ex = expectThrows(
            IllegalArgumentException.class,
            () -> WellKnownBinary.fromWKT("BBOX (12.0, 0.0, 60.0, 30.0)", ByteOrder.LITTLE_ENDIAN, false, validator)
        );
        assertEquals("max x cannot be less than min x", ex.getMessage());
    }
}
