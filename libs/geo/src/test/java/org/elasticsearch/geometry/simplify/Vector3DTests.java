/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry.simplify;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import static java.lang.Math.toRadians;
import static org.elasticsearch.geometry.simplify.SimplificationErrorCalculator.Point3D.from;
import static org.elasticsearch.geometry.simplify.SimplificationErrorCalculator.Point3D.sin;
import static org.hamcrest.Matchers.closeTo;

public class Vector3DTests extends ESTestCase {
    private static final double sin45 = sin(toRadians(45));
    private static final SimplificationErrorCalculator.Point3D xAxis = new SimplificationErrorCalculator.Point3D(1, 0, 0);
    private static final SimplificationErrorCalculator.Point3D yAxis = new SimplificationErrorCalculator.Point3D(0, 1, 0);
    private static final SimplificationErrorCalculator.Point3D zAxis = new SimplificationErrorCalculator.Point3D(0, 0, 1);

    public void testPoint3D() {
        assertThat("Should be on x-axis", from(0, 0), samePoint(1, 0, 0));
        assertThat("Should be on y-axis", from(0, 90), samePoint(0, 1, 0));
        assertThat("Should be on x-axis", from(0, 180), samePoint(-1, 0, 0));
        assertThat("Should be on y-axis", from(0, -90), samePoint(0, -1, 0));
        for (int longitude = -360; longitude < 360; longitude++) {
            assertThat("Should be on z-axis even with lon=" + longitude, from(90, 0), samePoint(0, 0, 1));
            assertThat("Should be on z-axis even with lon=" + longitude, from(-90, longitude), samePoint(0, 0, -1));
            for (int latitude = -90; latitude < 90; latitude++) {
                var point = from(latitude, latitude);
                assertThat("Point " + point + " should have unit length", point.length(), closeTo(1.0, 1e-15));
            }
        }
        assertThat("Should be between all three axes", from(45, 45), samePoint(0.5, 0.5, sin45));
        assertThat("Should be between all three axes", from(-45, -135), samePoint(-0.5, -0.5, -sin45));
        assertThat("XY should be between x and y axes", from(0, 45), samePoint(sin45, sin45, 0));
        assertThat("YZ should be between y and z axes", from(45, 90), samePoint(0, sin45, sin45));
        assertThat("ZX should be between z and x axes", from(45, 0), samePoint(sin45, 0, sin45));
    }

    public void testPoint3DCrossProduct() {
        assertThat("x-cross-y should be z", xAxis.cross(yAxis), samePoint(zAxis));
        assertThat("y-cross-z should be x", yAxis.cross(zAxis), samePoint(xAxis));
        assertThat("z-cross-x should be y", zAxis.cross(xAxis), samePoint(yAxis));
        assertThat("y-cross-x should be -z", yAxis.cross(xAxis), samePoint(0, 0, -1));
        assertThat("z-cross-y should be -x", zAxis.cross(yAxis), samePoint(-1, 0, 0));
        assertThat("x-cross-z should be -y", xAxis.cross(zAxis), samePoint(0, -1, 0));
        var xy = from(0, 45);
        var yz = from(45, 90);
        var zx = from(45, 0);
        assertThat("Inverse cross-product xy.cross(yz) == -yz.cross(xy)", xy.cross(yz), samePoint(yz.cross(xy).inverse()));
        assertThat("Inverse cross-product yz.cross(zx) == -zx.cross(yz)", yz.cross(zx), samePoint(zx.cross(yz).inverse()));
        assertThat("Inverse cross-product zx.cross(xy) == -xy.cross(zx)", zx.cross(xy), samePoint(xy.cross(zx).inverse()));
        assertThat("xy-cross-yz should point NW", xy.cross(yz), samePoint(0.5, -0.5, 0.5));
        assertThat("yz-cross-zx should point SE", yz.cross(zx), samePoint(0.5, 0.5, -0.5));
        assertThat("zx-cross-xy should point far-NE", zx.cross(xy), samePoint(-0.5, 0.5, 0.5));
        var xyz = from(45, 45);
        assertThat("xyz-cross-x should be on yz plane", xyz.cross(xAxis), samePoint(0.0, sin45, -0.5));
        assertThat("xyz-cross-y should be on zx plane", xyz.cross(yAxis), samePoint(-sin45, 0.0, 0.5));
        assertThat("xyz-cross-z should be on xy plane", xyz.cross(zAxis), samePoint(0.5, -0.5, 0.0));
    }

    public void testPoint3DDotProduct() {
        assertThat("x-dot-x should be 1", xAxis.dot(xAxis), closeTo(1.0, 1e-15));
        assertThat("y-dot-y should be 1", yAxis.dot(yAxis), closeTo(1.0, 1e-15));
        assertThat("z-dot-z should be 1", zAxis.dot(zAxis), closeTo(1.0, 1e-15));
        assertThat("x-dot-y should be 0", xAxis.dot(yAxis), closeTo(0.0, 1e-15));
        assertThat("y-dot-z should be 0", yAxis.dot(zAxis), closeTo(0.0, 1e-15));
        assertThat("z-dot-x should be 0", zAxis.dot(xAxis), closeTo(0.0, 1e-15));
        var xy = from(0, 45);
        var yz = from(45, 90);
        var zx = from(45, 0);
        assertThat("xy-dot-xy should be 1", xy.dot(xy), closeTo(1.0, 1e-15));
        assertThat("yz-dot-yz should be 1", yz.dot(yz), closeTo(1.0, 1e-15));
        assertThat("zx-dot-zx should be 1", zx.dot(zx), closeTo(1.0, 1e-15));
        assertThat("xy-dot-yz should be 0.5", xy.dot(yz), closeTo(0.5, 1e-15));
        assertThat("yz-dot-zx should be 0.5", yz.dot(zx), closeTo(0.5, 1e-15));
        assertThat("zx-dot-xy should be 0.5", zx.dot(xy), closeTo(0.5, 1e-15));
    }

    public void testPoint3DAngles() {
        double rad90 = Math.toRadians(90);
        assertThat("x-angle-x should be 0", xAxis.angleTo(xAxis), closeTo(0.0, 1e-15));
        assertThat("y-angle-y should be 0", yAxis.angleTo(yAxis), closeTo(0.0, 1e-15));
        assertThat("z-angle-z should be 0", zAxis.angleTo(zAxis), closeTo(0.0, 1e-15));
        assertThat("x-angle-y should be 90", xAxis.angleTo(yAxis), closeTo(rad90, 1e-15));
        assertThat("y-angle-z should be 90", yAxis.angleTo(zAxis), closeTo(rad90, 1e-15));
        assertThat("z-angle-x should be 90", zAxis.angleTo(xAxis), closeTo(rad90, 1e-15));
        var xy = from(0, 45);
        var yz = from(45, 90);
        var zx = from(45, 0);
        double expectedAngle = rad90 * 2.0 / 3.0;
        assertThat("xy-angle-xy should be 0", xy.angleTo(xy), closeTo(0.0, 1e-15));
        assertThat("yz-angle-yz should be 0", yz.angleTo(yz), closeTo(0.0, 1e-15));
        assertThat("zx-angle-zx should be 0", zx.angleTo(zx), closeTo(0.0, 1e-15));
        assertThat("xy-angle-yz should be <90", xy.angleTo(yz), closeTo(expectedAngle, 1e-15));
        assertThat("yz-angle-zx should be <90", yz.angleTo(zx), closeTo(expectedAngle, 1e-15));
        assertThat("zx-angle-xy should be <90", zx.angleTo(xy), closeTo(expectedAngle, 1e-15));
    }

    public void testRotationMatrix() {
        for (var axis : new SimplificationErrorCalculator.Point3D[] { xAxis, yAxis, zAxis, from(45, 45), from(-45, -45) }) {
            var rotate0 = new SimplificationErrorCalculator.RotationMatrix(axis, 0);
            assertThat("x-axis rotated about an axis by zero degrees", rotate0.multiply(xAxis), samePoint(xAxis));
            assertThat("y-axis rotated about an axis by zero degrees", rotate0.multiply(yAxis), samePoint(yAxis));
            assertThat("z-axis rotated about an axis by zero degrees", rotate0.multiply(zAxis), samePoint(zAxis));
        }
        {
            var rotateX90 = new SimplificationErrorCalculator.RotationMatrix(xAxis, toRadians(90));
            assertThat("x-axis rotated about the x-axis by 90 degrees", rotateX90.multiply(xAxis), samePoint(xAxis));
            assertThat("y-axis rotated about the x-axis by 90 degrees", rotateX90.multiply(yAxis), samePoint(zAxis));
            assertThat("z-axis rotated about the x-axis by 90 degrees", rotateX90.multiply(zAxis), samePoint(yAxis.inverse()));
        }
        {
            double angle = toRadians(45);
            var rotateX45 = new SimplificationErrorCalculator.RotationMatrix(xAxis, angle);
            assertThat("x-axis rotated about the x-axis by 45 degrees", rotateX45.multiply(xAxis), samePoint(xAxis));
            assertThat("y-axis rotated about the x-axis by 45 degrees", rotateX45.multiply(yAxis), samePoint(0, sin45, sin45));
            assertThat("z-axis rotated about the x-axis by 45 degrees", rotateX45.multiply(zAxis), samePoint(0, -sin45, sin45));
            assertThat("x-axis rotated about the x-axis by 45 degrees", rotate(xAxis, xAxis, angle, 1), samePoint(xAxis));
            assertThat("y-axis rotated about the x-axis by 45 degrees", rotate(yAxis, xAxis, angle, 1), samePoint(0, sin45, sin45));
            assertThat("z-axis rotated about the x-axis by 45 degrees", rotate(zAxis, xAxis, angle, 1), samePoint(0, -sin45, sin45));
            assertThat("x-axis rotated about the x-axis by 2*45 degrees", rotate(xAxis, xAxis, angle, 2), samePoint(xAxis));
            assertThat("y-axis rotated about the x-axis by 2*45 degrees", rotate(yAxis, xAxis, angle, 2), samePoint(zAxis));
            assertThat("z-axis rotated about the x-axis by 2*45 degrees", rotate(zAxis, xAxis, angle, 2), samePoint(yAxis.inverse()));
            assertThat("x-axis rotated about the x-axis by 3*45 degrees", rotate(xAxis, xAxis, angle, 3), samePoint(xAxis));
            assertThat("y-axis rotated about the x-axis by 3*45 degrees", rotate(yAxis, xAxis, angle, 3), samePoint(0, -sin45, sin45));
            assertThat("z-axis rotated about the x-axis by 3*45 degrees", rotate(zAxis, xAxis, angle, 3), samePoint(0, -sin45, -sin45));
            assertThat("x-axis rotated about the x-axis by 4*45 degrees", rotate(xAxis, xAxis, angle, 4), samePoint(xAxis));
            assertThat("y-axis rotated about the x-axis by 4*45 degrees", rotate(yAxis, xAxis, angle, 4), samePoint(yAxis.inverse()));
            assertThat("z-axis rotated about the x-axis by 4*45 degrees", rotate(zAxis, xAxis, angle, 4), samePoint(zAxis.inverse()));
        }
        {
            var rotateX_90 = new SimplificationErrorCalculator.RotationMatrix(xAxis, toRadians(-90));
            assertThat("x-axis rotated about the x-axis by -90 degrees", rotateX_90.multiply(xAxis), samePoint(xAxis));
            assertThat("y-axis rotated about the x-axis by -90 degrees", rotateX_90.multiply(yAxis), samePoint(zAxis.inverse()));
            assertThat("z-axis rotated about the x-axis by -90 degrees", rotateX_90.multiply(zAxis), samePoint(yAxis));
        }
        {
            var rotateY90 = new SimplificationErrorCalculator.RotationMatrix(yAxis, toRadians(90));
            assertThat("x-axis rotated about the y-axis by 90 degrees", rotateY90.multiply(xAxis), samePoint(zAxis.inverse()));
            assertThat("y-axis rotated about the y-axis by 90 degrees", rotateY90.multiply(yAxis), samePoint(yAxis));
            assertThat("z-axis rotated about the y-axis by 90 degrees", rotateY90.multiply(zAxis), samePoint(xAxis));
        }
        {
            var rotateZ_90 = new SimplificationErrorCalculator.RotationMatrix(zAxis, toRadians(-90));
            assertThat("x-axis rotated about the z-axis by -90 degrees", rotateZ_90.multiply(xAxis), samePoint(yAxis.inverse()));
            assertThat("y-axis rotated about the z-axis by -90 degrees", rotateZ_90.multiply(yAxis), samePoint(xAxis));
            assertThat("z-axis rotated about the z-axis by -90 degrees", rotateZ_90.multiply(zAxis), samePoint(zAxis));
        }
        {
            var xy = from(0, 45);
            var rotateXY90 = new SimplificationErrorCalculator.RotationMatrix(xy, toRadians(90));
            assertThat("xy-axis rotated about the xy-axis by 90 degrees", rotateXY90.multiply(xy), samePoint(xy));
            assertThat("x-axis rotated about the xy-axis by 90 degrees", rotateXY90.multiply(xAxis), samePoint(0.5, 0.5, -sin45));
            assertThat("y-axis rotated about the xy-axis by 90 degrees", rotateXY90.multiply(yAxis), samePoint(0.5, 0.5, sin45));
            assertThat("z-axis rotated about the xy-axis by 90 degrees", rotateXY90.multiply(zAxis), samePoint(sin45, -sin45, 0));
        }
        {
            var xyz = from(45, 45);
            var rotateXYZ_90 = new SimplificationErrorCalculator.RotationMatrix(xyz, toRadians(-90));
            double vs = 0.042893218813452316;
            double sm = 0.14644660940672605;
            assertThat("xyz-axis rotated about the xyz-axis by -90 degrees", rotateXYZ_90.multiply(xyz), samePoint(xyz));
            assertThat(
                "xyz-axis rotated about the xyz-axis by -90 degrees",
                rotateXYZ_90.multiply(xyz.inverse()),
                samePoint(xyz.inverse())
            );
            assertThat("x-axis rotated about the xyz-axis by -90 degrees", rotateXYZ_90.multiply(xAxis), samePoint(0.25, vs - 0.5, 1 - sm));
            assertThat("y-axis rotated about the xyz-axis by -90 degrees", rotateXYZ_90.multiply(yAxis), samePoint(1 - vs, 0.25, -sm));
            assertThat("z-axis rotated about the xyz-axis by -90 degrees", rotateXYZ_90.multiply(zAxis), samePoint(-sm, 1 - sm, 0.5));
            var v0 = from(10, -80);
            var v1 = rotateXYZ_90.multiply(v0);
            var v2 = rotateXYZ_90.multiply(v1);
            var v3 = rotateXYZ_90.multiply(v2);
            var v4 = rotateXYZ_90.multiply(v3);
            assertThat("Rotating vector by 360 degrees over any axis should lead to identity", v4, samePoint(v0));
        }
    }

    private SimplificationErrorCalculator.Point3D rotate(
        SimplificationErrorCalculator.Point3D original,
        SimplificationErrorCalculator.Point3D axis,
        double angle,
        int count
    ) {
        SimplificationErrorCalculator.Point3D result = original;
        for (int i = 0; i < count; i++) {
            result = result.rotate(axis, angle);
        }
        return result;
    }

    public void testVectorRotations() {
        var xAxis = from(0, 0);
        var a = from(45, 45);
        var b = from(46, 44);
        var c = from(44, 44);
        double xab = 0.021313151879915497;
        double xbc = 0.03490658503988249;
        double xca = 0.021437514314357493;
        double ab = a.angleTo(b);
        double bc = b.angleTo(c);
        double ca = c.angleTo(a);
        assertThat("Angle ab", ab, closeTo(xab, 1e-15));
        assertThat("Angle bc", bc, closeTo(xbc, 1e-15));
        assertThat("Angle ca", ca, closeTo(xca, 1e-15));
        double rotationAngle = xAxis.angleTo(a);
        assertThat("Angle xa", rotationAngle, closeTo(Math.PI / 3, 1e-15));
        var rotationAxis = a.cross(xAxis);
        assertThat("Rotation axis", rotationAxis, samePoint(0.0, sin45, -0.5));
        var na = a.rotate(rotationAxis, rotationAngle);
        var nb = b.rotate(rotationAxis, rotationAngle);
        var nc = c.rotate(rotationAxis, rotationAngle);
        // TODO: fix
        // assertThat("A rotated to x-Axis", na, samePoint(xAxis));
        double nab = na.angleTo(nb);
        double nbc = nb.angleTo(nc);
        double nca = nc.angleTo(na);
        double error = 1e-3; // TODO: fix to be more accurate
        assertThat("Angle ab (rotated)", nab, closeTo(xab, error));
        assertThat("Angle bc (rotated)", nbc, closeTo(xbc, error));
        assertThat("Angle ca (rotated)", nca, closeTo(xca, error));
    }

    private static Matcher<SimplificationErrorCalculator.Point3D> samePoint(double x, double y, double z) {
        return new TestPoint3DMatcher(new SimplificationErrorCalculator.Point3D(x, y, z), 1e-15);
    }

    private static Matcher<SimplificationErrorCalculator.Point3D> samePoint(SimplificationErrorCalculator.Point3D expected) {
        return new TestPoint3DMatcher(expected, 1e-15);
    }

    private static class TestPoint3DMatcher extends BaseMatcher<SimplificationErrorCalculator.Point3D> {
        private final Matcher<Double> xMatcher;
        private final Matcher<Double> yMatcher;
        private final Matcher<Double> zMatcher;
        private final SimplificationErrorCalculator.Point3D expected;

        TestPoint3DMatcher(SimplificationErrorCalculator.Point3D expected, double error) {
            this.expected = expected;
            this.xMatcher = closeTo(expected.x(), error);
            this.yMatcher = closeTo(expected.y(), error);
            this.zMatcher = closeTo(expected.z(), error);
        }

        @Override
        public boolean matches(Object actual) {
            if (actual instanceof SimplificationErrorCalculator.Point3D point3D) {
                return xMatcher.matches(point3D.x()) && yMatcher.matches(point3D.y()) && zMatcher.matches(point3D.z());
            }
            return false;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("Point3D close to <" + expected + ">");
        }
    }
}
