/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.util.math;

/**
 * @author kimchy (shay.banon)
 */
public class UnboxedMathUtils {

    public static double sin(Double a) {
        return Math.sin(a);
    }

    public static double cos(Double a) {
        return Math.cos(a); // default impl. delegates to StrictMath
    }

    public static double tan(Double a) {
        return Math.tan(a); // default impl. delegates to StrictMath
    }

    public static double asin(Double a) {
        return Math.asin(a); // default impl. delegates to StrictMath
    }

    public static double acos(Double a) {
        return Math.acos(a); // default impl. delegates to StrictMath
    }

    public static double atan(Double a) {
        return Math.atan(a); // default impl. delegates to StrictMath
    }

    public static double toRadians(Double angdeg) {
        return Math.toRadians(angdeg);
    }

    public static double toDegrees(Double angrad) {
        return Math.toDegrees(angrad);
    }

    public static double exp(Double a) {
        return Math.exp(a);
    }

    public static double log(Double a) {
        return Math.log(a);
    }

    public static double log10(Double a) {
        return Math.log10(a);
    }

    public static double sqrt(Double a) {
        return Math.sqrt(a);
    }


    public static double cbrt(Double a) {
        return Math.cbrt(a);
    }

    public static double IEEEremainder(Double f1, Double f2) {
        return Math.IEEEremainder(f1, f2);
    }

    public static double ceil(Double a) {
        return Math.ceil(a);
    }

    public static double floor(Double a) {
        return Math.floor(a);
    }

    public static double rint(Double a) {
        return Math.rint(a);
    }

    public static double atan2(Double y, Double x) {
        return Math.atan2(y, x);
    }

    public static double pow(Double a, Double b) {
        return Math.pow(a, b);
    }

    public static int round(Float a) {
        return Math.round(a);
    }

    public static long round(Double a) {
        return Math.round(a);
    }

    public static double random() {
        return Math.random();
    }

    public static int abs(Integer a) {
        return Math.abs(a);
    }

    public static long abs(Long a) {
        return Math.abs(a);
    }

    public static float abs(Float a) {
        return Math.abs(a);
    }

    public static double abs(Double a) {
        return Math.abs(a);
    }

    public static int max(Integer a, Integer b) {
        return Math.max(a, b);
    }

    public static long max(Long a, Long b) {
        return Math.max(a, b);
    }

    public static float max(Float a, Float b) {
        return Math.max(a, b);
    }

    public static double max(Double a, Double b) {
        return Math.max(a, b);
    }

    public static int min(Integer a, Integer b) {
        return Math.min(a, b);
    }

    public static long min(Long a, Long b) {
        return Math.min(a, b);
    }

    public static float min(Float a, Float b) {
        return Math.min(a, b);
    }

    public static double min(Double a, Double b) {
        return Math.min(a, b);
    }

    public static double ulp(Double d) {
        return Math.ulp(d);
    }

    public static float ulp(Float f) {
        return Math.ulp(f);
    }

    public static double signum(Double d) {
        return Math.signum(d);
    }

    public static float signum(Float f) {
        return Math.signum(f);
    }

    public static double sinh(Double x) {
        return Math.sinh(x);
    }

    public static double cosh(Double x) {
        return Math.cosh(x);
    }

    public static double tanh(Double x) {
        return Math.tanh(x);
    }

    public static double hypot(Double x, Double y) {
        return Math.hypot(x, y);
    }

    public static double expm1(Double x) {
        return Math.expm1(x);
    }

    public static double log1p(Double x) {
        return Math.log1p(x);
    }

    public static double copySign(Double magnitude, Double sign) {
        return Math.copySign(magnitude, sign);
    }

    public static float copySign(Float magnitude, Float sign) {
        return Math.copySign(magnitude, sign);
    }

    public static int getExponent(Float f) {
        return Math.getExponent(f);
    }

    public static int getExponent(Double d) {
        return Math.getExponent(d);
    }

    public static double nextAfter(Double start, Double direction) {
        return Math.nextAfter(start, direction);
    }

    public static float nextAfter(Float start, Double direction) {
        return Math.nextAfter(start, direction);
    }

    public static double nextUp(Double d) {
        return Math.nextUp(d);
    }

    public static float nextUp(Float f) {
        return Math.nextUp(f);
    }


    public static double scalb(Double d, Integer scaleFactor) {
        return Math.scalb(d, scaleFactor);
    }

    public static float scalb(Float f, Integer scaleFactor) {
        return Math.scalb(f, scaleFactor);
    }
}
