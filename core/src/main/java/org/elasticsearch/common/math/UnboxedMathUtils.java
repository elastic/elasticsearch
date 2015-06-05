/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.common.math;

import org.elasticsearch.common.SuppressForbidden;

import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class UnboxedMathUtils {

    public static double sin(Short a) {
        return Math.sin(a.doubleValue());
    }

    public static double sin(Integer a) {
        return Math.sin(a.doubleValue());
    }

    public static double sin(Float a) {
        return Math.sin(a.doubleValue());
    }

    public static double sin(Long a) {
        return Math.sin(a.doubleValue());
    }

    public static double sin(Double a) {
        return Math.sin(a);
    }

    public static double cos(Short a) {
        return Math.cos(a.doubleValue());
    }

    public static double cos(Integer a) {
        return Math.cos(a.doubleValue());
    }

    public static double cos(Float a) {
        return Math.cos(a.doubleValue());
    }

    public static double cos(Long a) {
        return Math.cos(a.doubleValue());
    }

    public static double cos(Double a) {
        return Math.cos(a);
    }

    public static double tan(Short a) {
        return Math.tan(a.doubleValue());
    }

    public static double tan(Integer a) {
        return Math.tan(a.doubleValue());
    }

    public static double tan(Float a) {
        return Math.tan(a.doubleValue());
    }

    public static double tan(Long a) {
        return Math.tan(a.doubleValue());
    }

    public static double tan(Double a) {
        return Math.tan(a);
    }

    public static double asin(Short a) {
        return Math.asin(a.doubleValue());
    }

    public static double asin(Integer a) {
        return Math.asin(a.doubleValue());
    }

    public static double asin(Float a) {
        return Math.asin(a.doubleValue());
    }

    public static double asin(Long a) {
        return Math.asin(a.doubleValue());
    }

    public static double asin(Double a) {
        return Math.asin(a);
    }

    public static double acos(Short a) {
        return Math.acos(a.doubleValue());
    }


    public static double acos(Integer a) {
        return Math.acos(a.doubleValue());
    }


    public static double acos(Float a) {
        return Math.acos(a.doubleValue());
    }

    public static double acos(Long a) {
        return Math.acos(a.doubleValue());
    }

    public static double acos(Double a) {
        return Math.acos(a);
    }

    public static double atan(Short a) {
        return Math.atan(a.doubleValue());
    }

    public static double atan(Integer a) {
        return Math.atan(a.doubleValue());
    }

    public static double atan(Float a) {
        return Math.atan(a.doubleValue());
    }

    public static double atan(Long a) {
        return Math.atan(a.doubleValue());
    }

    public static double atan(Double a) {
        return Math.atan(a);
    }

    public static double toRadians(Short angdeg) {
        return Math.toRadians(angdeg.doubleValue());
    }

    public static double toRadians(Integer angdeg) {
        return Math.toRadians(angdeg.doubleValue());
    }

    public static double toRadians(Float angdeg) {
        return Math.toRadians(angdeg.doubleValue());
    }

    public static double toRadians(Long angdeg) {
        return Math.toRadians(angdeg.doubleValue());
    }

    public static double toRadians(Double angdeg) {
        return Math.toRadians(angdeg);
    }

    public static double toDegrees(Short angrad) {
        return Math.toDegrees(angrad.doubleValue());
    }

    public static double toDegrees(Integer angrad) {
        return Math.toDegrees(angrad.doubleValue());
    }

    public static double toDegrees(Float angrad) {
        return Math.toDegrees(angrad.doubleValue());
    }

    public static double toDegrees(Long angrad) {
        return Math.toDegrees(angrad.doubleValue());
    }

    public static double toDegrees(Double angrad) {
        return Math.toDegrees(angrad);
    }

    public static double exp(Short a) {
        return Math.exp(a.doubleValue());
    }

    public static double exp(Integer a) {
        return Math.exp(a.doubleValue());
    }

    public static double exp(Float a) {
        return Math.exp(a.doubleValue());
    }

    public static double exp(Long a) {
        return Math.exp(a.doubleValue());
    }

    public static double exp(Double a) {
        return Math.exp(a);
    }

    public static double log(Short a) {
        return Math.log(a.doubleValue());
    }

    public static double log(Integer a) {
        return Math.log(a.doubleValue());
    }

    public static double log(Float a) {
        return Math.log(a.doubleValue());
    }

    public static double log(Long a) {
        return Math.log(a.doubleValue());
    }

    public static double log(Double a) {
        return Math.log(a);
    }

    public static double log10(Short a) {
        return Math.log10(a.doubleValue());
    }

    public static double log10(Integer a) {
        return Math.log10(a.doubleValue());
    }

    public static double log10(Float a) {
        return Math.log10(a.doubleValue());
    }

    public static double log10(Long a) {
        return Math.log10(a.doubleValue());
    }

    public static double log10(Double a) {
        return Math.log10(a);
    }

    public static double sqrt(Short a) {
        return Math.sqrt(a.doubleValue());
    }

    public static double sqrt(Integer a) {
        return Math.sqrt(a.doubleValue());
    }

    public static double sqrt(Float a) {
        return Math.sqrt(a.doubleValue());
    }

    public static double sqrt(Long a) {
        return Math.sqrt(a.doubleValue());
    }

    public static double sqrt(Double a) {
        return Math.sqrt(a);
    }

    public static double cbrt(Short a) {
        return Math.cbrt(a.doubleValue());
    }

    public static double cbrt(Integer a) {
        return Math.cbrt(a.doubleValue());
    }

    public static double cbrt(Float a) {
        return Math.cbrt(a.doubleValue());
    }

    public static double cbrt(Long a) {
        return Math.cbrt(a.doubleValue());
    }

    public static double cbrt(Double a) {
        return Math.cbrt(a);
    }

    public static double IEEEremainder(Short f1, Short f2) {
        return Math.IEEEremainder(f1.doubleValue(), f2.doubleValue());
    }

    public static double IEEEremainder(Integer f1, Integer f2) {
        return Math.IEEEremainder(f1.doubleValue(), f2.doubleValue());
    }

    public static double IEEEremainder(Float f1, Float f2) {
        return Math.IEEEremainder(f1.doubleValue(), f2.doubleValue());
    }

    public static double IEEEremainder(Long f1, Long f2) {
        return Math.IEEEremainder(f1.doubleValue(), f2.doubleValue());
    }

    public static double IEEEremainder(Double f1, Double f2) {
        return Math.IEEEremainder(f1, f2);
    }

    public static double ceil(Short a) {
        return Math.ceil(a.doubleValue());
    }

    public static double ceil(Integer a) {
        return Math.ceil(a.doubleValue());
    }

    public static double ceil(Float a) {
        return Math.ceil(a.doubleValue());
    }

    public static double ceil(Long a) {
        return Math.ceil(a.doubleValue());
    }

    public static double ceil(Double a) {
        return Math.ceil(a);
    }

    public static double floor(Short a) {
        return Math.floor(a.doubleValue());
    }

    public static double floor(Integer a) {
        return Math.floor(a.doubleValue());
    }

    public static double floor(Float a) {
        return Math.floor(a.doubleValue());
    }

    public static double floor(Long a) {
        return Math.floor(a.doubleValue());
    }

    public static double floor(Double a) {
        return Math.floor(a);
    }

    public static double rint(Short a) {
        return Math.rint(a.doubleValue());
    }

    public static double rint(Integer a) {
        return Math.rint(a.doubleValue());
    }

    public static double rint(Float a) {
        return Math.rint(a.doubleValue());
    }

    public static double rint(Long a) {
        return Math.rint(a.doubleValue());
    }

    public static double rint(Double a) {
        return Math.rint(a);
    }

    public static double atan2(Short y, Short x) {
        return Math.atan2(y.doubleValue(), x.doubleValue());
    }

    public static double atan2(Integer y, Integer x) {
        return Math.atan2(y.doubleValue(), x.doubleValue());
    }

    public static double atan2(Float y, Float x) {
        return Math.atan2(y.doubleValue(), x.doubleValue());
    }

    public static double atan2(Long y, Long x) {
        return Math.atan2(y.doubleValue(), x.doubleValue());
    }

    public static double atan2(Double y, Double x) {
        return Math.atan2(y, x);
    }

    public static double pow(Short a, Short b) {
        return Math.pow(a.doubleValue(), b.doubleValue());
    }

    public static double pow(Integer a, Integer b) {
        return Math.pow(a.doubleValue(), b.doubleValue());
    }

    public static double pow(Float a, Float b) {
        return Math.pow(a.doubleValue(), b.doubleValue());
    }

    public static double pow(Long a, Long b) {
        return Math.pow(a.doubleValue(), b.doubleValue());
    }

    public static double pow(Double a, Double b) {
        return Math.pow(a, b);
    }

    public static int round(Short a) {
        return Math.round(a.floatValue());
    }

    public static int round(Integer a) {
        return Math.round(a.floatValue());
    }

    public static int round(Float a) {
        return Math.round(a);
    }

    public static long round(Long a) {
        return Math.round(a.doubleValue());
    }

    public static long round(Double a) {
        return Math.round(a);
    }

    public static double random() {
        return ThreadLocalRandom.current().nextDouble();
    }

    public static double randomDouble() {
        return ThreadLocalRandom.current().nextDouble();
    }

    public static double randomFloat() {
        return ThreadLocalRandom.current().nextFloat();
    }

    public static double randomInt() {
        return ThreadLocalRandom.current().nextInt();
    }

    public static double randomInt(Integer i) {
        return ThreadLocalRandom.current().nextInt(i);
    }

    public static double randomLong() {
        return ThreadLocalRandom.current().nextLong();
    }

    public static double randomLong(Long l) {
        return ThreadLocalRandom.current().nextLong(l);
    }

    @SuppressForbidden(reason = "Math#abs is trappy")
    public static int abs(Integer a) {
        return Math.abs(a);
    }

    @SuppressForbidden(reason = "Math#abs is trappy")
    public static long abs(Long a) {
        return Math.abs(a);
    }

    @SuppressForbidden(reason = "Math#abs is trappy")
    public static float abs(Float a) {
        return Math.abs(a);
    }

    @SuppressForbidden(reason = "Math#abs is trappy")
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
