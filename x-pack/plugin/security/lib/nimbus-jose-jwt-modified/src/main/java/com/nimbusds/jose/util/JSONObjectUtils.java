/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package com.nimbusds.jose.util;

import java.net.URI;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

/**
 * This class wraps {@link org.elasticsearch.nimbus.jose.util.JSONObjectUtils}, which is copied directly from the source
 * library, and delegates to that class as quickly as possible. This layer is only here to provide a point at which we
 * can insert {@link java.security.AccessController#doPrivileged(PrivilegedAction)} calls as necessary. We don't do
 * anything here other than ensure gson has the proper security manager permissions.
 */
public class JSONObjectUtils {

    public static Map<String, Object> parse(final String s) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<Map<String, Object>>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils.parse(s)
            );
        } catch (PrivilegedActionException e) {
            throw (ParseException) e.getException();
        }
    }

    public static Map<String, Object> parse(final String s, final int sizeLimit) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<Map<String, Object>>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils.parse(
                    s,
                    sizeLimit
                )
            );
        } catch (PrivilegedActionException e) {
            throw (ParseException) e.getException();
        }
    }

    @Deprecated
    public static Map<String, Object> parseJSONObject(final String s) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<Map<String, Object>>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils.parseJSONObject(s)
            );
        } catch (PrivilegedActionException e) {
            throw (ParseException) e.getException();
        }
    }

    public static boolean getBoolean(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<Boolean>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils.getBoolean(o, key)
            );
        } catch (PrivilegedActionException e) {
            throw (ParseException) e.getException();
        }
    }

    public static int getInt(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<Integer>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils.getInt(o, key)
            );
        } catch (PrivilegedActionException e) {
            throw (ParseException) e.getException();
        }
    }

    public static long getLong(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<Long>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils.getLong(o, key)
            );
        } catch (PrivilegedActionException e) {
            throw (ParseException) e.getException();
        }
    }

    public static float getFloat(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<Float>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils.getFloat(o, key)
            );
        } catch (PrivilegedActionException e) {
            throw (ParseException) e.getException();
        }
    }

    public static double getDouble(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<Double>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils.getDouble(o, key)
            );
        } catch (PrivilegedActionException e) {
            throw (ParseException) e.getException();
        }
    }

    public static String getString(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<String>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils.getString(o, key)
            );
        } catch (PrivilegedActionException e) {
            throw (ParseException) e.getException();
        }
    }

    public static URI getURI(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<URI>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils.getURI(o, key)
            );
        } catch (PrivilegedActionException e) {
            throw (ParseException) e.getException();
        }
    }

    public static List<Object> getJSONArray(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<List<Object>>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils.getJSONArray(o, key)
            );
        } catch (PrivilegedActionException e) {
            throw (ParseException) e.getException();
        }
    }

    public static String[] getStringArray(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<String[]>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils.getStringArray(o, key)
            );
        } catch (PrivilegedActionException e) {
            throw (ParseException) e.getException();
        }
    }

    public static Map<String, Object>[] getJSONObjectArray(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<Map<String, Object>[]>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils
                    .getJSONObjectArray(o, key)
            );
        } catch (PrivilegedActionException e) {
            throw (ParseException) e.getException();
        }
    }

    public static List<String> getStringList(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<List<String>>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils.getStringList(o, key)
            );
        } catch (PrivilegedActionException e) {
            throw (ParseException) e.getException();
        }
    }

    public static Map<String, Object> getJSONObject(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<Map<String, Object>>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils.getJSONObject(
                    o,
                    key
                )
            );
        } catch (PrivilegedActionException e) {
            throw (ParseException) e.getException();
        }
    }

    public static Base64URL getBase64URL(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<Base64URL>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils.getBase64URL(o, key)
            );
        } catch (PrivilegedActionException e) {
            throw (ParseException) e.getException();
        }
    }

    public static String toJSONString(final Map<String, ?> o) {
        return AccessController.doPrivileged(
            (PrivilegedAction<String>) () -> org.elasticsearch.nimbus.jose.util.JSONObjectUtils.toJSONString(o)
        );
    }

    public static Map<String, Object> newJSONObject() {
        return AccessController.doPrivileged(
            (PrivilegedAction<Map<String, Object>>) org.elasticsearch.nimbus.jose.util.JSONObjectUtils::newJSONObject
        );
    }

    private JSONObjectUtils() {}
}
