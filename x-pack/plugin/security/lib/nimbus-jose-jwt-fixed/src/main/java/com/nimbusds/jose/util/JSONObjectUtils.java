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
 * This class wraps {@link InnerJSONObjectUtils}, which is copied directly from the source library, and delegates to
 * that class as quickly as possible. This layer is only here to provide a point at which we can insert
 * {@link java.security.AccessController#doPrivileged(PrivilegedAction)} calls as necessary. We don't do anything here
 * other than ensure gson has the proper security manager permissions.
 */
public class JSONObjectUtils {

    public static Map<String, Object> parse(final String s) throws ParseException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<Map<String, Object>>) () -> InnerJSONObjectUtils.parse(s));
        } catch (PrivilegedActionException e) {
            handleException(e);
            throw new RuntimeException("this should be unreachable");
        }
    }

    public static Map<String, Object> parse(final String s, final int sizeLimit) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<Map<String, Object>>) () -> InnerJSONObjectUtils.parse(s, sizeLimit)
            );
        } catch (PrivilegedActionException e) {
            handleException(e);
            throw new RuntimeException("this should be unreachable");
        }
    }

    @Deprecated
    public static Map<String, Object> parseJSONObject(final String s) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<Map<String, Object>>) () -> InnerJSONObjectUtils.parseJSONObject(s)
            );
        } catch (PrivilegedActionException e) {
            handleException(e);
            throw new RuntimeException("this should be unreachable");
        }
    }

    public static boolean getBoolean(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<Boolean>) () -> InnerJSONObjectUtils.getBoolean(o, key));
        } catch (PrivilegedActionException e) {
            handleException(e);
            throw new RuntimeException("this should be unreachable");
        }
    }

    public static int getInt(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<Integer>) () -> InnerJSONObjectUtils.getInt(o, key));
        } catch (PrivilegedActionException e) {
            handleException(e);
            throw new RuntimeException("this should be unreachable");
        }
    }

    public static long getLong(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<Long>) () -> InnerJSONObjectUtils.getLong(o, key));
        } catch (PrivilegedActionException e) {
            handleException(e);
            throw new RuntimeException("this should be unreachable");
        }
    }

    public static float getFloat(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<Float>) () -> InnerJSONObjectUtils.getFloat(o, key));
        } catch (PrivilegedActionException e) {
            handleException(e);
            throw new RuntimeException("this should be unreachable");
        }
    }

    public static double getDouble(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<Double>) () -> InnerJSONObjectUtils.getDouble(o, key));
        } catch (PrivilegedActionException e) {
            handleException(e);
            throw new RuntimeException("this should be unreachable");
        }
    }

    public static String getString(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<String>) () -> InnerJSONObjectUtils.getString(o, key));
        } catch (PrivilegedActionException e) {
            handleException(e);
            throw new RuntimeException("this should be unreachable");
        }
    }

    public static URI getURI(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<URI>) () -> InnerJSONObjectUtils.getURI(o, key));
        } catch (PrivilegedActionException e) {
            handleException(e);
            throw new RuntimeException("this should be unreachable");
        }
    }

    public static List<Object> getJSONArray(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<List<Object>>) () -> InnerJSONObjectUtils.getJSONArray(o, key));
        } catch (PrivilegedActionException e) {
            handleException(e);
            throw new RuntimeException("this should be unreachable");
        }
    }

    public static String[] getStringArray(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<String[]>) () -> InnerJSONObjectUtils.getStringArray(o, key));
        } catch (PrivilegedActionException e) {
            handleException(e);
            throw new RuntimeException("this should be unreachable");
        }
    }

    public static Map<String, Object>[] getJSONObjectArray(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<Map<String, Object>[]>) () -> InnerJSONObjectUtils.getJSONObjectArray(o, key)
            );
        } catch (PrivilegedActionException e) {
            handleException(e);
            throw new RuntimeException("this should be unreachable");
        }
    }

    public static List<String> getStringList(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<List<String>>) () -> InnerJSONObjectUtils.getStringList(o, key)
            );
        } catch (PrivilegedActionException e) {
            handleException(e);
            throw new RuntimeException("this should be unreachable");
        }
    }

    public static Map<String, Object> getJSONObject(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<Map<String, Object>>) () -> InnerJSONObjectUtils.getJSONObject(o, key)
            );
        } catch (PrivilegedActionException e) {
            handleException(e);
            throw new RuntimeException("this should be unreachable");
        }
    }

    public static Base64URL getBase64URL(final Map<String, Object> o, final String key) throws ParseException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<Base64URL>) () -> InnerJSONObjectUtils.getBase64URL(o, key));
        } catch (PrivilegedActionException e) {
            handleException(e);
            throw new RuntimeException("this should be unreachable");
        }
    }

    public static String toJSONString(final Map<String, ?> o) {
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> InnerJSONObjectUtils.toJSONString(o));
    }

    public static Map<String, Object> newJSONObject() {
        return AccessController.doPrivileged((PrivilegedAction<Map<String, Object>>) InnerJSONObjectUtils::newJSONObject);
    }

    private static void handleException(final PrivilegedActionException e) throws ParseException {
        if (e.getException() instanceof ParseException pe) {
            throw pe;
        } else if (e.getException() instanceof RuntimeException re) {
            throw re;
        }
        throw new RuntimeException(e);
    }

    private JSONObjectUtils() {}
}
