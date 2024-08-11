/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package com.nimbusds.jose.util;

import com.nimbusds.jose.shaded.gson.Gson;
import com.nimbusds.jose.shaded.gson.GsonBuilder;
import com.nimbusds.jose.shaded.gson.ToNumberPolicy;
import com.nimbusds.jose.shaded.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copied from nimbus-jose-jwt version 9.37.3.
 *
 * Original code Copyright 2012-2016, Connect2id Ltd. Licensed under the Apache License, Version 2.0
 *
 * The only modifications in this file are:
 * 1) {@link AccessController#doPrivileged(PrivilegedAction)} calls to make gson work with the security manager
 * 2) Formatting/Warning suppression as necessary to work with our infrastructure
 * 3) This comment and the license comment
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class JSONObjectUtils {

    /**
     * The GSon instance for serialisation and parsing.
     */
    private static final Gson GSON = new GsonBuilder().serializeNulls()
        .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
        .disableHtmlEscaping()
        .create();

    /**
     * Parses a JSON object.
     *
     * <p>Specific JSON to Java entity mapping (as per JSON Smart):
     *
     * <ul>
     *     <li>JSON true|false map to {@code java.lang.Boolean}.
     *     <li>JSON numbers map to {@code java.lang.Number}.
     *         <ul>
     *             <li>JSON integer numbers map to {@code long}.
     *             <li>JSON fraction numbers map to {@code double}.
     *         </ul>
     *     <li>JSON strings map to {@code java.lang.String}.
     *     <li>JSON arrays map to {@code java.util.List<Object>}.
     *     <li>JSON objects map to {@code java.util.Map<String,Object>}.
     * </ul>
     *
     * @param s The JSON object string to parse. Must not be {@code null}.
     *
     * @return The JSON object.
     *
     * @throws ParseException If the string cannot be parsed to a valid JSON
     *                        object.
     */
    public static Map<String, Object> parse(final String s) throws ParseException {

        return parse(s, -1);
    }

    /**
     * Parses a JSON object with the option to limit the input string size.
     *
     * <p>Specific JSON to Java entity mapping (as per JSON Smart):
     *
     * <ul>
     *     <li>JSON true|false map to {@code java.lang.Boolean}.
     *     <li>JSON numbers map to {@code java.lang.Number}.
     *         <ul>
     *             <li>JSON integer numbers map to {@code long}.
     *             <li>JSON fraction numbers map to {@code double}.
     *         </ul>
     *     <li>JSON strings map to {@code java.lang.String}.
     *     <li>JSON arrays map to {@code java.util.List<Object>}.
     *     <li>JSON objects map to {@code java.util.Map<String,Object>}.
     * </ul>
     *
     * @param s         The JSON object string to parse. Must not be
     *                  {@code null}.
     * @param sizeLimit The max allowed size of the string to parse. A
     *                  negative integer means no limit.
     *
     * @return The JSON object.
     *
     * @throws ParseException If the string cannot be parsed to a valid JSON
     *                        object.
     */
    public static Map<String, Object> parse(final String s, final int sizeLimit) throws ParseException {

        if (s.trim().isEmpty()) {
            throw new ParseException("Invalid JSON object", 0);
        }

        if (sizeLimit >= 0 && s.length() > sizeLimit) {
            throw new ParseException("The parsed string is longer than the max accepted size of " + sizeLimit + " characters", 0);
        }

        Type mapType = TypeToken.getParameterized(Map.class, String.class, Object.class).getType();

        try {
            return AccessController.doPrivileged((PrivilegedAction<Map<String, Object>>) () -> GSON.fromJson(s, mapType));
        } catch (Exception e) {
            throw new ParseException("Invalid JSON: " + e.getMessage(), 0);
        } catch (StackOverflowError e) {
            throw new ParseException("Excessive JSON object and / or array nesting", 0);
        }
    }

    /**
     * Use {@link #parse(String)} instead.
     *
     * @param s The JSON object string to parse. Must not be {@code null}.
     *
     * @return The JSON object.
     *
     * @throws ParseException If the string cannot be parsed to a valid JSON
     *                        object.
     */
    @Deprecated
    public static Map<String, Object> parseJSONObject(final String s) throws ParseException {

        return parse(s);
    }

    /**
     * Gets a generic member of a JSON object.
     *
     * @param o     The JSON object. Must not be {@code null}.
     * @param name  The JSON object member name. Must not be {@code null}.
     * @param clazz The expected class of the JSON object member value.
     *              Must not be {@code null}.
     *
     * @return The JSON object member value, may be {@code null}.
     *
     * @throws ParseException If the value is not of the expected type.
     */
    private static <T> T getGeneric(final Map<String, Object> o, final String name, final Class<T> clazz) throws ParseException {

        if (o.get(name) == null) {
            return null;
        }

        Object value = o.get(name);

        if (clazz.isAssignableFrom(value.getClass()) == false) {
            throw new ParseException("Unexpected type of JSON object member " + name + "", 0);
        }

        @SuppressWarnings("unchecked")
        T castValue = (T) value;
        return castValue;
    }

    /**
     * Gets a boolean member of a JSON object.
     *
     * @param o    The JSON object. Must not be {@code null}.
     * @param name The JSON object member name. Must not be {@code null}.
     *
     * @return The JSON object member value.
     *
     * @throws ParseException If the member is missing, the value is
     *                        {@code null} or not of the expected type.
     */
    public static boolean getBoolean(final Map<String, Object> o, final String name) throws ParseException {

        Boolean value = getGeneric(o, name, Boolean.class);

        if (value == null) {
            throw new ParseException("JSON object member " + name + " is missing or null", 0);
        }

        return value;
    }

    /**
     * Gets a number member of a JSON object as {@code int}.
     *
     * @param o    The JSON object. Must not be {@code null}.
     * @param name The JSON object member name. Must not be {@code null}.
     *
     * @return The JSON object member value.
     *
     * @throws ParseException If the member is missing, the value is
     *                        {@code null} or not of the expected type.
     */
    public static int getInt(final Map<String, Object> o, final String name) throws ParseException {

        Number value = getGeneric(o, name, Number.class);

        if (value == null) {
            throw new ParseException("JSON object member " + name + " is missing or null", 0);
        }

        return value.intValue();
    }

    /**
     * Gets a number member of a JSON object as {@code long}.
     *
     * @param o    The JSON object. Must not be {@code null}.
     * @param name The JSON object member name. Must not be {@code null}.
     *
     * @return The JSON object member value.
     *
     * @throws ParseException If the member is missing, the value is
     *                        {@code null} or not of the expected type.
     */
    public static long getLong(final Map<String, Object> o, final String name) throws ParseException {

        Number value = getGeneric(o, name, Number.class);

        if (value == null) {
            throw new ParseException("JSON object member " + name + " is missing or null", 0);
        }

        return value.longValue();
    }

    /**
     * Gets a number member of a JSON object {@code float}.
     *
     * @param o    The JSON object. Must not be {@code null}.
     * @param name The JSON object member name. Must not be {@code null}.
     *
     * @return The JSON object member value, may be {@code null}.
     *
     * @throws ParseException If the member is missing, the value is
     *                        {@code null} or not of the expected type.
     */
    public static float getFloat(final Map<String, Object> o, final String name) throws ParseException {

        Number value = getGeneric(o, name, Number.class);

        if (value == null) {
            throw new ParseException("JSON object member " + name + " is missing or null", 0);
        }

        return value.floatValue();
    }

    /**
     * Gets a number member of a JSON object as {@code double}.
     *
     * @param o    The JSON object. Must not be {@code null}.
     * @param name The JSON object member name. Must not be {@code null}.
     *
     * @return The JSON object member value, may be {@code null}.
     *
     * @throws ParseException If the member is missing, the value is
     *                        {@code null} or not of the expected type.
     */
    public static double getDouble(final Map<String, Object> o, final String name) throws ParseException {

        Number value = getGeneric(o, name, Number.class);

        if (value == null) {
            throw new ParseException("JSON object member " + name + " is missing or null", 0);
        }

        return value.doubleValue();
    }

    /**
     * Gets a string member of a JSON object.
     *
     * @param o    The JSON object. Must not be {@code null}.
     * @param name The JSON object member name. Must not be {@code null}.
     *
     * @return The JSON object member value, may be {@code null}.
     *
     * @throws ParseException If the value is not of the expected type.
     */
    public static String getString(final Map<String, Object> o, final String name) throws ParseException {

        return getGeneric(o, name, String.class);
    }

    /**
     * Gets a string member of a JSON object as {@code java.net.URI}.
     *
     * @param o    The JSON object. Must not be {@code null}.
     * @param name The JSON object member name. Must not be {@code null}.
     *
     * @return The JSON object member value, may be {@code null}.
     *
     * @throws ParseException If the value is not of the expected type.
     */
    public static URI getURI(final Map<String, Object> o, final String name) throws ParseException {

        String value = getString(o, name);

        if (value == null) {
            return null;
        }

        try {
            return new URI(value);

        } catch (URISyntaxException e) {

            throw new ParseException(e.getMessage(), 0);
        }
    }

    /**
     * Gets a JSON array member of a JSON object.
     *
     * @param o    The JSON object. Must not be {@code null}.
     * @param name The JSON object member name. Must not be {@code null}.
     *
     * @return The JSON object member value, may be {@code null}.
     *
     * @throws ParseException If the value is not of the expected type.
     */
    public static List<Object> getJSONArray(final Map<String, Object> o, final String name) throws ParseException {

        @SuppressWarnings("unchecked")
        List<Object> jsonArray = getGeneric(o, name, List.class);
        return jsonArray;
    }

    /**
     * Gets a string array member of a JSON object.
     *
     * @param o    The JSON object. Must not be {@code null}.
     * @param name The JSON object member name. Must not be {@code null}.
     *
     * @return The JSON object member value, may be {@code null}.
     *
     * @throws ParseException If the value is not of the expected type.
     */
    public static String[] getStringArray(final Map<String, Object> o, final String name) throws ParseException {

        List<Object> jsonArray = getJSONArray(o, name);

        if (jsonArray == null) {
            return null;
        }

        try {
            return jsonArray.toArray(new String[0]);
        } catch (ArrayStoreException e) {
            throw new ParseException("JSON object member " + name + " is not an array of strings", 0);
        }
    }

    /**
     * Gets a JSON objects array member of a JSON object.
     *
     * @param o    The JSON object. Must not be {@code null}.
     * @param name The JSON object member name. Must not be {@code null}.
     *
     * @return The JSON object member value, may be {@code null}.
     *
     * @throws ParseException If the value is not of the expected type.
     */
    public static Map<String, Object>[] getJSONObjectArray(final Map<String, Object> o, final String name) throws ParseException {

        List<Object> jsonArray = getJSONArray(o, name);

        if (jsonArray == null) {
            return null;
        }

        if (jsonArray.isEmpty()) {
            return new HashMap[0];
        }

        for (Object member : jsonArray) {
            if (member == null) {
                continue;
            }
            if (member instanceof Map) {
                try {
                    return jsonArray.toArray(new Map[0]);
                } catch (ArrayStoreException e) {
                    break; // throw parse exception below
                }
            }
        }
        throw new ParseException("JSON object member " + name + " is not an array of JSON objects", 0);
    }

    /**
     * Gets a string list member of a JSON object
     *
     * @param o    The JSON object. Must not be {@code null}.
     * @param name The JSON object member name. Must not be {@code null}.
     *
     * @return The JSON object member value, may be {@code null}.
     *
     * @throws ParseException If the value is not of the expected type.
     */
    public static List<String> getStringList(final Map<String, Object> o, final String name) throws ParseException {

        String[] array = getStringArray(o, name);

        if (array == null) {
            return null;
        }

        return Arrays.asList(array);
    }

    /**
     * Gets a JSON object member of a JSON object.
     *
     * @param o    The JSON object. Must not be {@code null}.
     * @param name The JSON object member name. Must not be {@code null}.
     *
     * @return The JSON object member value, may be {@code null}.
     *
     * @throws ParseException If the value is not of the expected type.
     */
    public static Map<String, Object> getJSONObject(final Map<String, Object> o, final String name) throws ParseException {

        Map<?, ?> jsonObject = getGeneric(o, name, Map.class);

        if (jsonObject == null) {
            return null;
        }

        // Verify keys are String
        for (Object oKey : jsonObject.keySet()) {
            if ((oKey instanceof String) == false) {
                throw new ParseException("JSON object member " + name + " not a JSON object", 0);
            }
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> castJSONObject = (Map<String, Object>) jsonObject;
        return castJSONObject;
    }

    /**
     * Gets a string member of a JSON object as {@link Base64URL}.
     *
     * @param o    The JSON object. Must not be {@code null}.
     * @param name The JSON object member name. Must not be {@code null}.
     *
     * @return The JSON object member value, may be {@code null}.
     *
     * @throws ParseException If the value is not of the expected type.
     */
    public static Base64URL getBase64URL(final Map<String, Object> o, final String name) throws ParseException {

        String value = getString(o, name);

        if (value == null) {
            return null;
        }

        return new Base64URL(value);
    }

    /**
     * Gets a number member of a JSON object as a {@link Date} expressed in
     * seconds since the Unix epoch.
     *
     * @param o    The JSON object. Must not be {@code null}.
     * @param name The JSON object member name. Must not be {@code null}.
     *
     * @return The JSON object member value, may be {@code null}.
     *
     * @throws ParseException If the value is not of the expected type.
     */
    public static Date getEpochSecondAsDate(final Map<String, Object> o, final String name) throws ParseException {

        Number value = getGeneric(o, name, Number.class);

        if (value == null) {
            return null;
        }

        return DateUtils.fromSecondsSinceEpoch(value.longValue());
    }

    /**
     * Serialises the specified map to a JSON object using the entity
     * mapping specified in {@link #parse(String)}.
     *
     * @param o The map. Must not be {@code null}.
     *
     * @return The JSON object as string.
     */
    public static String toJSONString(final Map<String, ?> o) {
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> GSON.toJson(o));
    }

    /**
     * Creates a new JSON object (unordered).
     *
     * @return The new empty JSON object.
     */
    public static Map<String, Object> newJSONObject() {
        return new HashMap<>();
    }

    /**
     * Prevents public instantiation.
     */
    private JSONObjectUtils() {}
}
