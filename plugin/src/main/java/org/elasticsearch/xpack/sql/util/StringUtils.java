/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.elasticsearch.common.Strings;

import static java.util.stream.Collectors.joining;

public abstract class StringUtils {

    public static final String EMPTY = "";
    public static final String NEW_LINE = "\n";

    private static final int TO_STRING_LIMIT = 52;

    public static String limitedToString(Object o) {
        String s = String.valueOf(o);
        return s.length() > TO_STRING_LIMIT ? s.substring(0, TO_STRING_LIMIT).concat("...") : s;
    }

    public static <E> String limitedToString(Collection<E> c) {
        Iterator<E> it = c.iterator();
        if (!it.hasNext())
            return "[]";

        // ..]
        StringBuilder sb = new StringBuilder(TO_STRING_LIMIT + 4);
        sb.append('[');
        for (;;) {
            E e = it.next();
            String next = e == c ? "(this Collection)" : String.valueOf(e);
            if (next.length() + sb.length() > TO_STRING_LIMIT) {
                sb.append(next.substring(0, Math.max(0, TO_STRING_LIMIT - sb.length())));
                sb.append('.').append('.').append(']');
                return sb.toString();
            }
            else {
                sb.append(next);
            }
            if (!it.hasNext())
                return sb.append(']').toString();
            sb.append(',').append(' ');
        }
    }

    public static String concatWithDot(List<String> strings) {
        if (strings == null || strings.isEmpty()) {
            return EMPTY;
        }
        return strings.stream().collect(joining("."));
    }

    //CamelCase to camel_case
    public static String camelCaseToUnderscore(String string) {
        if (!Strings.hasText(string)) {
            return EMPTY;
        }
        StringBuilder sb = new StringBuilder();
        String s = string.trim();

        boolean previousCharWasUp = false;
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (Character.isUpperCase(ch)) {
                if (i > 0 && !previousCharWasUp) {
                    sb.append("_");
                }
                previousCharWasUp = true;
            }
            else {
                previousCharWasUp = (ch == '_');
            }
            sb.append(ch);
        }
        return sb.toString().toUpperCase(Locale.ROOT);
    }
}