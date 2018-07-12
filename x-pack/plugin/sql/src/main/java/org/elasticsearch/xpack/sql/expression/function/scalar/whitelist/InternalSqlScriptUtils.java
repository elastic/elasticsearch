/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.whitelist;

import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.StringProcessor.StringOperation;

/**
 * Whitelisted class for SQL scripts.
 * Acts as a registry of the various static methods used <b>internally</b> by the scalar functions
 * (to simplify the whitelist definition).
 */
public final class InternalSqlScriptUtils {

    private InternalSqlScriptUtils() {}

    public static Integer dateTimeChrono(long millis, String tzId, String chronoName) {
        return DateTimeFunction.dateTimeChrono(millis, tzId, chronoName);
    }
    
    public static Integer ascii(String s) {
        return (Integer) StringOperation.ASCII.apply(s);
    }
    
    public static Integer bitLength(String s) {
        return (Integer) StringOperation.BIT_LENGTH.apply(s);
    }
    
    public static String character(Number n) {
        return (String) StringOperation.CHAR.apply(n);
    }
    
    public static Integer charLength(String s) {
        return (Integer) StringOperation.CHAR_LENGTH.apply(s);
    }
    
    public static String lcase(String s) {
        return (String) StringOperation.LCASE.apply(s);
    }
    
    public static String ucase(String s) {
        return (String) StringOperation.UCASE.apply(s);
    }
    
    public static Integer length(String s) {
        return (Integer) StringOperation.LENGTH.apply(s);
    }
    
    public static String rtrim(String s) {
        return (String) StringOperation.RTRIM.apply(s);
    }
    
    public static String ltrim(String s) {
        return (String) StringOperation.LTRIM.apply(s);
    }
    
    public static String space(Number n) {
        return (String) StringOperation.SPACE.apply(n);
    }
}
