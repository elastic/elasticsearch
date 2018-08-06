/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.whitelist;

import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringNumericProcessor.BinaryStringNumericOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringStringProcessor.BinaryStringStringOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.ConcatFunctionProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.InsertFunctionProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.LocateFunctionProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.ReplaceFunctionProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.StringProcessor.StringOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.SubstringFunctionProcessor;

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

    public static String left(String s, int count) {
        return BinaryStringNumericOperation.LEFT.apply(s, count);
    }
    
    public static String right(String s, int count) {
        return BinaryStringNumericOperation.RIGHT.apply(s, count);
    }
    
    public static String concat(String s1, String s2) {
        return ConcatFunctionProcessor.doProcessInScripts(s1, s2).toString();
    }
    
    public static String repeat(String s, int count) {
        return BinaryStringNumericOperation.REPEAT.apply(s, count);
    }
    
    public static Integer position(String s1, String s2) {
        return (Integer) BinaryStringStringOperation.POSITION.apply(s1, s2);
    }
    
    public static String insert(String s, int start, int length, String r) {
        return InsertFunctionProcessor.doProcess(s, start, length, r).toString();
    }
    
    public static String substring(String s, int start, int length) {
        return SubstringFunctionProcessor.doProcess(s, start, length).toString();
    }
    
    public static String replace(String s1, String s2, String s3) {
        return ReplaceFunctionProcessor.doProcess(s1, s2, s3).toString();
    }
    
    public static Integer locate(String s1, String s2, Integer pos) {
        return (Integer) LocateFunctionProcessor.doProcess(s1, s2, pos);
    }
    
    public static Integer locate(String s1, String s2) {
        return locate(s1, s2, null);
    }
}
