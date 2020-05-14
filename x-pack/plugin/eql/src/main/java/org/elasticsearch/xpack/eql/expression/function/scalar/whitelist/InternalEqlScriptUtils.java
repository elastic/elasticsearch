/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.whitelist;

import org.elasticsearch.xpack.eql.expression.function.scalar.string.BetweenFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.CIDRMatchFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.ConcatFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.EndsWithFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.IndexOfFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.LengthFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.StringContainsFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.SubstringFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.math.ToNumberFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.ToStringFunctionProcessor;
import org.elasticsearch.xpack.ql.expression.function.scalar.whitelist.InternalQlScriptUtils;

import java.util.List;

/*
 * Whitelisted class for EQL scripts.
 * Acts as a registry of the various static methods used <b>internally</b> by the scalar functions
 * (to simplify the whitelist definition).
 */
public class InternalEqlScriptUtils extends InternalQlScriptUtils {

    InternalEqlScriptUtils() {}

    public static String between(String s, String left, String right, Boolean greedy, Boolean caseSensitive) {
        return (String) BetweenFunctionProcessor.doProcess(s, left, right, greedy, caseSensitive);
    }

    public static Boolean cidrMatch(String s, List<Object>  addresses) {
        return (Boolean) CIDRMatchFunctionProcessor.doProcess(s, addresses);
    }

    public static String concat(List<Object> values) {
        return (String) ConcatFunctionProcessor.doProcess(values);
    }

    public static Boolean endsWith(String s, String pattern) {
        return (Boolean) EndsWithFunctionProcessor.doProcess(s, pattern);
    }

    public static Integer indexOf(String s, String substring, Number start) {
        return (Integer) IndexOfFunctionProcessor.doProcess(s, substring, start);
    }

    public static Integer length(String s) {
        return (Integer) LengthFunctionProcessor.doProcess(s);
    }

    public static String string(Object s) {
        return (String) ToStringFunctionProcessor.doProcess(s);
    }

    public static Boolean stringContains(String string, String substring) {
        return (Boolean) StringContainsFunctionProcessor.doProcess(string, substring);
    }

    public static Number number(String source, Number base) {
        return (Number) ToNumberFunctionProcessor.doProcess(source, base);
    }

    public static String substring(String s, Number start, Number end) {
        return (String) SubstringFunctionProcessor.doProcess(s, start, end);
    }
}
