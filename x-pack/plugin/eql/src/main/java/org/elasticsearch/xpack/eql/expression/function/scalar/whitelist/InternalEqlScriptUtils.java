/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.whitelist;

import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.xpack.eql.expression.function.scalar.math.ToNumberFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.BetweenFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.CIDRMatchFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.ConcatFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.EndsWithFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.IndexOfFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.LengthFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.StringContainsFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.SubstringFunctionProcessor;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.ToStringFunctionProcessor;
import org.elasticsearch.xpack.ql.expression.function.scalar.whitelist.InternalQlScriptUtils;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.eql.expression.predicate.operator.comparison.InsensitiveBinaryComparisonProcessor.InsensitiveBinaryComparisonOperation;

/*
 * Whitelisted class for EQL scripts.
 * Acts as a registry of the various static methods used <b>internally</b> by the scalar functions
 * (to simplify the whitelist definition).
 */
public class InternalEqlScriptUtils extends InternalQlScriptUtils {

    InternalEqlScriptUtils() {}

    public static <T> Boolean multiValueDocValues(Map<String, ScriptDocValues<T>> doc, String fieldName, Predicate<T> script) {
        ScriptDocValues<T> docValues = doc.get(fieldName);
        if (docValues != null && docValues.isEmpty() == false) {
            for (T value : docValues) {
                if (script.test(value)) {
                    return true;
                }
            }
            return false;
        }
        // missing value means "null"
        return script.test(null);
    }

    public static Boolean seq(Object left, Object right) {
        return InsensitiveBinaryComparisonOperation.SEQ.apply(left, right);
    }

    public static Boolean sneq(Object left, Object right) {
        return InsensitiveBinaryComparisonOperation.SNEQ.apply(left, right);
    }

    public static String between(String s, String left, String right, Boolean greedy, Boolean caseInsensitive) {
        return (String) BetweenFunctionProcessor.doProcess(s, left, right, greedy, caseInsensitive);
    }

    public static Boolean cidrMatch(String s, List<Object> addresses) {
        return (Boolean) CIDRMatchFunctionProcessor.doProcess(s, addresses);
    }

    public static String concat(List<Object> values) {
        return (String) ConcatFunctionProcessor.doProcess(values);
    }

    public static Boolean endsWith(String s, String pattern, Boolean caseInsensitive) {
        return (Boolean) EndsWithFunctionProcessor.doProcess(s, pattern, caseInsensitive);
    }

    public static Integer indexOf(String s, String substring, Number start, Boolean caseInsensitive) {
        return (Integer) IndexOfFunctionProcessor.doProcess(s, substring, start, caseInsensitive);
    }

    public static Integer length(String s) {
        return (Integer) LengthFunctionProcessor.doProcess(s);
    }

    public static String string(Object s) {
        return (String) ToStringFunctionProcessor.doProcess(s);
    }

    public static Boolean stringContains(String string, String substring, Boolean caseInsensitive) {
        return (Boolean) StringContainsFunctionProcessor.doProcess(string, substring, caseInsensitive);
    }

    public static Number number(String source, Number base) {
        return (Number) ToNumberFunctionProcessor.doProcess(source, base);
    }

    public static String substring(String s, Number start, Number end) {
        return (String) SubstringFunctionProcessor.doProcess(s, start, end);
    }
}
