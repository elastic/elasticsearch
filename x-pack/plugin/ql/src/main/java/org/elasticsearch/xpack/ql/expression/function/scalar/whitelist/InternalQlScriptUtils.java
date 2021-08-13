/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.function.scalar.whitelist;

import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.xpack.ql.expression.function.scalar.string.StartsWithFunctionProcessor;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogicProcessor.BinaryLogicOperation;
import org.elasticsearch.xpack.ql.expression.predicate.logical.NotProcessor;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.CheckNullProcessor.CheckNullOperation;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.UnaryArithmeticProcessor.UnaryArithmeticOperation;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.InProcessor;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RegexProcessor.RegexOperation;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.ql.type.DataTypeConverter.convert;
import static org.elasticsearch.xpack.ql.type.DataTypes.fromTypeName;

public class InternalQlScriptUtils {

    //
    // Utilities
    //

    // safe missing mapping/value extractor
    public static <T> Object docValue(Map<String, ScriptDocValues<T>> doc, String fieldName) {
        if (doc.containsKey(fieldName)) {
            ScriptDocValues<T> docValues = doc.get(fieldName);
            if (docValues.isEmpty() == false) {
                return docValues.get(0);
            }
        }
        return null;
    }

    public static boolean nullSafeFilter(Boolean filter) {
        return filter == null ? false : filter.booleanValue();
    }

    public static double nullSafeSortNumeric(Number sort) {
        return sort == null ? 0.0d : sort.doubleValue();
    }

    public static String nullSafeSortString(Object sort) {
        return sort == null ? StringUtils.EMPTY : sort.toString();
    }

    public static Number nullSafeCastNumeric(Number number, String typeName) {
        return number == null || Double.isNaN(number.doubleValue()) ? null : (Number) convert(number, fromTypeName(typeName));
    }


    //
    // Operators
    //

    //
    // Logical
    //
    public static Boolean eq(Object left, Object right) {
        return BinaryComparisonOperation.EQ.apply(left, right);
    }

    public static Boolean nulleq(Object left, Object right) {
        return BinaryComparisonOperation.NULLEQ.apply(left, right);
    }

    public static Boolean neq(Object left, Object right) {
        return BinaryComparisonOperation.NEQ.apply(left, right);
    }

    public static Boolean lt(Object left, Object right) {
        return BinaryComparisonOperation.LT.apply(left, right);
    }

    public static Boolean lte(Object left, Object right) {
        return BinaryComparisonOperation.LTE.apply(left, right);
    }

    public static Boolean gt(Object left, Object right) {
        return BinaryComparisonOperation.GT.apply(left, right);
    }

    public static Boolean gte(Object left, Object right) {
        return BinaryComparisonOperation.GTE.apply(left, right);
    }

    public static Boolean in(Object value, List<Object> values) {
        return InProcessor.apply(value, values);
    }

    public static Boolean and(Boolean left, Boolean right) {
        return BinaryLogicOperation.AND.apply(left, right);
    }

    public static Boolean or(Boolean left, Boolean right) {
        return BinaryLogicOperation.OR.apply(left, right);
    }

    public static Boolean not(Boolean expression) {
        return NotProcessor.apply(expression);
    }

    public static Boolean isNull(Object expression) {
        return CheckNullOperation.IS_NULL.apply(expression);
    }

    public static Boolean isNotNull(Object expression) {
        return CheckNullOperation.IS_NOT_NULL.apply(expression);
    }

    //
    // Regex
    //
    public static Boolean regex(String value, String pattern) {
        return regex(value, pattern, Boolean.FALSE);
    }

    public static Boolean regex(String value, String pattern, Boolean caseInsensitive) {
        // TODO: this needs to be improved to avoid creating the pattern on every call
        return RegexOperation.match(value, pattern, caseInsensitive);
    }

    //
    // Math
    //
    public static Number add(Number left, Number right) {
        return (Number) DefaultBinaryArithmeticOperation.ADD.apply(left, right);
    }

    public static Number div(Number left, Number right) {
        return (Number) DefaultBinaryArithmeticOperation.DIV.apply(left, right);
    }

    public static Number mod(Number left, Number right) {
        return (Number) DefaultBinaryArithmeticOperation.MOD.apply(left, right);
    }

    public static Number mul(Number left, Number right) {
        return (Number) DefaultBinaryArithmeticOperation.MUL.apply(left, right);
    }

    public static Number neg(Number value) {
        return UnaryArithmeticOperation.NEGATE.apply(value);
    }

    public static Number sub(Number left, Number right) {
        return (Number) DefaultBinaryArithmeticOperation.SUB.apply(left, right);
    }

    //
    // String
    //
    public static Boolean startsWith(String s, String pattern, Boolean caseInsensitive) {
        return (Boolean) StartsWithFunctionProcessor.doProcess(s, pattern, caseInsensitive);
    }
}
