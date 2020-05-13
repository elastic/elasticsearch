/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function;

import org.elasticsearch.xpack.eql.expression.function.scalar.string.Between;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.CIDRMatch;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.IndexOf;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.Match;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.StringContains;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.eql.expression.function.scalar.math.ToNumber;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.ToString;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.Wildcard;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Sub;

import java.util.Locale;

public class EqlFunctionRegistry extends FunctionRegistry {

    public EqlFunctionRegistry() {
        super(functions());
    }

    private static FunctionDefinition[][] functions() {
        return new FunctionDefinition[][] {
        // Scalar functions
        // String
            new FunctionDefinition[] {
                def(Between.class, Between::new, 2, "between"),
                def(CIDRMatch.class, CIDRMatch::new, "cidrmatch"),
                def(Concat.class, Concat::new, "concat"),
                def(EndsWith.class, EndsWith::new, "endswith"),
                def(IndexOf.class, IndexOf::new, "indexof"),
                def(Length.class, Length::new, "length"),
                def(Match.class, Match::new, "match", "matchlite"),
                def(StartsWith.class, StartsWith::new, "startswith"),
                def(ToString.class, ToString::new, "string"),
                def(StringContains.class, StringContains::new, "stringcontains"),
                def(Substring.class, Substring::new, "substring"),
                def(Wildcard.class, Wildcard::new, "wildcard"),
            },
        // Arithmetic
            new FunctionDefinition[] {
                def(Add.class, Add::new, "add"),
                def(Div.class, Div::new, "divide"),
                def(Mod.class, Mod::new, "modulo"),
                def(Mul.class, Mul::new, "multiply"),
                def(ToNumber.class, ToNumber::new, "number"),
                def(Sub.class, Sub::new, "subtract"),
            }
        };
    }

    @Override
    protected String normalize(String name) {
        return name.toLowerCase(Locale.ROOT);
    }
}
