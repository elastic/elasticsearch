/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * Every signature {@link TypedTests} does not cover has to be refused, and with the right argument named - which is
 * the half of the contract a positive test cannot check.
 * <p>
 * The message is spelled out here rather than taken from {@code typeErrorMessage} because that helper decides which
 * argument is at fault from the signature's types alone, and for this function the second argument's validity is a
 * property of its <em>value</em>: it has to name a type. So a {@code keyword} second argument is a well-typed
 * argument that is still wrong whenever its value is not a type name, which is every value the harness generates.
 * Encoding the real order of the checks is the point. The order is also not free to change: the field's own type is
 * never objected to, because a conflicted field arrives typed {@code UNSUPPORTED} and refusing that would refuse the
 * query the function exists for.
 * </p>
 */
public class TypedErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(TypedTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Typed(source, args.get(0), args.get(1));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        String source = sourceForSignature(signature);
        DataType type = signature.get(1);

        // The field's type is never the objection, whatever it is: a conflicted field arrives typed UNSUPPORTED and
        // this function has to resolve on it, so the type name is the only argument it can refuse.
        // NULL passes the string check - a null literal stands in for any type - and the check below it catches that.
        if (type != DataType.NULL && DataType.isString(type) == false) {
            return equalTo("second argument of [" + source + "] must be [string], found value [] type [" + type.typeName() + "]");
        }
        // A well-typed second argument whose value names no type, which is every string value generated here and the
        // null one as well.
        return equalTo("second argument of [" + source + "] must be a known type name");
    }
}
