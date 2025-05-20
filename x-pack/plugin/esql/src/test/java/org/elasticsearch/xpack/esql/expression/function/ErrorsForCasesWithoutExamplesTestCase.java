/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCountErrorTests;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Extend me to test that all cases not mentioned in a subclass of
 * {@link AbstractFunctionTestCase} produce type errors.
 */
public abstract class ErrorsForCasesWithoutExamplesTestCase extends ESTestCase {
    protected abstract List<TestCaseSupplier> cases();

    /**
     * Build the expression being tested, for the given source and list of arguments.  Test classes need to implement this
     * to have something to test.
     *
     * @param source the source
     * @param args   arg list from the test case, should match the length expected
     * @return an expression for evaluating the function being tested on the given arguments
     */
    protected abstract Expression build(Source source, List<Expression> args);

    /**
     * A matcher for the invalid type error message.
     * <p>
     *     If you are implementing this for a function that should process all types
     *     then have a look how {@link MvCountErrorTests} does it. It's nice to throw
     *     an error explaining this. But while someone is implementing a new type
     *     they will want to turn that off temporarily. And we say that in the note too.
     * </p>
     */
    protected abstract Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature);

    protected final List<TestCaseSupplier> paramsToSuppliers(Iterable<Object[]> cases) {
        List<TestCaseSupplier> result = new ArrayList<>();
        for (Object[] c : cases) {
            if (c.length != 1) {
                throw new IllegalArgumentException("weird layout for test cases");
            }
            TestCaseSupplier supplier = (TestCaseSupplier) c[0];
            result.add(supplier);
        }
        return result;
    }

    public final void test() {
        int checked = 0;
        List<TestCaseSupplier> cases = cases();
        Set<List<DataType>> valid = cases.stream().map(TestCaseSupplier::types).collect(Collectors.toSet());
        List<Set<DataType>> validPerPosition = AbstractFunctionTestCase.validPerPosition(valid);
        Iterable<List<DataType>> testCandidates = testCandidates(cases, valid)::iterator;
        for (List<DataType> signature : testCandidates) {
            logger.debug("checking {}", signature);
            List<Expression> args = new ArrayList<>(signature.size());
            for (DataType type : signature) {
                args.add(randomLiteral(type));
            }
            Expression expression = build(Source.synthetic(sourceForSignature(signature)), args);
            assertTrue("expected unresolved " + expression, expression.typeResolved().unresolved());
            assertThat(expression.typeResolved().message(), expectedTypeErrorMatcher(validPerPosition, signature));
            checked++;
        }
        logger.info("checked {} signatures", checked);
        assertNumberOfCheckedSignatures(checked);
    }

    /**
     * Assert the number of checked signature. Generally shouldn't be overridden but
     * can be to assert that, for example, there weren't any unsupported signatures.
     */
    protected void assertNumberOfCheckedSignatures(int checked) {
        assertThat("didn't check any signatures", checked, greaterThan(0));
    }

    /**
     * Build a {@link Stream} of test signatures that we should check are invalid.
     */
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        return cases.stream()
            .map(s -> s.types().size())
            .collect(Collectors.toSet())
            .stream()
            .flatMap(AbstractFunctionTestCase::allPermutations)
            .filter(types -> valid.contains(types) == false)
            /*
             * Skip any cases with more than one null. Our tests don't generate
             * the full combinatorial explosions of all nulls - just a single null.
             * Hopefully <null>, <null> cases will function the same as <null>, <valid>
             * cases.
             */
            .filter(types -> types.stream().filter(t -> t == DataType.NULL).count() <= 1);
    }

    protected static String sourceForSignature(List<DataType> signature) {
        StringBuilder source = new StringBuilder();
        for (DataType type : signature) {
            if (false == source.isEmpty()) {
                source.append(", ");
            }
            source.append(type.typeName());
        }
        return source.toString();
    }

    /**
     * Build the expected error message for an invalid type signature.
     */
    protected static String typeErrorMessage(
        boolean includeOrdinal,
        List<Set<DataType>> validPerPosition,
        List<DataType> signature,
        AbstractFunctionTestCase.PositionalErrorMessageSupplier expectedTypeSupplier
    ) {
        int badArgPosition = -1;
        for (int i = 0; i < signature.size(); i++) {
            if (validPerPosition.get(i).contains(signature.get(i)) == false) {
                badArgPosition = i;
                break;
            }
        }
        if (badArgPosition == -1) {
            throw new IllegalStateException(
                "Can't generate error message for these types, you probably need a custom error message function signature =" + signature
            );
        }
        String ordinal = includeOrdinal ? TypeResolutions.ParamOrdinal.fromIndex(badArgPosition).name().toLowerCase(Locale.ROOT) + " " : "";
        String source = sourceForSignature(signature);
        String expectedTypeString = expectedTypeSupplier.apply(validPerPosition.get(badArgPosition), badArgPosition);
        String name = signature.get(badArgPosition).typeName();
        return ordinal + "argument of [" + source + "] must be [" + expectedTypeString + "], found value [] type [" + name + "]";
    }

    protected static String errorMessageStringForBinaryOperators(
        List<Set<DataType>> validPerPosition,
        List<DataType> signature,
        AbstractFunctionTestCase.PositionalErrorMessageSupplier positionalErrorMessageSupplier
    ) {
        try {
            return typeErrorMessage(true, validPerPosition, signature, positionalErrorMessageSupplier);
        } catch (IllegalStateException e) {
            String source = sourceForSignature(signature);
            // This means all the positional args were okay, so the expected error is from the combination
            if (signature.get(0).equals(DataType.UNSIGNED_LONG)) {
                return "first argument of ["
                    + source
                    + "] is [unsigned_long] and second is ["
                    + signature.get(1).typeName()
                    + "]. [unsigned_long] can only be operated on together with another [unsigned_long]";

            }
            if (signature.get(1).equals(DataType.UNSIGNED_LONG)) {
                return "first argument of ["
                    + source
                    + "] is ["
                    + signature.get(0).typeName()
                    + "] and second is [unsigned_long]. [unsigned_long] can only be operated on together with another [unsigned_long]";
            }
            return "first argument of ["
                + source
                + "] is ["
                + (signature.get(0).isNumeric() ? "numeric" : signature.get(0).typeName())
                + "] so second argument must also be ["
                + (signature.get(0).isNumeric() ? "numeric" : signature.get(0).typeName())
                + "] but was ["
                + signature.get(1).typeName()
                + "]";

        }
    }
}
