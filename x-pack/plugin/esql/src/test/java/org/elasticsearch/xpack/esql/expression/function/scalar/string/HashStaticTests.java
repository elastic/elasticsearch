/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.Result;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.OptionalMatchers;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.After;

import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.evaluator;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.field;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class HashStaticTests extends ESTestCase {

    public void testInvalidAlgorithmLiteral() {
        Source source = new Source(0, 0, "hast(\"invalid\", input)");
        DriverContext driverContext = driverContext();
        InvalidArgumentException e = expectThrows(
            InvalidArgumentException.class,
            () -> evaluator(
                new Hash(source, new Literal(source, new BytesRef("invalid"), DataType.KEYWORD), field("input", DataType.KEYWORD))
            ).get(driverContext)
        );
        assertThat(e.getMessage(), startsWith("invalid algorithm for [hast(\"invalid\", input)]: invalid MessageDigest not available"));
    }

    public void testTryCreateUnavailableMd5() throws NoSuchAlgorithmException {
        assumeFalse("We run with different security providers in FIPS, and changing them at runtime is more complicated", inFipsJvm());
        final Provider sunProvider = Security.getProvider("SUN");
        try {
            Security.removeProvider("SUN");
            final Result<Hash.HashFunction, NoSuchAlgorithmException> result = Hash.HashFunction.tryCreate("MD5");
            assertThat(result.isSuccessful(), is(false));
            assertThat(result.failure(), OptionalMatchers.isPresentWith(throwableWithMessage(containsString("MD5"))));
            expectThrows(NoSuchAlgorithmException.class, result::get);
        } finally {
            Security.addProvider(sunProvider);
        }

        {
            final Result<Hash.HashFunction, NoSuchAlgorithmException> result = Hash.HashFunction.tryCreate("MD5");
            assertThat(result.isSuccessful(), is(true));
            assertThat(result.failure(), OptionalMatchers.isEmpty());
            assertThat(result.get().algorithm(), is("MD5"));
        }
    }

    /**
     * The following fields and methods were borrowed from AbstractScalarFunctionTestCase
     */
    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    private DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        return new DriverContext(bigArrays, new BlockFactory(breaker, bigArrays));
    }

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }
}
