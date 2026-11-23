/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class MaxSerializationTests extends AbstractExpressionSerializationTests<Max> {
    @Override
    protected Max createTestInstance() {
        return new Max(randomSource(), randomChild(), randomChild(), randomChild(), randomBoolean(), randomBoolean());
    }

    /**
     * {@code createTestInstance} randomizes the non-finite flag, so a single generic round-trip only exercises one of
     * the two forms; this covers both.
     */
    public void testAllowNonFiniteSurvivesCurrentVersionRoundTrip() throws IOException {
        Max nonFinite = new Max(randomSource(), randomChild(), randomChild(), randomChild(), true);
        assertTrue(copyInstance(nonFinite).allowNonFinite());

        Max strict = new Max(randomSource(), randomChild(), randomChild(), randomChild(), false);
        assertFalse(copyInstance(strict).allowNonFinite());
    }

    /**
     * The NaN-skipping flag is written only when the receiving node supports it; a node that predates it reads the old rule,
     * where a NaN input makes the result NaN.
     */
    public void testSkipNanSurvivesCurrentVersionRoundTrip() throws IOException {
        Max skipping = new Max(randomSource(), randomChild(), randomChild(), randomChild(), false, true);
        assertTrue(copyInstance(skipping).skipNan());

        Max propagating = new Max(randomSource(), randomChild(), randomChild(), randomChild(), false, false);
        assertFalse(copyInstance(propagating).skipNan());
    }

    public void testSkipNanIsNotWrittenToNodesThatPredateIt() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(Min.ESQL_MINMAX_SKIP_NAN);
        Max skipping = new Max(
            Source.EMPTY,
            new Literal(Source.EMPTY, 1.0, DataType.DOUBLE),
            Literal.TRUE,
            AggregateFunction.NO_WINDOW,
            false,
            true
        );

        Max copy = copyInstance(skipping, oldVersion);

        assertFalse(copy.skipNan());
        assertThat(copy.field(), equalTo(skipping.field()));
    }

    @Override
    protected Max mutateInstance(Max instance) throws IOException {
        return new Max(instance.source(), randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild));
    }
}
