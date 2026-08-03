/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.eql.EqlQueryOptions;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;

public class EqlQueryExecSerializationTests extends AbstractPhysicalPlanSerializationTests<EqlQueryExec> {

    public static EqlQueryExec randomEqlQueryExec() {
        Source source = randomSource();
        String index = randomIdentifier();
        String query = randomAlphaOfLengthBetween(1, 50);
        EqlQueryOptions options = randomEqlQueryOptions();
        List<Attribute> output = randomList(1, 10, () -> randomReferenceAttribute(true));
        Integer limit = randomBoolean() ? randomIntBetween(1, 10_000) : null;
        return new EqlQueryExec(source, index, query, options, output, limit);
    }

    private static EqlQueryOptions randomEqlQueryOptions() {
        return new EqlQueryOptions(randomOptionalIdentifier(), randomOptionalIdentifier(), randomOptionalIdentifier());
    }

    private static String randomOptionalIdentifier() {
        return randomBoolean() ? randomIdentifier() : null;
    }

    @Override
    protected EqlQueryExec createTestInstance() {
        return randomEqlQueryExec();
    }

    @Override
    protected EqlQueryExec mutateInstance(EqlQueryExec instance) throws IOException {
        String index = instance.index();
        String query = instance.query();
        EqlQueryOptions options = instance.options();
        List<Attribute> output = instance.output();
        Integer limit = instance.limit();
        switch (between(0, 4)) {
            case 0 -> index = randomValueOtherThan(index, () -> randomIdentifier());
            case 1 -> query = randomValueOtherThan(query, () -> randomAlphaOfLengthBetween(1, 50));
            case 2 -> options = randomValueOtherThan(options, EqlQueryExecSerializationTests::randomEqlQueryOptions);
            case 3 -> output = randomValueOtherThan(output, () -> randomList(1, 10, () -> randomReferenceAttribute(true)));
            case 4 -> limit = randomValueOtherThan(limit, () -> randomBoolean() ? randomIntBetween(1, 10_000) : null);
            default -> throw new AssertionError("unexpected mutation branch");
        }
        return new EqlQueryExec(instance.source(), index, query, options, output, limit);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
