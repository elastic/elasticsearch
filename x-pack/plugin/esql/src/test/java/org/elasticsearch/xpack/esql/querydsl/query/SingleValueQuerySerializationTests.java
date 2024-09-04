/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.AbstractWireTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;

public class SingleValueQuerySerializationTests extends AbstractWireTestCase<SingleValueQuery.Builder> {
    /**
     * We use a single random config for all serialization because it's pretty
     * heavy to build, especially in {@link #testConcurrentSerialization()}.
     */
    private Configuration config;

    @Override
    protected SingleValueQuery.Builder createTestInstance() {
        return new SingleValueQuery.Builder(randomQuery(), randomFieldName(), Source.EMPTY);
    }

    private QueryBuilder randomQuery() {
        return new TermQueryBuilder(randomAlphaOfLength(1), randomAlphaOfLength(2));
    }

    private String randomFieldName() {
        return randomAlphaOfLength(3);
    }

    @Override
    protected SingleValueQuery.Builder mutateInstance(SingleValueQuery.Builder instance) {
        return switch (between(0, 1)) {
            case 0 -> new SingleValueQuery.Builder(
                randomValueOtherThan(instance.next(), this::randomQuery),
                instance.field(),
                Source.EMPTY
            );
            case 1 -> new SingleValueQuery.Builder(
                instance.next(),
                randomValueOtherThan(instance.field(), this::randomFieldName),
                Source.EMPTY
            );
            default -> throw new IllegalArgumentException();
        };
    }

    @Override
    protected final SingleValueQuery.Builder copyInstance(SingleValueQuery.Builder instance, TransportVersion version) throws IOException {
        return copyInstance(
            instance,
            getNamedWriteableRegistry(),
            (out, v) -> new PlanStreamOutput(out, new PlanNameRegistry(), config).writeNamedWriteable(v),
            in -> {
                PlanStreamInput pin = new PlanStreamInput(in, new PlanNameRegistry(), in.namedWriteableRegistry(), config);
                return (SingleValueQuery.Builder) pin.readNamedWriteable(QueryBuilder.class);
            },
            version
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                SingleValueQuery.ENTRY,
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new)
            )
        );
    }

    @Before
    public void initConfig() {
        this.config = randomConfiguration();
    }
}
