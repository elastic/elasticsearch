/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;

public class SingleValueQuerySerializationTests extends AbstractWireSerializingTestCase<SingleValueQuery.Builder> {
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
    protected Writeable.Reader<SingleValueQuery.Builder> instanceReader() {
        return SingleValueQuery.Builder::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new))
        );
    }
}
