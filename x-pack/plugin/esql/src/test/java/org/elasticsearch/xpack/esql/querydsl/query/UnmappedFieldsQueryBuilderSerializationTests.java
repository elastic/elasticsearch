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

import java.util.List;

public class UnmappedFieldsQueryBuilderSerializationTests extends AbstractWireSerializingTestCase<UnmappedFieldsQueryBuilder> {
    @Override
    protected UnmappedFieldsQueryBuilder createTestInstance() {
        return new UnmappedFieldsQueryBuilder(randomFields(), randomQuery());
    }

    private static List<String> randomFields() {
        return randomList(1, 3, () -> randomAlphaOfLength(3));
    }

    private static QueryBuilder randomQuery() {
        return new TermQueryBuilder(randomAlphaOfLength(1), randomAlphaOfLength(2));
    }

    @Override
    protected UnmappedFieldsQueryBuilder mutateInstance(UnmappedFieldsQueryBuilder instance) {
        return switch (between(0, 1)) {
            case 0 -> new UnmappedFieldsQueryBuilder(
                randomValueOtherThan(instance.fields(), UnmappedFieldsQueryBuilderSerializationTests::randomFields),
                instance.query()
            );
            case 1 -> new UnmappedFieldsQueryBuilder(
                instance.fields(),
                randomValueOtherThan(instance.query(), UnmappedFieldsQueryBuilderSerializationTests::randomQuery)
            );
            default -> throw new IllegalArgumentException();
        };
    }

    @Override
    protected Writeable.Reader<UnmappedFieldsQueryBuilder> instanceReader() {
        return UnmappedFieldsQueryBuilder::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                UnmappedFieldsQueryBuilder.ENTRY,
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new)
            )
        );
    }
}
