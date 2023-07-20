/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.builder;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public class SubSearchSourceBuilderTests extends AbstractXContentSerializingTestCase<SubSearchSourceBuilder> {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(TermQueryBuilder.NAME), TermQueryBuilder::fromXContent)
            )
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new))
        );
    }

    @Override
    protected SubSearchSourceBuilder createTestInstance() {
        return new SubSearchSourceBuilder(
            new TermQueryBuilder(randomAlphaOfLength(randomIntBetween(1, 30)), randomAlphaOfLength(randomIntBetween(1, 30)))
        );
    }

    @Override
    protected SubSearchSourceBuilder mutateInstance(SubSearchSourceBuilder instance) throws IOException {
        TermQueryBuilder tqb = (TermQueryBuilder) instance.getQueryBuilder();
        if (randomBoolean()) {
            return new SubSearchSourceBuilder(new TermQueryBuilder(tqb.fieldName() + "z", tqb.value()));
        } else {
            return new SubSearchSourceBuilder(new TermQueryBuilder(tqb.fieldName(), tqb.value() + "z"));
        }
    }

    @Override
    protected Writeable.Reader<SubSearchSourceBuilder> instanceReader() {
        return SubSearchSourceBuilder::new;
    }

    @Override
    protected SubSearchSourceBuilder doParseInstance(XContentParser parser) throws IOException {
        return SubSearchSourceBuilder.fromXContent(parser, new SearchUsage());
    }
}
