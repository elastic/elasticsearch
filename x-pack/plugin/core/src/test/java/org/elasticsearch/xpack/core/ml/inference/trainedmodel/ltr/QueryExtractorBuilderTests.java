/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.QueryProviderTests;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Predicate;

public class QueryExtractorBuilderTests extends AbstractXContentSerializingTestCase<QueryExtractorBuilder> {

    protected boolean lenient;

    public static QueryExtractorBuilder randomInstance() {
        return new QueryExtractorBuilder(randomAlphaOfLength(10), QueryProviderTests.createRandomValidQueryProvider());
    }

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected Writeable.Reader<QueryExtractorBuilder> instanceReader() {
        return QueryExtractorBuilder::new;
    }

    @Override
    protected QueryExtractorBuilder createTestInstance() {
        return randomInstance();
    }

    @Override
    protected QueryExtractorBuilder mutateInstance(QueryExtractorBuilder instance) throws IOException {
        int i = randomInt(1);
        return switch (i) {
            case 0 -> new QueryExtractorBuilder(randomAlphaOfLength(10), instance.query());
            case 1 -> new QueryExtractorBuilder(instance.featureName(), QueryProviderTests.createRandomValidQueryProvider());
            default -> throw new AssertionError("unknown random case for instance mutation");
        };
    }

    @Override
    protected QueryExtractorBuilder doParseInstance(XContentParser parser) throws IOException {
        return QueryExtractorBuilder.fromXContent(parser, lenient);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedWriteables());
    }
}
