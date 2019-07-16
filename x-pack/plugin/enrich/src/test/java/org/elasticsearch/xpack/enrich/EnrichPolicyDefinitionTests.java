/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicyDefinition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class EnrichPolicyDefinitionTests extends AbstractSerializingTestCase<EnrichPolicyDefinition> {

    @Override
    protected EnrichPolicyDefinition doParseInstance(XContentParser parser) throws IOException {
        return EnrichPolicyDefinition.fromXContent(parser);
    }

    @Override
    protected EnrichPolicyDefinition createTestInstance() {
        return randomEnrichPolicyDefinition(randomFrom(XContentType.values()));
    }

    @Override
    protected EnrichPolicyDefinition createXContextTestInstance(XContentType xContentType) {
        return randomEnrichPolicyDefinition(xContentType);
    }

    public static EnrichPolicyDefinition randomEnrichPolicyDefinition(XContentType xContentType) {
        final QueryBuilder queryBuilder;
        if (randomBoolean()) {
            queryBuilder = new MatchAllQueryBuilder();
        } else {
            queryBuilder = new TermQueryBuilder(randomAlphaOfLength(4), randomAlphaOfLength(4));
        }

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (XContentBuilder xContentBuilder = XContentFactory.contentBuilder(xContentType, out)) {
            XContentBuilder content = queryBuilder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            content.flush();
            EnrichPolicyDefinition.QuerySource querySource = new EnrichPolicyDefinition.QuerySource(
                new BytesArray(out.toByteArray()), content.contentType());
            return new EnrichPolicyDefinition(
                randomFrom(EnrichPolicyDefinition.SUPPORTED_POLICY_TYPES),
                randomBoolean() ? querySource : null,
                Arrays.asList(generateRandomStringArray(8, 4, false, false)),
                randomAlphaOfLength(4),
                Arrays.asList(generateRandomStringArray(8, 4, false, false))
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }

    @Override
    protected Writeable.Reader<EnrichPolicyDefinition> instanceReader() {
        return EnrichPolicyDefinition::new;
    }

    @Override
    protected void assertEqualInstances(EnrichPolicyDefinition expectedInstance, EnrichPolicyDefinition newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertEqualPolicyDefinitions(expectedInstance, newInstance);
    }

    public static void assertEqualPolicyDefinitions(EnrichPolicyDefinition expectedInstance, EnrichPolicyDefinition newInstance) {
        assertThat(newInstance.getType(), equalTo(expectedInstance.getType()));
        if (newInstance.getQuery() != null) {
            // testFromXContent, always shuffles the xcontent and then byte wise the query is different, so we check the parsed version:
            assertThat(newInstance.getQuery().getQueryAsMap(), equalTo(expectedInstance.getQuery().getQueryAsMap()));
        } else {
            assertThat(expectedInstance.getQuery(), nullValue());
        }
        assertThat(newInstance.getIndices(), equalTo(expectedInstance.getIndices()));
        assertThat(newInstance.getEnrichKey(), equalTo(expectedInstance.getEnrichKey()));
        assertThat(newInstance.getEnrichValues(), equalTo(expectedInstance.getEnrichValues()));
    }
}
