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
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class EnrichPolicyTests extends AbstractSerializingTestCase<EnrichPolicy> {

    @Override
    protected EnrichPolicy doParseInstance(XContentParser parser) throws IOException {
        return EnrichPolicy.fromXContent(parser);
    }

    @Override
    protected EnrichPolicy createTestInstance() {
        return randomEnrichPolicy(randomFrom(XContentType.values()));
    }

    @Override
    protected EnrichPolicy createXContextTestInstance(XContentType xContentType) {
        return randomEnrichPolicy(xContentType);
    }

    public static EnrichPolicy randomEnrichPolicy(XContentType xContentType) {
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
            EnrichPolicy.QuerySource querySource = new EnrichPolicy.QuerySource(new BytesArray(out.toByteArray()), content.contentType());
            return new EnrichPolicy(
                randomFrom(EnrichPolicy.SUPPORTED_POLICY_TYPES),
                randomBoolean() ? querySource : null,
                Arrays.stream(generateRandomStringArray(8, 4, false, false))
                    .map(s -> s.toLowerCase(Locale.ROOT))
                    .collect(Collectors.toList()),
                randomAlphaOfLength(4),
                Arrays.asList(generateRandomStringArray(8, 4, false, false))
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }

    @Override
    protected Writeable.Reader<EnrichPolicy> instanceReader() {
        return EnrichPolicy::new;
    }

    @Override
    protected void assertEqualInstances(EnrichPolicy expectedInstance, EnrichPolicy newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertEqualPolicies(expectedInstance, newInstance);
    }

    public static void assertEqualPolicies(EnrichPolicy expectedInstance, EnrichPolicy newInstance) {
        assertThat(newInstance.getType(), equalTo(expectedInstance.getType()));
        if (newInstance.getQuery() != null) {
            // testFromXContent, always shuffles the xcontent and then byte wise the query is different, so we check the parsed version:
            assertThat(newInstance.getQuery().getQueryAsMap(), equalTo(expectedInstance.getQuery().getQueryAsMap()));
        } else {
            assertThat(expectedInstance.getQuery(), nullValue());
        }
        assertThat(newInstance.getIndices(), equalTo(expectedInstance.getIndices()));
        assertThat(newInstance.getMatchField(), equalTo(expectedInstance.getMatchField()));
        assertThat(newInstance.getEnrichFields(), equalTo(expectedInstance.getEnrichFields()));
    }
}
