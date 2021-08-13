/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.metadata.AliasMetadata.Builder;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;

public class AliasMetadataTests extends AbstractXContentTestCase<AliasMetadata> {

    public void testSerialization() throws IOException {
        final AliasMetadata before =
                AliasMetadata
                        .builder("alias")
                        .filter("{ \"term\": \"foo\"}")
                        .indexRouting("indexRouting")
                        .routing("routing")
                        .searchRouting("trim,tw , ltw , lw")
                        .writeIndex(randomBoolean() ? null : randomBoolean())
                        .isHidden(randomBoolean() ? null : randomBoolean())
                        .build();

        assertThat(before.searchRoutingValues(), equalTo(Sets.newHashSet("trim", "tw ", " ltw ", " lw")));

        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final AliasMetadata after = new AliasMetadata(in);

        assertThat(after, equalTo(before));
    }

    @Override
    protected void assertEqualInstances(AliasMetadata expectedInstance, AliasMetadata newInstance) {
        assertNotSame(newInstance, expectedInstance);
        if (expectedInstance.writeIndex() == null) {
            expectedInstance = AliasMetadata.builder(expectedInstance.alias())
                .filter(expectedInstance.filter())
                .indexRouting(expectedInstance.indexRouting())
                .searchRouting(expectedInstance.searchRouting())
                .writeIndex(randomBoolean() ? null : randomBoolean())
                .isHidden(randomBoolean() ? null : randomBoolean())
                .build();
        }
        assertEquals(expectedInstance, newInstance);
        assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
    }

    @Override
    protected AliasMetadata createTestInstance() {
        return createTestItem();
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return p -> p.equals("") // do not add elements at the top-level as any element at this level is parsed as a new alias
                || p.contains(".filter"); // do not insert random data into AliasMetadata#filter
    }

    @Override
    protected AliasMetadata doParseInstance(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        assertEquals(XContentParser.Token.FIELD_NAME, parser.currentToken());
        AliasMetadata aliasMetadata = AliasMetadata.Builder.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return aliasMetadata;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    private static AliasMetadata createTestItem() {
        Builder builder = AliasMetadata.builder(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            builder.routing(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.searchRouting(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.indexRouting(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.filter("{\"term\":{\"year\":2016}}");
        }
        builder.writeIndex(randomBoolean());

        if (randomBoolean()) {
            builder.isHidden(randomBoolean());
        }
        return builder.build();
    }

}
