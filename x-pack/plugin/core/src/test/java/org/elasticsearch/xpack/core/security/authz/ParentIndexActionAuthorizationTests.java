/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@link ParentIndexActionAuthorization} class.
 */
public class ParentIndexActionAuthorizationTests extends ESTestCase {

    public void testSerialization() throws IOException {
        ParentIndexActionAuthorization authorization = createRandom();
        final BytesStreamOutput out = new BytesStreamOutput();
        authorization.writeTo(out);
        assertThat(ParentIndexActionAuthorization.readFrom(out.bytes().streamInput()), equalTo(authorization));
    }

    public void testXContent() throws IOException {
        ParentIndexActionAuthorization authorization = createRandom();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        authorization.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        assertThat(ParentIndexActionAuthorization.fromXContent(parser), equalTo(authorization));
    }

    private static ParentIndexActionAuthorization createRandom() {
        String action = randomAlphaOfLengthBetween(5, 20);
        boolean granted = randomBoolean();
        Version version = randomFrom(VersionUtils.allVersions());
        return new ParentIndexActionAuthorization(version, action, granted);
    }
}
