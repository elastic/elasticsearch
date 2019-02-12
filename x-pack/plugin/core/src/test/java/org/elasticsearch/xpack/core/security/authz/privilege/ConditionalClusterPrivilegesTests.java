/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackClientPlugin;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION;
import static org.hamcrest.Matchers.equalTo;

public class ConditionalClusterPrivilegesTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final ConditionalClusterPrivilege[] original = buildSecurityPrivileges();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            ConditionalClusterPrivileges.writeArray(out, original);
            final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin(Settings.EMPTY).getNamedWriteables());
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                final ConditionalClusterPrivilege[] copy = ConditionalClusterPrivileges.readArray(in);
                assertThat(copy, equalTo(original));
                assertThat(original, equalTo(copy));
            }
        }
    }

    public void testGenerateAndParseXContent() throws Exception {
        final XContent xContent = randomFrom(XContentType.values()).xContent();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final XContentBuilder builder = new XContentBuilder(xContent, out);

            final List<ConditionalClusterPrivilege> original = Arrays.asList(buildSecurityPrivileges());
            ConditionalClusterPrivileges.toXContent(builder, ToXContent.EMPTY_PARAMS, original);
            builder.flush();

            final byte[] bytes = out.toByteArray();
            try (XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY, THROW_UNSUPPORTED_OPERATION, bytes)) {
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                final List<ConditionalClusterPrivilege> clone = ConditionalClusterPrivileges.parse(parser);
                assertThat(clone, equalTo(original));
                assertThat(original, equalTo(clone));
            }
        }
    }

    private ConditionalClusterPrivilege[] buildSecurityPrivileges() {
        return buildSecurityPrivileges(randomIntBetween(4, 7));
    }

    private ConditionalClusterPrivilege[] buildSecurityPrivileges(int applicationNameLength) {
        return new ConditionalClusterPrivilege[] {
            ManageApplicationPrivilegesTests.buildPrivileges(applicationNameLength)
        };
    }
}
