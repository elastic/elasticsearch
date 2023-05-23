/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.RoleRestriction;

import java.io.IOException;

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptor.RoleRestriction.WORKFLOWS_RESTRICTION_VERSION;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;

public class RoleRestrictionTests extends ESTestCase {

    public void testParse() throws Exception {
        String json = """
            {
                "workflows": ["search_application"]
            }
            """;
        RoleRestriction r = RoleRestriction.parse("test_restriction", createJsonParser(json));
        assertThat(r.getWorkflows(), arrayContainingInAnyOrder("search_application"));
        assertThat(r.hasWorkflowsRestriction(), equalTo(true));
        assertThat(r.isEmpty(), equalTo(false));

        json = randomBoolean() ? """
            {
                "workflows": []
            }
            """ : "{}";
        r = RoleRestriction.parse("test_restriction", createJsonParser(json));
        assertThat(r.getWorkflows(), arrayWithSize(0));
        assertThat(r.hasWorkflowsRestriction(), equalTo(false));
        assertThat(r.isEmpty(), equalTo(true));
    }

    public void testToXContent() throws Exception {
        final RoleRestriction restriction = randomWorkflowsRestriction(0, 5);
        final XContentType xContentType = randomFrom(XContentType.values());
        final BytesReference xContentValue = toShuffledXContent(restriction, xContentType, ToXContent.EMPTY_PARAMS, false);
        final XContentParser parser = xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, xContentValue.streamInput());
        final RoleRestriction parsed = RoleRestriction.parse(randomAlphaOfLengthBetween(3, 6), parser);
        assertThat(parsed, equalTo(restriction));
    }

    public void testSerializationWithSupportedVersion() throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        final TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            WORKFLOWS_RESTRICTION_VERSION,
            TransportVersion.CURRENT
        );
        out.setTransportVersion(version);
        final RoleRestriction original = randomWorkflowsRestriction(0, 3);
        original.writeTo(out);

        final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin().getNamedWriteables());
        StreamInput in = new NamedWriteableAwareStreamInput(ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())), registry);
        in.setTransportVersion(out.getTransportVersion());
        final RoleRestriction actual = RoleRestriction.readFrom(in);

        assertThat(actual, equalTo(original));
    }

    public void testSerializationWithUnsupportedVersion() throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        final TransportVersion versionBeforeWorkflowsRestriction = TransportVersionUtils.getPreviousVersion(WORKFLOWS_RESTRICTION_VERSION);
        final TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersion.MINIMUM_COMPATIBLE,
            versionBeforeWorkflowsRestriction
        );
        out.setTransportVersion(version);
        final RoleRestriction original = randomWorkflowsRestriction(0, 3);
        original.writeTo(out);

        final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin().getNamedWriteables());
        StreamInput in = new NamedWriteableAwareStreamInput(ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())), registry);
        in.setTransportVersion(out.getTransportVersion());
        final RoleRestriction actual = RoleRestriction.readFrom(in);

        assertThat(actual, equalTo(RoleRestriction.NONE));
    }

    public void testIsEmpty() {
        String[] workflows = randomBoolean() ? null : new String[] {};
        RoleRestriction r = new RoleRestriction(workflows);
        assertThat(r.isEmpty(), equalTo(true));
        assertThat(r.hasWorkflowsRestriction(), equalTo(false));

        workflows = randomWorkflowNames(1, 2);
        r = new RoleRestriction(workflows);
        assertThat(r.isEmpty(), equalTo(false));
        assertThat(r.hasWorkflowsRestriction(), equalTo(true));
    }

    private static XContentParser createJsonParser(String json) throws IOException {
        return XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, new BytesArray(json).streamInput());
    }

    public static RoleRestriction randomWorkflowsRestriction(int min, int max) {
        return RoleRestriction.builder().workflows(randomWorkflowNames(min, max)).build();
    }

    public static String[] randomWorkflowNames(int min, int max) {
        // TODO: Change this to use actual workflow names instead of random ones.
        return randomArray(min, max, String[]::new, () -> randomAlphaOfLengthBetween(3, 6));
    }
}
