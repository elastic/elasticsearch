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
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.XPackClientPlugin;

import java.io.ByteArrayOutputStream;

import static org.elasticsearch.common.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION;
import static org.hamcrest.Matchers.equalTo;

public class SecurityPrivilegesTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final SecurityPrivileges original = buildSecurityPrivileges();
        try (final BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin(Settings.EMPTY).getNamedWriteables());
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                final SecurityPrivileges copy = SecurityPrivileges.createFrom(in);
                assertThat(copy, equalTo(original));
                assertThat(original, equalTo(copy));
            }
        }
    }

    public void testGenerateAndParseXContent() throws Exception {
        final XContent xContent = randomFrom(XContentType.values()).xContent();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final XContentBuilder builder = new XContentBuilder(xContent, out);

            final SecurityPrivileges original = buildSecurityPrivileges();
            original.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.flush();

            final byte[] bytes = out.toByteArray();
            try (XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY, THROW_UNSUPPORTED_OPERATION, bytes)) {
                final SecurityPrivileges clone = SecurityPrivileges.parse(parser);
                assertThat(clone, equalTo(original));
                assertThat(original, equalTo(clone));
            }
        }
    }

    public void testEqualsAndHashCode() {
        final int applicationNameLength = randomIntBetween(4, 7);
        final SecurityPrivileges privileges = buildSecurityPrivileges(applicationNameLength);
        final EqualsHashCodeTestUtils.MutateFunction<SecurityPrivileges> mutate = orig ->
            rarely() ? SecurityPrivileges.EMPTY : buildSecurityPrivileges(applicationNameLength + randomIntBetween(1, 3));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(privileges, this::clone, mutate);
    }

    private SecurityPrivileges clone(SecurityPrivileges original) {
        final SecurityPrivileges clone = new SecurityPrivileges();
        for (SecurityPrivileges.Category category : SecurityPrivileges.Category.values()) {
            original.get(category).forEach(clone::add);
        }
        return clone;
    }

    private SecurityPrivileges buildSecurityPrivileges() {
        return buildSecurityPrivileges(randomIntBetween(4, 7));
    }

    private SecurityPrivileges buildSecurityPrivileges(int applicationNameLength) {
        final SecurityPrivileges privileges = new SecurityPrivileges();
        privileges.add(ManageApplicationPrivilegesTests.buildPrivileges(applicationNameLength));
        return privileges;
    }
}
