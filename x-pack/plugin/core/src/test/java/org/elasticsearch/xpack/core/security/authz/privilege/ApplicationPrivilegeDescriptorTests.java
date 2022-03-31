/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.iterableWithSize;

public class ApplicationPrivilegeDescriptorTests extends ESTestCase {

    public void testEqualsAndHashCode() {
        final ApplicationPrivilegeDescriptor privilege = randomPrivilege();
        final EqualsHashCodeTestUtils.MutateFunction<ApplicationPrivilegeDescriptor> mutate = randomFrom(
            orig -> new ApplicationPrivilegeDescriptor("x" + orig.getApplication(), orig.getName(), orig.getActions(), orig.getMetadata()),
            orig -> new ApplicationPrivilegeDescriptor(orig.getApplication(), "x" + orig.getName(), orig.getActions(), orig.getMetadata()),
            orig -> new ApplicationPrivilegeDescriptor(
                orig.getApplication(),
                orig.getName(),
                Collections.singleton("*"),
                orig.getMetadata()
            ),
            orig -> new ApplicationPrivilegeDescriptor(
                orig.getApplication(),
                orig.getName(),
                orig.getActions(),
                Collections.singletonMap("mutate", -1L)
            )
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            privilege,
            original -> new ApplicationPrivilegeDescriptor(
                original.getApplication(),
                original.getName(),
                original.getActions(),
                original.getMetadata()
            ),
            mutate
        );
    }

    public void testSerialization() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            final ApplicationPrivilegeDescriptor original = randomPrivilege();
            original.writeTo(out);
            final ApplicationPrivilegeDescriptor clone = new ApplicationPrivilegeDescriptor(out.bytes().streamInput());
            assertThat(clone, Matchers.equalTo(original));
            assertThat(original, Matchers.equalTo(clone));
        }
    }

    public void testXContentGenerationAndParsing() throws IOException {
        final boolean includeTypeField = randomBoolean();

        final XContent xContent = randomFrom(XContentType.values()).xContent();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final XContentBuilder builder = new XContentBuilder(xContent, out);

            final ApplicationPrivilegeDescriptor original = randomPrivilege();
            if (includeTypeField) {
                original.toXContent(builder, true);
            } else if (randomBoolean()) {
                original.toXContent(builder, false);
            } else {
                original.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            builder.flush();

            final byte[] bytes = out.toByteArray();
            try (XContentParser parser = xContent.createParser(XContentParserConfiguration.EMPTY, bytes)) {
                final ApplicationPrivilegeDescriptor clone = ApplicationPrivilegeDescriptor.parse(
                    parser,
                    randomBoolean() ? randomAlphaOfLength(3) : null,
                    randomBoolean() ? randomAlphaOfLength(3) : null,
                    includeTypeField
                );
                assertThat(clone, Matchers.equalTo(original));
                assertThat(original, Matchers.equalTo(clone));
            }
        }
    }

    public void testParseXContentWithDefaultNames() throws IOException {
        final String json = "{ \"actions\": [ \"data:read\" ], \"metadata\" : { \"num\": 1, \"bool\":false } }";
        final XContent xContent = XContentType.JSON.xContent();
        try (XContentParser parser = xContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            final ApplicationPrivilegeDescriptor privilege = ApplicationPrivilegeDescriptor.parse(parser, "my_app", "read", false);
            assertThat(privilege.getApplication(), equalTo("my_app"));
            assertThat(privilege.getName(), equalTo("read"));
            assertThat(privilege.getActions(), contains("data:read"));
            assertThat(privilege.getMetadata().entrySet(), iterableWithSize(2));
            assertThat(privilege.getMetadata().get("num"), equalTo(1));
            assertThat(privilege.getMetadata().get("bool"), equalTo(false));
        }
    }

    public void testParseXContentWithoutUsingDefaultNames() throws IOException {
        final String json = """
            {  "application": "your_app",  "name": "write",  "actions": [ "data:write" ]}""";
        final XContent xContent = XContentType.JSON.xContent();
        try (XContentParser parser = xContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            final ApplicationPrivilegeDescriptor privilege = ApplicationPrivilegeDescriptor.parse(parser, "my_app", "read", false);
            assertThat(privilege.getApplication(), equalTo("your_app"));
            assertThat(privilege.getName(), equalTo("write"));
            assertThat(privilege.getActions(), contains("data:write"));
            assertThat(privilege.getMetadata().entrySet(), iterableWithSize(0));
        }
    }

    private ApplicationPrivilegeDescriptor randomPrivilege() {
        final String applicationName;
        if (randomBoolean()) {
            applicationName = "*";
        } else {
            applicationName = randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(2, 10);
        }
        final String privilegeName = randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(2, 8);
        final String[] patterns = new String[randomIntBetween(0, 5)];
        for (int i = 0; i < patterns.length; i++) {
            final String suffix = randomBoolean() ? "*" : randomAlphaOfLengthBetween(4, 9);
            patterns[i] = randomAlphaOfLengthBetween(2, 5) + "/" + suffix;
        }

        final Map<String, Object> metadata = new HashMap<>();
        for (int i = randomInt(3); i > 0; i--) {
            metadata.put(randomAlphaOfLengthBetween(2, 5), randomFrom(randomBoolean(), randomInt(10), randomAlphaOfLength(5)));
        }
        return new ApplicationPrivilegeDescriptor(applicationName, privilegeName, Sets.newHashSet(patterns), metadata);
    }

}
