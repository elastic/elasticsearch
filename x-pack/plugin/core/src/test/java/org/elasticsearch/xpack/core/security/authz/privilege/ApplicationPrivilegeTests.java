/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.hamcrest.Matchers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static org.elasticsearch.common.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.sameInstance;

public class ApplicationPrivilegeTests extends ESTestCase {

    public void testValidationOfApplicationName() {
        // too short
        assertValidationFailure("Application names", () -> new ApplicationPrivilege("ap", "read", "data:read"));
        // must start with lowercase
        assertValidationFailure("Application names", () -> new ApplicationPrivilege("App", "read", "data:read"));
        // must start with letter
        assertValidationFailure("Application names", () -> new ApplicationPrivilege("1app", "read", "data:read"));
        // cannot contain special characters
        assertValidationFailure("Application names",
            () -> new ApplicationPrivilege("app" + randomFrom(":;$#%()+=/'.,".toCharArray()), "read", "data:read"));
        // these should all be OK
        assertNotNull(new ApplicationPrivilege("app", "read", "data:read"));
        assertNotNull(new ApplicationPrivilege("app1", "read", "data:read"));
        assertNotNull(new ApplicationPrivilege("myApp", "read", "data:read"));
        assertNotNull(new ApplicationPrivilege("my-App", "read", "data:read"));
        assertNotNull(new ApplicationPrivilege("my_App", "read", "data:read"));
    }

    public void testValidationOfPrivilegeName() {
        // must start with lowercase
        assertValidationFailure("privilege names", () -> new ApplicationPrivilege("app", "Read", "data:read"));
        // must start with letter
        assertValidationFailure("privilege names", () -> new ApplicationPrivilege("app", "1read", "data:read"));
        // cannot contain special characters
        assertValidationFailure("privilege names",
            () -> new ApplicationPrivilege("app", "read" + randomFrom(":;$#%()+=/',".toCharArray()), "data:read"));
        // these should all be OK
        assertNotNull(new ApplicationPrivilege("app", "read", "data:read"));
        assertNotNull(new ApplicationPrivilege("app", "read1", "data:read"));
        assertNotNull(new ApplicationPrivilege("app", "readData", "data:read"));
        assertNotNull(new ApplicationPrivilege("app", "read-data", "data:read"));
        assertNotNull(new ApplicationPrivilege("app", "read.data", "data:read"));
        assertNotNull(new ApplicationPrivilege("app", "read_data", "data:read"));
    }

    public void testValidationOfActions() {
        // must contain '/' ':' or '*'
        final List<String> invalid = Arrays.asList("data.read", "data_read", "data+read", "read");
        for (String action : invalid) {
            assertValidationFailure("privilege pattern", () -> new ApplicationPrivilege("app", "read", action));
            assertValidationFailure("privilege pattern", () -> new ApplicationPrivilege("app", "read", "data:read", action));
            assertValidationFailure("privilege pattern", () -> new ApplicationPrivilege("app", "read", action, "data/read"));
        }

        // these should all be OK
        assertNotNull(new ApplicationPrivilege("app", "read", "data:read"));
        assertNotNull(new ApplicationPrivilege("app", "read", "data/read"));
        assertNotNull(new ApplicationPrivilege("app", "read", "data/*"));
        assertNotNull(new ApplicationPrivilege("app", "read", "*/read"));
        assertNotNull(new ApplicationPrivilege("app", "read", "*/read", "read:*", "data:read"));
    }

    public void testGetPrivilegeByName() {
        final ApplicationPrivilege myRead = new ApplicationPrivilege("my-app", "read", "data:read/*", "action:login");
        final ApplicationPrivilege myWrite = new ApplicationPrivilege("my-app", "write", "data:write/*", "action:login");
        final ApplicationPrivilege myAdmin = new ApplicationPrivilege("my-app", "admin", "data:read/*", "action:*");
        final ApplicationPrivilege yourRead = new ApplicationPrivilege("your-app", "read", "data:read/*", "action:login");
        final Set<ApplicationPrivilege> stored = Sets.newHashSet(myRead, myWrite, myAdmin, yourRead);

        assertThat(ApplicationPrivilege.get("my-app", Collections.singleton("read"), stored), sameInstance(myRead));
        assertThat(ApplicationPrivilege.get("my-app", Collections.singleton("write"), stored), sameInstance(myWrite));

        final ApplicationPrivilege readWrite = ApplicationPrivilege.get("my-app", Sets.newHashSet("read", "write"), stored);
        assertThat(readWrite.getApplication(), equalTo("my-app"));
        assertThat(readWrite.name(), containsInAnyOrder("read", "write"));
        assertThat(readWrite.getPatterns(), arrayContainingInAnyOrder("data:read/*", "data:write/*", "action:login"));
        assertThat(readWrite.getMetadata().entrySet(), empty());

        CharacterRunAutomaton run = new CharacterRunAutomaton(readWrite.getAutomaton());
        for (String action : Arrays.asList("data:read/settings", "data:write/user/kimchy", "action:login")) {
            assertTrue(run.run(action));
        }
        for (String action : Arrays.asList("data:delete/user/kimchy", "action:shutdown")) {
            assertFalse(run.run(action));
        }
    }

    public void testEqualsAndHashCode() {
        final ApplicationPrivilege privilege = randomPrivilege();
        final EqualsHashCodeTestUtils.MutateFunction<ApplicationPrivilege> mutate = randomFrom(
            orig -> new ApplicationPrivilege(
                "x" + orig.getApplication(), orig.getPrivilegeName(), asList(orig.getPatterns()), orig.getMetadata()),
            orig -> new ApplicationPrivilege(
                orig.getApplication(), "x" + orig.getPrivilegeName(), asList(orig.getPatterns()), orig.getMetadata()),
            orig -> new ApplicationPrivilege(
                orig.getApplication(), orig.getPrivilegeName(), Collections.singleton("*"), orig.getMetadata()),
            orig -> new ApplicationPrivilege(
                orig.getApplication(), orig.getPrivilegeName(), asList(orig.getPatterns()), Collections.singletonMap("clone", "yes"))
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(privilege,
            original -> new ApplicationPrivilege(
                original.getApplication(), original.getPrivilegeName(), asList(original.getPatterns()), original.getMetadata()),
            mutate
        );
    }

    public void testSerialization() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            final ApplicationPrivilege original = randomPrivilege();
            original.writeTo(out);
            final ApplicationPrivilege clone = ApplicationPrivilege.readFrom(out.bytes().streamInput());
            assertThat(clone, Matchers.equalTo(original));
            assertThat(original, Matchers.equalTo(clone));
        }
    }

    public void testXContentGenerationAndParsing() throws IOException {
        final boolean includeTypeField = randomBoolean();

        final XContent xContent = randomFrom(XContentType.values()).xContent();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final XContentBuilder builder = new XContentBuilder(xContent, out);

            final ApplicationPrivilege original = randomPrivilege();
            if (includeTypeField) {
                original.toIndexContent(builder);
            } else {
                original.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            builder.flush();

            final byte[] bytes = out.toByteArray();
            try (XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY, THROW_UNSUPPORTED_OPERATION, bytes)) {
                final ApplicationPrivilege clone = ApplicationPrivilege.parse(parser,
                    randomBoolean() ? randomAlphaOfLength(3) : null,
                    randomBoolean() ? randomAlphaOfLength(3) : null,
                    includeTypeField);
                assertThat(clone, Matchers.equalTo(original));
                assertThat(original, Matchers.equalTo(clone));
            }
        }
    }

    public void testParseXContentWithDefaultNames() throws IOException {
        final String json = "{ \"actions\": [ \"data:read\" ], \"metadata\" : { \"num\": 1, \"bool\":false } }";
        final XContent xContent = XContentType.JSON.xContent();
        try (XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY, THROW_UNSUPPORTED_OPERATION, json)) {
            final ApplicationPrivilege privilege = ApplicationPrivilege.parse(parser, "my_app", "read", false);
            assertThat(privilege.getApplication(), equalTo("my_app"));
            assertThat(privilege.getPrivilegeName(), equalTo("read"));
            assertThat(privilege.getPatterns(), equalTo(new String[] { "data:read" }));
            assertThat(privilege.getMetadata().entrySet(), iterableWithSize(2));
            assertThat(privilege.getMetadata().get("num"), equalTo(1));
            assertThat(privilege.getMetadata().get("bool"), equalTo(false));
        }
    }

    public void testParseXContentWithoutUsingDefaultNames() throws IOException {
        final String json = "{" +
            "  \"application\": \"your_app\"," +
            "  \"name\": \"write\"," +
            "  \"actions\": [ \"data:write\" ]" +
            "}";
        final XContent xContent = XContentType.JSON.xContent();
        try (XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY, THROW_UNSUPPORTED_OPERATION, json)) {
            final ApplicationPrivilege privilege = ApplicationPrivilege.parse(parser, "my_app", "read", false);
            assertThat(privilege.getApplication(), equalTo("your_app"));
            assertThat(privilege.getPrivilegeName(), equalTo("write"));
            assertThat(privilege.getPatterns(), equalTo(new String[] { "data:write" }));
            assertThat(privilege.getMetadata().entrySet(), iterableWithSize(0));
        }
    }

    private void assertValidationFailure(String messageContent, Supplier<ApplicationPrivilege> supplier) {
        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, supplier::get);
        assertThat(exception.getMessage(), containsString(messageContent));
    }

    private ApplicationPrivilege randomPrivilege() {
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
        return new ApplicationPrivilege(applicationName, privilegeName, asList(patterns), metadata);
    }

}
