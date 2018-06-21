/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.Strings.collectionToCommaDelimitedString;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ApplicationPrivilegeTests extends ESTestCase {

    public void testValidationOfApplicationName() {
        // too short
        assertValidationFailure("Application names", () -> ApplicationPrivilege.validateApplicationName("ap"));
        // must start with lowercase
        assertValidationFailure("Application names", () -> ApplicationPrivilege.validateApplicationName("App"));
        // must start with letter
        assertValidationFailure("Application names", () -> ApplicationPrivilege.validateApplicationName("1app"));
        // cannot contain special characters
        assertValidationFailure("Application names",
            () -> ApplicationPrivilege.validateApplicationName("app" + randomFrom(":;$#%()+=/'.,".toCharArray())));

        // no wildcards
        assertValidationFailure("Application names", () -> ApplicationPrivilege.validateApplicationName("app*"));
        // no special characters with wildcards
        assertValidationFailure("Application names",
            () -> ApplicationPrivilege.validateApplicationNameOrWildcard("app" + randomFrom((":;$#%()+=/'.,").toCharArray()) + "*"));

        // these should all be OK
        assertNoException(() -> ApplicationPrivilege.validateApplicationName("app"));
        assertNoException(() -> ApplicationPrivilege.validateApplicationName("app1"));
        assertNoException(() -> ApplicationPrivilege.validateApplicationName("myApp"));
        assertNoException(() -> ApplicationPrivilege.validateApplicationName("my-App"));
        assertNoException(() -> ApplicationPrivilege.validateApplicationName("my_App"));
        assertNoException(() -> ApplicationPrivilege.validateApplicationNameOrWildcard("app*"));
    }

    public void testValidationOfPrivilegeName() {
        // must start with lowercase
        assertValidationFailure("privilege names", () -> ApplicationPrivilege.validatePrivilegeName("Read"));
        // must start with letter
        assertValidationFailure("privilege names", () -> ApplicationPrivilege.validatePrivilegeName("1read"));
        // cannot contain special characters
        final String withSpecialChar = "read" + randomFrom(":;$#%()+=/',".toCharArray());
        assertValidationFailure("privilege names", () -> ApplicationPrivilege.validatePrivilegeName(withSpecialChar));

        // these should all be OK
        assertNoException(() -> ApplicationPrivilege.validatePrivilegeName("read"));
        assertNoException(() -> ApplicationPrivilege.validatePrivilegeName("read1"));
        assertNoException(() -> ApplicationPrivilege.validatePrivilegeName("readData"));
        assertNoException(() -> ApplicationPrivilege.validatePrivilegeName("read-data"));
        assertNoException(() -> ApplicationPrivilege.validatePrivilegeName("read.data"));
        assertNoException(() -> ApplicationPrivilege.validatePrivilegeName("read_data"));

        assertValidationFailure("privilege names and action", () -> ApplicationPrivilege.validatePrivilegeOrActionName("r e a d"));
        assertValidationFailure("privilege names and action", () -> ApplicationPrivilege.validatePrivilegeOrActionName("read\n"));
        assertValidationFailure("privilege names and action", () -> ApplicationPrivilege.validatePrivilegeOrActionName("copy®"));

        assertNoException(() -> ApplicationPrivilege.validatePrivilegeOrActionName("read"));
        assertNoException(() -> ApplicationPrivilege.validatePrivilegeOrActionName("read1"));
        assertNoException(() -> ApplicationPrivilege.validatePrivilegeOrActionName("readData"));
        assertNoException(() -> ApplicationPrivilege.validatePrivilegeOrActionName("read-data"));
        assertNoException(() -> ApplicationPrivilege.validatePrivilegeOrActionName("read.data"));
        assertNoException(() -> ApplicationPrivilege.validatePrivilegeOrActionName("read_data"));
        assertNoException(() -> ApplicationPrivilege.validatePrivilegeOrActionName("read:*"));
        assertNoException(() -> ApplicationPrivilege.validatePrivilegeOrActionName("read/*"));
        assertNoException(() -> ApplicationPrivilege.validatePrivilegeOrActionName("read/a_b.c-d+e%f#(g)"));

    }

    public void testGetPrivilegeByName() {
        final ApplicationPrivilegeDescriptor descriptor = descriptor("my-app", "read", "data:read/*", "action:login");
        final ApplicationPrivilegeDescriptor myWrite = descriptor("my-app", "write", "data:write/*", "action:login");
        final ApplicationPrivilegeDescriptor myAdmin = descriptor("my-app", "admin", "data:read/*", "action:*");
        final ApplicationPrivilegeDescriptor yourRead = descriptor("your-app", "read", "data:read/*", "action:login");
        final Set<ApplicationPrivilegeDescriptor> stored = Sets.newHashSet(descriptor, myWrite, myAdmin, yourRead);

        assertEqual(ApplicationPrivilege.get("my-app", Collections.singleton("read"), stored), descriptor);
        assertEqual(ApplicationPrivilege.get("my-app", Collections.singleton("write"), stored), myWrite);

        final ApplicationPrivilege readWrite = ApplicationPrivilege.get("my-app", Sets.newHashSet("read", "write"), stored);
        assertThat(readWrite.getApplication(), equalTo("my-app"));
        assertThat(readWrite.name(), containsInAnyOrder("read", "write"));
        assertThat(readWrite.getPatterns(), arrayContainingInAnyOrder("data:read/*", "data:write/*", "action:login"));

        CharacterRunAutomaton run = new CharacterRunAutomaton(readWrite.getAutomaton());
        for (String action : Arrays.asList("data:read/settings", "data:write/user/kimchy", "action:login")) {
            assertTrue(run.run(action));
        }
        for (String action : Arrays.asList("data:delete/user/kimchy", "action:shutdown")) {
            assertFalse(run.run(action));
        }
    }

    private void assertEqual(ApplicationPrivilege myReadPriv, ApplicationPrivilegeDescriptor myRead) {
        assertThat(myReadPriv.getApplication(), equalTo(myRead.getApplication()));
        assertThat(getPrivilegeName(myReadPriv), equalTo(myRead.getName()));
        assertThat(Sets.newHashSet(myReadPriv.getPatterns()), equalTo(myRead.getActions()));
    }

    private ApplicationPrivilegeDescriptor descriptor(String application, String name, String... actions) {
        return new ApplicationPrivilegeDescriptor(application, name, Sets.newHashSet(actions), Collections.emptyMap());
    }

    public void testEqualsAndHashCode() {
        final ApplicationPrivilege privilege = randomPrivilege();
        final EqualsHashCodeTestUtils.MutateFunction<ApplicationPrivilege> mutate = randomFrom(
            orig -> createPrivilege("x" + orig.getApplication(), getPrivilegeName(orig), orig.getPatterns()),
            orig -> createPrivilege(orig.getApplication(), "x" + getPrivilegeName(orig), orig.getPatterns()),
            orig -> new ApplicationPrivilege(orig.getApplication(), getPrivilegeName(orig), "*")
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(privilege,
            original -> createPrivilege(original.getApplication(), getPrivilegeName(original), original.getPatterns()),
            mutate
        );
    }

    private ApplicationPrivilege createPrivilege(String applicationName, String privilegeName, String... patterns) {
        return new ApplicationPrivilege(applicationName, privilegeName, patterns);
    }

    private String getPrivilegeName(ApplicationPrivilege privilege) {
        if (privilege.name.size() == 1) {
            return privilege.name.iterator().next();
        } else {
            throw new IllegalStateException(privilege + " has a multivariate name: " + collectionToCommaDelimitedString(privilege.name));
        }
    }

    private void assertValidationFailure(String messageContent, ThrowingRunnable body) {
        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, body);
        assertThat(exception.getMessage(), containsString(messageContent));
    }

    private void assertNoException(ThrowingRunnable body) {
        try {
            body.run();
            // pass
        } catch (Throwable e) {
            Assert.fail("Expected no exception, but got: " + e);
        }
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
        return createPrivilege(applicationName, privilegeName, patterns);
    }

}
