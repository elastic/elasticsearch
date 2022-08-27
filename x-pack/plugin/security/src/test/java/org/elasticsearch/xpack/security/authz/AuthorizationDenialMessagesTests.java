/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthorizationDenialMessagesTests extends ESTestCase {

    public void testNoRolesDescriptionIfSubjectIsNotAUser() {
        final Authentication authentication = randomFrom(
            AuthenticationTestHelper.builder().apiKey().build(),
            AuthenticationTestHelper.builder().serviceAccount().build()
        );

        assertThat(
            AuthorizationDenialMessages.rolesDescription(authentication.getEffectiveSubject(), mock(AuthorizationInfo.class)),
            equalTo("")
        );
    }

    public void testRolesDescriptionWithNullAuthorizationInfo() {
        // random 0 - 3 uniquely named roles
        final List<String> declaredRoleNames = IntStream.range(0, randomIntBetween(0, 3))
            .mapToObj(i -> randomAlphaOfLengthBetween(3, 8) + i)
            .toList();
        final Subject subject = AuthenticationTestHelper.builder()
            .realm()
            .user(new User(randomAlphaOfLengthBetween(3, 8), declaredRoleNames.toArray(String[]::new)))
            .build(false)
            .getEffectiveSubject();
        final String rolesDescription = AuthorizationDenialMessages.rolesDescription(subject, null);

        if (declaredRoleNames.isEmpty()) {
            assertThat(rolesDescription, equalTo(" with no declared roles"));
        } else {
            assertThat(
                rolesDescription,
                equalTo(" with declared roles [" + Strings.collectionToCommaDelimitedString(declaredRoleNames) + "]")
            );
        }
    }

    public void testRoleDescriptionWithEmptyResolvedRole() {
        // random 0 - 3 uniquely named roles
        final List<String> declaredRoleNames = IntStream.range(0, randomIntBetween(0, 3))
            .mapToObj(i -> randomAlphaOfLengthBetween(3, 8) + i)
            .toList();
        final Subject subject = AuthenticationTestHelper.builder()
            .realm()
            .user(new User(randomAlphaOfLengthBetween(3, 8), declaredRoleNames.toArray(String[]::new)))
            .build(false)
            .getEffectiveSubject();

        final RBACEngine.RBACAuthorizationInfo rbacAuthorizationInfo = mock(RBACEngine.RBACAuthorizationInfo.class);
        when(rbacAuthorizationInfo.getRole()).thenReturn(Role.EMPTY);
        final String rolesDescription = AuthorizationDenialMessages.rolesDescription(subject, rbacAuthorizationInfo);

        if (declaredRoleNames.isEmpty()) {
            assertThat(rolesDescription, equalTo(" with no declared roles"));
        } else {
            assertThat(
                rolesDescription,
                equalTo(" with declared roles [" + Strings.collectionToCommaDelimitedString(declaredRoleNames) + "] (none resolved)")
            );
        }
    }

    public void testRoleDescriptionWithEmptyDeclaredRoleAndAdditionalRole() {
        final Subject subject = AuthenticationTestHelper.builder()
            .realm()
            .user(new User(randomAlphaOfLengthBetween(3, 8)))
            .build(false)
            .getEffectiveSubject();

        final List<String> additionalRoleNames = randomList(1, 2, () -> randomAlphaOfLength(10)).stream().sorted().toList();
        final AuthorizationInfo authorizationInfo = mock(AuthorizationInfo.class);
        when(authorizationInfo.asMap()).thenReturn(Map.of("user.roles", additionalRoleNames.toArray(String[]::new)));

        final String rolesDescription = AuthorizationDenialMessages.rolesDescription(subject, authorizationInfo);

        assertThat(
            rolesDescription,
            equalTo(
                " with no declared roles (additionally resolved [%s])".formatted(
                    Strings.collectionToCommaDelimitedString(additionalRoleNames)
                )
            )
        );
    }

    public void testRoleDescriptionAllResolved() {
        // random 1 - 3 uniquely named roles
        final List<String> declaredRoleNames = IntStream.range(0, randomIntBetween(1, 3))
            .mapToObj(i -> randomAlphaOfLengthBetween(3, 8) + i)
            .toList();
        final Subject subject = AuthenticationTestHelper.builder()
            .realm()
            .user(new User(randomAlphaOfLengthBetween(3, 8), declaredRoleNames.toArray(String[]::new)))
            .build(false)
            .getEffectiveSubject();

        final List<String> additionalRoleNames = randomList(2, () -> randomAlphaOfLength(10)).stream().sorted().toList();
        final List<String> resolvedRoleNames = Stream.concat(declaredRoleNames.stream(), additionalRoleNames.stream()).toList();

        final AuthorizationInfo authorizationInfo = mock(AuthorizationInfo.class);
        when(authorizationInfo.asMap()).thenReturn(Map.of("user.roles", resolvedRoleNames.toArray(String[]::new)));

        final String rolesDescription = AuthorizationDenialMessages.rolesDescription(subject, authorizationInfo);

        if (additionalRoleNames.isEmpty()) {
            assertThat(
                rolesDescription,
                equalTo(" with declared roles [" + Strings.collectionToCommaDelimitedString(declaredRoleNames) + "] (all resolved)")
            );
        } else {
            assertThat(
                rolesDescription,
                equalTo(
                    " with declared roles ["
                        + Strings.collectionToCommaDelimitedString(declaredRoleNames)
                        + "] (all resolved, additionally resolved ["
                        + Strings.collectionToCommaDelimitedString(additionalRoleNames)
                        + "])"
                )
            );
        }
    }

    public void testRoleDescriptionWithUnresolvedRoles() {
        // random 1 - 3 uniquely named roles
        final List<String> declaredRoleNames = IntStream.range(0, randomIntBetween(1, 3))
            .mapToObj(i -> randomAlphaOfLengthBetween(3, 8) + i)
            .toList();
        final Subject subject = AuthenticationTestHelper.builder()
            .realm()
            .user(new User(randomAlphaOfLengthBetween(3, 8), declaredRoleNames.toArray(String[]::new)))
            .build(false)
            .getEffectiveSubject();

        final List<String> unresolvedRoleNames = randomSubsetOf(randomIntBetween(1, declaredRoleNames.size()), declaredRoleNames).stream()
            .sorted()
            .toList();
        final List<String> additionalRoleNames = randomList(2, () -> randomAlphaOfLength(10)).stream().sorted().toList();

        final List<String> resolvedRoleNames = Stream.concat(
            declaredRoleNames.stream().filter(name -> false == unresolvedRoleNames.contains(name)),
            additionalRoleNames.stream()
        ).toList();

        final AuthorizationInfo authorizationInfo = mock(AuthorizationInfo.class);
        when(authorizationInfo.asMap()).thenReturn(Map.of("user.roles", resolvedRoleNames.toArray(String[]::new)));

        final String rolesDescription = AuthorizationDenialMessages.rolesDescription(subject, authorizationInfo);

        final String messagePart1 = " with declared roles [%s]".formatted(Strings.collectionToCommaDelimitedString(declaredRoleNames));
        final String messagePart2 = unresolvedRoleNames.size() == declaredRoleNames.size()
            ? "none resolved"
            : "unresolved [%s]".formatted(Strings.collectionToCommaDelimitedString(unresolvedRoleNames));
        final String messagePart3 = additionalRoleNames.isEmpty()
            ? ""
            : ", additionally resolved [%s]".formatted(Strings.collectionToCommaDelimitedString(additionalRoleNames));

        assertThat(rolesDescription, equalTo(messagePart1 + " (" + messagePart2 + messagePart3 + ")"));
    }
}
