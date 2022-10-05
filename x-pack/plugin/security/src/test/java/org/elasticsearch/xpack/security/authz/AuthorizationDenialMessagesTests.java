/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
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
        final List<String> assignedRoleNames = IntStream.range(0, randomIntBetween(0, 3))
            .mapToObj(i -> randomAlphaOfLengthBetween(3, 8) + i)
            .toList();
        final Subject subject = AuthenticationTestHelper.builder()
            .realm()
            .user(new User(randomAlphaOfLengthBetween(3, 8), assignedRoleNames.toArray(String[]::new)))
            .build(false)
            .getEffectiveSubject();
        final String rolesDescription = AuthorizationDenialMessages.rolesDescription(subject, null);

        assertThat(
            rolesDescription,
            equalTo(" with assigned roles [%s]".formatted(Strings.collectionToCommaDelimitedString(assignedRoleNames)))
        );
    }

    public void testRolesDescriptionWithNullRolesField() {
        // random 0 - 3 uniquely named roles
        final List<String> assignedRoleNames = IntStream.range(0, randomIntBetween(0, 3))
            .mapToObj(i -> randomAlphaOfLengthBetween(3, 8) + i)
            .toList();
        final Subject subject = AuthenticationTestHelper.builder()
            .realm()
            .user(new User(randomAlphaOfLengthBetween(3, 8), assignedRoleNames.toArray(String[]::new)))
            .build(false)
            .getEffectiveSubject();
        final AuthorizationInfo authorizationInfo = mock(AuthorizationInfo.class);
        when(authorizationInfo.asMap()).thenReturn(Map.of());
        final String rolesDescription = AuthorizationDenialMessages.rolesDescription(subject, authorizationInfo);

        assertThat(
            rolesDescription,
            equalTo(" with assigned roles [%s]".formatted(Strings.collectionToCommaDelimitedString(assignedRoleNames)))
        );
    }

    public void testRolesDescriptionWithIncompatibleRolesField() {
        // random 0 - 3 uniquely named roles
        final List<String> assignedRoleNames = IntStream.range(0, randomIntBetween(0, 3))
            .mapToObj(i -> randomAlphaOfLengthBetween(3, 8) + i)
            .toList();
        final Subject subject = AuthenticationTestHelper.builder()
            .realm()
            .user(new User(randomAlphaOfLengthBetween(3, 8), assignedRoleNames.toArray(String[]::new)))
            .build(false)
            .getEffectiveSubject();
        final AuthorizationInfo authorizationInfo = mock(AuthorizationInfo.class);
        when(authorizationInfo.asMap()).thenReturn(
            Map.of(
                "user.roles",
                randomFrom(
                    randomAlphaOfLength(8),
                    42,
                    randomBoolean(),
                    randomList(3, () -> randomAlphaOfLength(8)),
                    randomMap(0, 3, () -> Tuple.tuple(randomAlphaOfLength(5), randomAlphaOfLength(8)))
                )
            )
        );
        final String rolesDescription = AuthorizationDenialMessages.rolesDescription(subject, authorizationInfo);

        assertThat(
            rolesDescription,
            equalTo(" with assigned roles [%s]".formatted(Strings.collectionToCommaDelimitedString(assignedRoleNames)))
        );
    }

    public void testRoleDescriptionWithEmptyResolvedRole() {
        // random 0 - 3 uniquely named roles
        final List<String> assignedRoleNames = IntStream.range(0, randomIntBetween(0, 3))
            .mapToObj(i -> randomAlphaOfLengthBetween(3, 8) + i)
            .toList();
        final Subject subject = AuthenticationTestHelper.builder()
            .realm()
            .user(new User(randomAlphaOfLengthBetween(3, 8), assignedRoleNames.toArray(String[]::new)))
            .build(false)
            .getEffectiveSubject();

        final AuthorizationInfo authorizationInfo = mock(AuthorizationInfo.class);
        when(authorizationInfo.asMap()).thenReturn(Map.of("user.roles", Strings.EMPTY_ARRAY));
        final String rolesDescription = AuthorizationDenialMessages.rolesDescription(subject, authorizationInfo);

        if (assignedRoleNames.isEmpty()) {
            assertThat(rolesDescription, equalTo(" with effective roles []"));
        } else {
            assertThat(
                rolesDescription,
                equalTo(
                    " with effective roles [] (assigned roles [%s] were not found)".formatted(
                        Strings.collectionToCommaDelimitedString(assignedRoleNames.stream().sorted().toList())
                    )
                )
            );
        }
    }

    public void testRoleDescriptionAllResolvedAndMaybeWithAnonymousRoles() {
        // random 0 - 3 uniquely named roles
        final List<String> assignedRoleNames = IntStream.range(0, randomIntBetween(0, 3))
            .mapToObj(i -> randomAlphaOfLengthBetween(3, 8) + i)
            .toList();
        final Subject subject = AuthenticationTestHelper.builder()
            .realm()
            .user(new User(randomAlphaOfLengthBetween(3, 8), assignedRoleNames.toArray(String[]::new)))
            .build(false)
            .getEffectiveSubject();

        // 0 - 2 anonymous roles
        final List<String> anonymousRoleNames = randomList(2, () -> randomAlphaOfLength(10)).stream().toList();

        // all roles
        final List<String> effectiveRoleNames = Stream.concat(assignedRoleNames.stream(), anonymousRoleNames.stream()).toList();

        final AuthorizationInfo authorizationInfo = mock(AuthorizationInfo.class);
        when(authorizationInfo.asMap()).thenReturn(Map.of("user.roles", effectiveRoleNames.toArray(String[]::new)));

        final String rolesDescription = AuthorizationDenialMessages.rolesDescription(subject, authorizationInfo);

        assertThat(
            rolesDescription,
            equalTo(
                " with effective roles [%s]".formatted(
                    Strings.collectionToCommaDelimitedString(effectiveRoleNames.stream().sorted().toList())
                )
            )
        );
    }

    public void testRoleDescriptionWithUnresolvedRoles() {
        // random 1 - 3 uniquely named roles
        final List<String> assignedRoleNames = IntStream.range(0, randomIntBetween(1, 3))
            .mapToObj(i -> randomAlphaOfLengthBetween(3, 8) + i)
            .toList();
        final Subject subject = AuthenticationTestHelper.builder()
            .realm()
            .user(new User(randomAlphaOfLengthBetween(3, 8), assignedRoleNames.toArray(String[]::new)))
            .build(false)
            .getEffectiveSubject();

        final List<String> unfoundedRoleNames = randomSubsetOf(randomIntBetween(1, assignedRoleNames.size()), assignedRoleNames);
        final List<String> anonymousRoleNames = randomList(2, () -> randomAlphaOfLength(10)).stream().sorted().toList();

        final List<String> effectiveRoleNames = Stream.concat(
            assignedRoleNames.stream().filter(name -> false == unfoundedRoleNames.contains(name)),
            anonymousRoleNames.stream()
        ).toList();

        final AuthorizationInfo authorizationInfo = mock(AuthorizationInfo.class);
        when(authorizationInfo.asMap()).thenReturn(Map.of("user.roles", effectiveRoleNames.toArray(String[]::new)));

        final String rolesDescription = AuthorizationDenialMessages.rolesDescription(subject, authorizationInfo);

        assertThat(
            rolesDescription,
            equalTo(
                " with effective roles [%s] (assigned roles [%s] were not found)".formatted(
                    Strings.collectionToCommaDelimitedString(effectiveRoleNames.stream().sorted().toList()),
                    Strings.collectionToCommaDelimitedString(unfoundedRoleNames.stream().sorted().toList())
                )
            )
        );
    }
}
