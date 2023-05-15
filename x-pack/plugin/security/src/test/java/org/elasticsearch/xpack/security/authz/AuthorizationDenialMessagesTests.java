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
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.List;
import java.util.Locale;
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
            equalTo(Strings.format(" with assigned roles [%s]", Strings.collectionToCommaDelimitedString(assignedRoleNames)))
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
            equalTo(Strings.format(" with assigned roles [%s]", Strings.collectionToCommaDelimitedString(assignedRoleNames)))
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
            equalTo(Strings.format(" with assigned roles [%s]", Strings.collectionToCommaDelimitedString(assignedRoleNames)))
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
                    Strings.format(
                        " with effective roles [] (assigned roles [%s] were not found)",
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
                Strings.format(
                    " with effective roles [%s]",
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
                String.format(
                    Locale.ROOT,
                    " with effective roles [%s] (assigned roles [%s] were not found)",
                    Strings.collectionToCommaDelimitedString(effectiveRoleNames.stream().sorted().toList()),
                    Strings.collectionToCommaDelimitedString(unfoundedRoleNames.stream().sorted().toList())
                )
            )
        );
    }

    public void testActionDeniedForCrossClusterAccessAuthentication() {
        final var crossClusterAccessSubjectInfo = AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo();
        final Authentication authentication = AuthenticationTestHelper.builder()
            .crossClusterAccess(randomAlphaOfLength(42), crossClusterAccessSubjectInfo)
            .build();
        final Authentication innerAuthentication = (Authentication) authentication.getAuthenticatingSubject()
            .getMetadata()
            .get(AuthenticationField.CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY);
        final String action = "indices:/some/action/" + randomAlphaOfLengthBetween(0, 8);
        assertThat(
            AuthorizationDenialMessages.actionDenied(authentication, null, action, mock(), null),
            equalTo(
                Strings.format(
                    "action [%s] towards remote cluster is unauthorized for %s authenticated by API key id [%s] of user [%s], "
                        + "this action is granted by the index privileges [all]",
                    action,
                    AuthorizationDenialMessages.successfulAuthenticationDescription(innerAuthentication, null),
                    authentication.getAuthenticatingSubject().getMetadata().get(AuthenticationField.API_KEY_ID_KEY),
                    authentication.getEffectiveSubject().getUser().principal()
                )
            )
        );
    }

    public void testSuccessfulAuthenticationDescription() {
        final Authentication authentication1 = AuthenticationTestHelper.builder().realm().build(false);
        assertThat(
            AuthorizationDenialMessages.successfulAuthenticationDescription(authentication1, null),
            equalTo(
                Strings.format(
                    "user [%s] with assigned roles [%s]",
                    authentication1.getEffectiveSubject().getUser().principal(),
                    Strings.arrayToCommaDelimitedString(authentication1.getEffectiveSubject().getUser().roles())
                )
            )
        );

        final Authentication authentication2 = AuthenticationTestHelper.builder().realm().runAs().build();
        assertThat(
            AuthorizationDenialMessages.successfulAuthenticationDescription(authentication2, null),
            equalTo(
                Strings.format(
                    "user [%s] run as [%s] with assigned roles [%s]",
                    authentication2.getAuthenticatingSubject().getUser().principal(),
                    authentication2.getEffectiveSubject().getUser().principal(),
                    Strings.arrayToCommaDelimitedString(authentication2.getEffectiveSubject().getUser().roles())
                )
            )
        );

        final Authentication authentication3 = AuthenticationTestHelper.builder().apiKey().build();
        assertThat(
            AuthorizationDenialMessages.successfulAuthenticationDescription(authentication3, null),
            equalTo(
                Strings.format(
                    "API key id [%s] of user [%s]",
                    authentication3.getEffectiveSubject().getMetadata().get(AuthenticationField.API_KEY_ID_KEY),
                    authentication3.getEffectiveSubject().getUser().principal()
                )
            )
        );

        final Authentication authentication4 = AuthenticationTestHelper.builder().serviceAccount().build();
        assertThat(
            AuthorizationDenialMessages.successfulAuthenticationDescription(authentication4, null),
            equalTo(Strings.format("service account [%s]", authentication4.getEffectiveSubject().getUser().principal()))
        );

        final var crossClusterAccessSubjectInfo = AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo();
        final Authentication authentication5 = AuthenticationTestHelper.builder()
            .crossClusterAccess(randomAlphaOfLength(42), crossClusterAccessSubjectInfo)
            .build();
        final Authentication innerAuthentication = (Authentication) authentication5.getAuthenticatingSubject()
            .getMetadata()
            .get(AuthenticationField.CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY);
        assertThat(
            AuthorizationDenialMessages.successfulAuthenticationDescription(authentication5, null),
            equalTo(
                Strings.format(
                    "%s authenticated by API key id [%s] of user [%s]",
                    AuthorizationDenialMessages.successfulAuthenticationDescription(innerAuthentication, null),
                    authentication5.getAuthenticatingSubject().getMetadata().get(AuthenticationField.API_KEY_ID_KEY),
                    authentication5.getEffectiveSubject().getUser().principal()
                )
            )
        );
    }

    public void testRemoteActionDenied() {
        final Authentication authentication = AuthenticationTestHelper.builder().build();
        final String action = "indices:/some/action/" + randomAlphaOfLengthBetween(0, 8);
        final String clusterAlias = randomAlphaOfLengthBetween(5, 12);
        assertThat(
            AuthorizationDenialMessages.remoteActionDenied(authentication, null, action, clusterAlias),
            equalTo(
                Strings.format(
                    "action [%s] towards remote cluster [%s] is unauthorized for %s"
                        + " because no remote indices privileges apply for the target cluster",
                    action,
                    clusterAlias,
                    AuthorizationDenialMessages.successfulAuthenticationDescription(authentication, null)
                )
            )
        );
    }
}
