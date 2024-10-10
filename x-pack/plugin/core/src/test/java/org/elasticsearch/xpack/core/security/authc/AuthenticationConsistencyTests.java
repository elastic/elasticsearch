/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static org.hamcrest.Matchers.equalTo;

public class AuthenticationConsistencyTests extends ESTestCase {

    public void testCheckConsistency() throws IOException {
        getErrorMessageToEncodedAuthentication().forEach((errorMessage, encodedAuthentication) -> {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                "expected [" + errorMessage + "]",
                () -> AuthenticationContextSerializer.decode(encodedAuthentication)
            );
            assertThat(e.getMessage(), equalTo(errorMessage));
        });
    }

    private Map<String, String> getErrorMessageToEncodedAuthentication() throws IOException {
        final User userFoo = new User("foo");
        final User userBar = new User("bar");
        final Authentication.RealmRef realm1 = new Authentication.RealmRef("realm_1", "realm_1", "node");
        final Authentication.RealmRef realm2 = new Authentication.RealmRef("realm_2", "realm_2", "node");

        return Map.ofEntries(
            // Authentication type: anonymous
            entry(
                "Anonymous authentication cannot have realm type [realm_1]",
                encodeAuthentication(new Subject(userFoo, realm1), Authentication.AuthenticationType.ANONYMOUS)
            ),
            entry(
                "Anonymous authentication cannot have domain",
                encodeAuthentication(
                    new Subject(
                        userFoo,
                        new Authentication.RealmRef(
                            AuthenticationField.ANONYMOUS_REALM_NAME,
                            AuthenticationField.ANONYMOUS_REALM_TYPE,
                            "node",
                            new RealmDomain(
                                "domain1",
                                Set.of(
                                    new RealmConfig.RealmIdentifier(
                                        AuthenticationField.ANONYMOUS_REALM_TYPE,
                                        AuthenticationField.ANONYMOUS_REALM_NAME
                                    )
                                )
                            )
                        )
                    ),
                    Authentication.AuthenticationType.ANONYMOUS
                )
            ),
            entry(
                "Anonymous authentication cannot have internal user [_xpack]",
                encodeAuthentication(
                    new Subject(InternalUsers.XPACK_USER, Authentication.RealmRef.newAnonymousRealmRef("node")),
                    Authentication.AuthenticationType.ANONYMOUS
                )
            ),
            entry(
                "Anonymous authentication cannot run-as other user",
                encodeAuthentication(
                    new Subject(userBar, realm2),
                    new Subject(userFoo, Authentication.RealmRef.newAnonymousRealmRef("node")),
                    Authentication.AuthenticationType.ANONYMOUS
                )
            ),
            // Authentication type: internal
            entry(
                "Internal authentication cannot have realm type [realm_1]",
                encodeAuthentication(new Subject(userFoo, realm1), Authentication.AuthenticationType.INTERNAL)
            ),
            entry(
                "Internal authentication cannot have domain",
                encodeAuthentication(
                    new Subject(
                        userFoo,
                        new Authentication.RealmRef(
                            AuthenticationField.FALLBACK_REALM_NAME,
                            AuthenticationField.FALLBACK_REALM_TYPE,
                            "node",
                            new RealmDomain(
                                "domain1",
                                Set.of(
                                    new RealmConfig.RealmIdentifier(
                                        AuthenticationField.FALLBACK_REALM_TYPE,
                                        AuthenticationField.FALLBACK_REALM_NAME
                                    )
                                )
                            )
                        )
                    ),
                    Authentication.AuthenticationType.INTERNAL
                )
            ),
            entry(
                "Internal authentication must have internal user",
                encodeAuthentication(
                    new Subject(userFoo, Authentication.RealmRef.newInternalAttachRealmRef("node")),
                    Authentication.AuthenticationType.INTERNAL
                )
            ),
            // Authentication type: API key
            entry(
                "API key authentication cannot have realm type [realm_1]",
                encodeAuthentication(new Subject(userFoo, realm1), Authentication.AuthenticationType.API_KEY)
            ),
            entry(
                "API key authentication cannot have domain",
                encodeAuthentication(
                    new Subject(
                        userFoo,
                        new Authentication.RealmRef(
                            AuthenticationField.API_KEY_REALM_NAME,
                            AuthenticationField.API_KEY_REALM_TYPE,
                            "node",
                            new RealmDomain(
                                "domain1",
                                Set.of(
                                    new RealmConfig.RealmIdentifier(
                                        AuthenticationField.API_KEY_REALM_TYPE,
                                        AuthenticationField.API_KEY_REALM_NAME
                                    )
                                )
                            )
                        )
                    ),
                    Authentication.AuthenticationType.API_KEY
                )
            ),
            entry(
                "API key authentication cannot have internal user [_xpack]",
                encodeAuthentication(
                    new Subject(InternalUsers.XPACK_USER, Authentication.RealmRef.newApiKeyRealmRef("node")),
                    Authentication.AuthenticationType.API_KEY
                )
            ),
            entry(
                "API key authentication user must have no role",
                encodeAuthentication(
                    new Subject(new User("foo", "role"), Authentication.RealmRef.newApiKeyRealmRef("node")),
                    Authentication.AuthenticationType.API_KEY
                )
            ),
            entry(
                "API key authentication requires metadata to contain a non-null API key ID",
                encodeAuthentication(
                    new Subject(userFoo, Authentication.RealmRef.newApiKeyRealmRef("node")),
                    Authentication.AuthenticationType.API_KEY
                )
            ),
            entry(
                "Cross cluster access authentication requires metadata to contain "
                    + "a non-null serialized cross cluster access authentication field",
                encodeAuthentication(
                    new Subject(
                        userFoo,
                        Authentication.RealmRef.newCrossClusterAccessRealmRef("node"),
                        TransportVersion.current(),
                        Map.of(AuthenticationField.API_KEY_ID_KEY, "abc")
                    ),
                    Authentication.AuthenticationType.API_KEY
                )
            ),
            entry(
                "Cross cluster access authentication cannot contain another cross cluster access authentication in its metadata",
                encodeAuthentication(
                    new Subject(
                        userFoo,
                        Authentication.RealmRef.newCrossClusterAccessRealmRef("node"),
                        TransportVersion.current(),
                        Map.of(
                            AuthenticationField.API_KEY_ID_KEY,
                            "abc",
                            AuthenticationField.CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY,
                            AuthenticationTestHelper.builder().crossClusterAccess().build()
                        )
                    ),
                    Authentication.AuthenticationType.API_KEY
                )
            ),
            entry(
                "Cross cluster access authentication requires metadata to contain "
                    + "a non-null serialized cross cluster access role descriptors field",
                encodeAuthentication(
                    new Subject(
                        userFoo,
                        Authentication.RealmRef.newCrossClusterAccessRealmRef("node"),
                        TransportVersion.current(),
                        Map.of(
                            AuthenticationField.API_KEY_ID_KEY,
                            "abc",
                            AuthenticationField.CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY,
                            Authentication.newRealmAuthentication(userBar, realm2)
                        )
                    ),
                    Authentication.AuthenticationType.API_KEY
                )
            ),
            entry(
                "Cross cluster access authentication cannot run-as other user",
                encodeAuthentication(
                    new Subject(userBar, realm2),
                    new Subject(
                        userFoo,
                        Authentication.RealmRef.newCrossClusterAccessRealmRef("node"),
                        TransportVersion.current(),
                        Map.of(
                            AuthenticationField.API_KEY_ID_KEY,
                            "abc",
                            AuthenticationField.CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY,
                            Authentication.newRealmAuthentication(userBar, realm2),
                            AuthenticationField.CROSS_CLUSTER_ACCESS_ROLE_DESCRIPTORS_KEY,
                            List.of()
                        )
                    ),
                    Authentication.AuthenticationType.API_KEY
                )
            ),
            // Authentication type: Realm
            entry(
                "Realm authentication must have subject type of user",
                encodeAuthentication(
                    new Subject(userFoo, Authentication.RealmRef.newApiKeyRealmRef("node")),
                    Authentication.AuthenticationType.REALM
                )
            ),
            // Authentication type: Token
            entry(
                "Token authentication cannot have internal user [_xpack]",
                encodeAuthentication(new Subject(InternalUsers.XPACK_USER, realm1), Authentication.AuthenticationType.TOKEN)
            ),
            entry(
                "Service account authentication cannot have domain",
                encodeAuthentication(
                    new Subject(
                        userFoo,
                        new Authentication.RealmRef(
                            ServiceAccountSettings.REALM_NAME,
                            ServiceAccountSettings.REALM_TYPE,
                            "node",
                            new RealmDomain(
                                "domain1",
                                Set.of(
                                    new RealmConfig.RealmIdentifier(ServiceAccountSettings.REALM_TYPE, ServiceAccountSettings.REALM_NAME)
                                )
                            )
                        )
                    ),
                    Authentication.AuthenticationType.TOKEN
                )
            ),
            entry(
                "Service account authentication user must have no role",
                encodeAuthentication(
                    new Subject(new User("foo", "role"), Authentication.RealmRef.newServiceAccountRealmRef("node")),
                    Authentication.AuthenticationType.TOKEN
                )
            ),
            entry(
                "Service account authentication cannot run-as other user",
                encodeAuthentication(
                    new Subject(userBar, realm2),
                    new Subject(userFoo, Authentication.RealmRef.newServiceAccountRealmRef("node")),
                    Authentication.AuthenticationType.TOKEN
                )
            ),
            entry(
                "API key token authentication cannot have domain",
                encodeAuthentication(
                    new Subject(
                        userFoo,
                        new Authentication.RealmRef(
                            AuthenticationField.API_KEY_REALM_NAME,
                            AuthenticationField.API_KEY_REALM_TYPE,
                            "node",
                            new RealmDomain(
                                "domain1",
                                Set.of(
                                    new RealmConfig.RealmIdentifier(
                                        AuthenticationField.API_KEY_REALM_TYPE,
                                        AuthenticationField.API_KEY_REALM_NAME
                                    )
                                )
                            )
                        )
                    ),
                    Authentication.AuthenticationType.TOKEN
                )
            ),
            entry(
                "API key token authentication user must have no role",
                encodeAuthentication(
                    new Subject(new User("foo", "role"), Authentication.RealmRef.newApiKeyRealmRef("node")),
                    Authentication.AuthenticationType.TOKEN
                )
            ),
            entry(
                "API key token authentication requires metadata to contain a non-null API key ID",
                encodeAuthentication(
                    new Subject(userFoo, Authentication.RealmRef.newApiKeyRealmRef("node")),
                    Authentication.AuthenticationType.TOKEN
                )
            ),
            // RunAs consistency
            entry(
                "Run-as subject type cannot be [API_KEY]",
                encodeAuthentication(
                    new Subject(userBar, Authentication.RealmRef.newApiKeyRealmRef("node")),
                    new Subject(userFoo, realm1),
                    Authentication.AuthenticationType.REALM
                )
            )
        );
    }

    private String encodeAuthentication(Subject effectiveSubject, Authentication.AuthenticationType type) throws IOException {
        return encodeAuthentication(effectiveSubject, effectiveSubject, type);
    }

    private String encodeAuthentication(Subject effectiveSubject, Subject authenticatingSubject, Authentication.AuthenticationType type)
        throws IOException {
        return Authentication.doEncode(effectiveSubject, authenticatingSubject, type);
    }
}
