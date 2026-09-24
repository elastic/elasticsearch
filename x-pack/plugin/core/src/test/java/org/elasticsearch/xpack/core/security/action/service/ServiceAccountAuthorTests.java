/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ServiceAccountAuthorTests extends AbstractWireSerializingTestCase<ServiceAccountAuthor> {

    @Override
    protected Writeable.Reader<ServiceAccountAuthor> instanceReader() {
        return ServiceAccountAuthor::readFrom;
    }

    @Override
    protected ServiceAccountAuthor createTestInstance() {
        return randomAuthor();
    }

    @Override
    protected ServiceAccountAuthor mutateInstance(ServiceAccountAuthor instance) {
        return switch (between(0, 5)) {
            case 0 -> new ServiceAccountAuthor(
                randomValueOtherThan(instance.principal(), () -> randomAlphaOfLengthBetween(3, 8)),
                instance.fullName(),
                instance.email(),
                instance.realm(),
                instance.realmType(),
                instance.realmDomain()
            );
            case 1 -> new ServiceAccountAuthor(
                instance.principal(),
                randomValueOtherThan(instance.fullName(), ServiceAccountAuthorTests::randomOptionalName),
                instance.email(),
                instance.realm(),
                instance.realmType(),
                instance.realmDomain()
            );
            case 2 -> new ServiceAccountAuthor(
                instance.principal(),
                instance.fullName(),
                randomValueOtherThan(instance.email(), ServiceAccountAuthorTests::randomOptionalName),
                instance.realm(),
                instance.realmType(),
                instance.realmDomain()
            );
            case 3 -> new ServiceAccountAuthor(
                instance.principal(),
                instance.fullName(),
                instance.email(),
                randomValueOtherThan(instance.realm(), () -> randomAlphaOfLengthBetween(3, 8)),
                instance.realmType(),
                instance.realmDomain()
            );
            case 4 -> new ServiceAccountAuthor(
                instance.principal(),
                instance.fullName(),
                instance.email(),
                instance.realm(),
                randomValueOtherThan(instance.realmType(), () -> randomAlphaOfLengthBetween(3, 8)),
                instance.realmDomain()
            );
            case 5 -> new ServiceAccountAuthor(
                instance.principal(),
                instance.fullName(),
                instance.email(),
                instance.realm(),
                instance.realmType(),
                randomValueOtherThan(instance.realmDomain(), ServiceAccountAuthorTests::randomOptionalDomain)
            );
            default -> throw new AssertionError("unreachable");
        };
    }

    /**
     * The author is the effective subject, so a request run as another user is attributed to that user rather
     * than to the one who authenticated, as an API key's creator is.
     */
    public void testFromAuthenticationRecordsTheEffectiveSubject() {
        final Authentication authentication = AuthenticationTestHelper.builder().realm().build(randomBoolean());
        final ServiceAccountAuthor author = ServiceAccountAuthor.fromAuthentication(authentication);
        final User user = authentication.getEffectiveSubject().getUser();
        final Authentication.RealmRef realm = authentication.getEffectiveSubject().getRealm();
        assertThat(author.principal(), equalTo(user.principal()));
        assertThat(author.fullName(), equalTo(user.fullName()));
        assertThat(author.email(), equalTo(user.email()));
        assertThat(author.realm(), equalTo(realm.getName()));
        assertThat(author.realmType(), equalTo(realm.getType()));
        assertThat(author.realmDomain(), equalTo(realm.getDomain()));
        if (authentication.isRunAs()) {
            assertThat(author.principal(), not(equalTo(authentication.getAuthenticatingSubject().getUser().principal())));
        }
    }

    public void testFromAuthenticationRecordsTheOwnerOfAnApiKey() {
        final Authentication authentication = AuthenticationTestHelper.builder().apiKey().build(false);
        final ServiceAccountAuthor author = ServiceAccountAuthor.fromAuthentication(authentication);
        assertThat(author.principal(), equalTo(authentication.getEffectiveSubject().getUser().principal()));
        assertThat(author.realm(), equalTo(authentication.getEffectiveSubject().getRealm().getName()));
        assertThat(author.realmType(), equalTo(authentication.getEffectiveSubject().getRealm().getType()));
    }

    /**
     * Absent fields are left out rather than written as {@code null}, and the domain is reduced to its name.
     */
    public void testToXContentLeavesOutAbsentFieldsAndNamesTheDomain() throws IOException {
        final RealmDomain domain = AuthenticationTestHelper.randomDomain(randomBoolean());
        final ServiceAccountAuthor author = new ServiceAccountAuthor("alice", "Alice", "alice@example.com", "ldap1", "ldap", domain);
        assertThat(
            toMap(author),
            equalTo(
                Map.of(
                    "principal",
                    "alice",
                    "full_name",
                    "Alice",
                    "email",
                    "alice@example.com",
                    "realm",
                    "ldap1",
                    "realm_type",
                    "ldap",
                    "realm_domain",
                    domain.name()
                )
            )
        );

        final ServiceAccountAuthor bare = new ServiceAccountAuthor("alice", null, null, "ldap1", "ldap", null);
        assertThat(toMap(bare), equalTo(Map.of("principal", "alice", "realm", "ldap1", "realm_type", "ldap")));
        assertThat(toMap(bare), not(hasKey("full_name")));
        assertThat(toMap(bare), not(hasKey("realm_domain")));
        assertThat(bare.realmDomain(), nullValue());
    }

    public void testTheAuthorRequiresAPrincipalAndARealm() {
        expectThrows(NullPointerException.class, () -> new ServiceAccountAuthor(null, null, null, "realm", "type", null));
        expectThrows(NullPointerException.class, () -> new ServiceAccountAuthor("alice", null, null, null, "type", null));
        expectThrows(NullPointerException.class, () -> new ServiceAccountAuthor("alice", null, null, "realm", null, null));
    }

    static ServiceAccountAuthor randomAuthor() {
        return new ServiceAccountAuthor(
            randomAlphaOfLengthBetween(3, 8),
            randomOptionalName(),
            randomOptionalName(),
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8),
            randomOptionalDomain()
        );
    }

    private static String randomOptionalName() {
        return randomBoolean() ? null : randomAlphaOfLengthBetween(3, 12);
    }

    private static RealmDomain randomOptionalDomain() {
        return randomBoolean() ? null : AuthenticationTestHelper.randomDomain(randomBoolean());
    }

    private static Map<String, Object> toMap(ServiceAccountAuthor author) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        author.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
    }
}
