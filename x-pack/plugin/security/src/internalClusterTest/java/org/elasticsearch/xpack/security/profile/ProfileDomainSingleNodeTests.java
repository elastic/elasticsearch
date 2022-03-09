/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationContext;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;

import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.INTERNAL_SECURITY_PROFILE_INDEX_8;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_PROFILE_ALIAS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ProfileDomainSingleNodeTests extends AbstractProfileSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings());
        // Register both file and native realms under the same domain
        builder.put("xpack.security.authc.domains.my_domain.realms", "file,index");
        return builder.build();
    }

    public void testActivateProfileUnderDomain() {
        // Activate 1st time with the file realm user
        final Profile profile1 = doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);
        assertThat(profile1.user().username(), equalTo(RAC_USER_NAME));
        assertThat(profile1.user().realmName(), equalTo("file"));
        assertThat(profile1.user().domainName(), equalTo("my_domain"));
        assertThat(profile1.user().email(), nullValue());
        assertThat(profile1.user().fullName(), nullValue());

        // Get the profile back by ID
        assertThat(getProfile(profile1.uid(), Set.of()), equalTo(profile1));

        // Activate 2nd time with the native realm user and it should get the same profile
        // because they are under the same domain. User fields are updated to the native user's info
        final Profile profile2 = doActivateProfile(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD);
        assertThat(profile2.uid(), equalTo(profile1.uid()));
        assertThat(profile2.user().username(), equalTo(RAC_USER_NAME));
        assertThat(profile2.user().realmName(), equalTo("index"));
        assertThat(profile2.user().domainName(), equalTo("my_domain"));
        assertThat(profile2.user().email(), equalTo(RAC_USER_NAME + "@example.com"));
        assertThat(profile2.user().fullName(), nullValue());
        assertThat(profile2.user().roles(), containsInAnyOrder(RAC_ROLE));

        // Activate 3rd time with the file realm user again and it should get the same profile
        // User fields are updated to the file user's info again
        final Profile profile3 = doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);
        assertThat(profile3.uid(), equalTo(profile1.uid()));
        assertThat(profile3.user().realmName(), equalTo("file"));
        assertThat(profile3.user().domainName(), equalTo("my_domain"));
        assertThat(profile3.user().email(), nullValue());
        assertThat(profile3.user().fullName(), nullValue());
        assertThat(profile3.user().roles(), containsInAnyOrder(RAC_ROLE));

        // Update native rac user
        final PutUserRequest putUserRequest1 = new PutUserRequest();
        putUserRequest1.username(RAC_USER_NAME);
        putUserRequest1.roles(RAC_ROLE, "superuser");
        putUserRequest1.email(null);
        putUserRequest1.fullName("Native RAC User");
        assertThat(client().execute(PutUserAction.INSTANCE, putUserRequest1).actionGet().created(), is(false));

        // Activate again with the native RAC user to the same profile
        final Profile profile4 = doActivateProfile(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD);
        assertThat(profile4.uid(), equalTo(profile1.uid()));
        assertThat(profile4.user().username(), equalTo(RAC_USER_NAME));
        assertThat(profile4.user().realmName(), equalTo("index"));
        assertThat(profile4.user().domainName(), equalTo("my_domain"));
        assertThat(profile4.user().email(), nullValue());
        assertThat(profile4.user().fullName(), equalTo("Native RAC User"));
        assertThat(profile4.user().roles(), containsInAnyOrder(RAC_ROLE, "superuser"));

        // Get by ID immediately should get the same document and content as the response to activate
        assertThat(getProfile(profile1.uid(), Set.of()), equalTo(profile4));
    }

    public void testGetProfileByAuthenticationUnderDomain() {
        final ProfileService profileService = node().injector().getInstance(ProfileService.class);

        final String nodeName = randomAlphaOfLengthBetween(3, 8);
        final RealmConfig.RealmIdentifier realmIdentifier1 = new RealmConfig.RealmIdentifier("realm_type_1", "realm_name_1");
        final RealmConfig.RealmIdentifier realmIdentifier2 = new RealmConfig.RealmIdentifier("realm_type_2", "realm_name_2");

        // Domain name does not matter
        final String domainName = randomFrom("domainA", randomAlphaOfLengthBetween(5, 12));
        // The recorded realm is realm_name_1, domain realms must contain the recorded realm
        final Set<RealmConfig.RealmIdentifier> domainRealms = randomBoolean()
            ? Set.of(realmIdentifier1, realmIdentifier2)
            : Set.of(realmIdentifier1);
        final RealmDomain realmDomain = new RealmDomain(domainName, domainRealms);

        final RealmConfig.RealmIdentifier authenticationRealmIdentifier = randomFrom(domainRealms);

        final Authentication authentication = new Authentication(
            new User("foo"),
            new Authentication.RealmRef(
                authenticationRealmIdentifier.getName(),
                authenticationRealmIdentifier.getType(),
                nodeName,
                realmDomain
            ),
            null
        );
        final Subject subject = AuthenticationContext.fromAuthentication(authentication).getEffectiveSubject();

        // Profile does not exist yet
        final PlainActionFuture<ProfileService.VersionedDocument> future1 = new PlainActionFuture<>();
        profileService.getVersionedDocument(subject, future1);
        assertThat(future1.actionGet(), nullValue());

        // Index the document so it can be found
        // The document is created with realm_name_1 under domainA (member realms are realm_name_1 and realm_name_2)
        final String uid2 = indexDocument();
        final PlainActionFuture<ProfileService.VersionedDocument> future2 = new PlainActionFuture<>();
        profileService.getVersionedDocument(subject, future2);
        final ProfileService.VersionedDocument versionedDocument = future2.actionGet();
        assertThat(versionedDocument, notNullValue());
        assertThat(versionedDocument.doc().uid(), equalTo(uid2));

        // Index it again to trigger duplicate exception
        final String uid3 = indexDocument();
        final PlainActionFuture<ProfileService.VersionedDocument> future3 = new PlainActionFuture<>();
        profileService.getVersionedDocument(subject, future3);
        final ElasticsearchException e3 = expectThrows(ElasticsearchException.class, future3::actionGet);

        assertThat(
            e3.getMessage(),
            containsString(
                "multiple [2] profiles [" + Stream.of(uid2, uid3).sorted().collect(Collectors.joining(",")) + "] found for user [foo]"
            )
        );
    }

    public void testGetProfileByAuthenticationDomainless() {
        final ProfileService profileService = node().injector().getInstance(ProfileService.class);
        // The document is created with realm_name_1 under domainA (member realms are realm_name_1 and realm_name_2)
        final String uid1 = indexDocument();
        final String nodeName = randomAlphaOfLengthBetween(3, 8);
        final RealmConfig.RealmIdentifier realmIdentifier1 = new RealmConfig.RealmIdentifier("realm_type_1", "realm_name_1");
        final RealmConfig.RealmIdentifier realmIdentifier2 = new RealmConfig.RealmIdentifier("realm_type_2", "realm_name_2");

        // Scenario 1
        // The recorded realm_name_1 is no longer part of a domain.
        // Authentication for this realm still works for retrieving the same profile document
        final Authentication authentication1 = new Authentication(
            new User("foo"),
            new Authentication.RealmRef(realmIdentifier1.getName(), realmIdentifier1.getType(), nodeName),
            null
        );
        final Subject subject1 = AuthenticationContext.fromAuthentication(authentication1).getEffectiveSubject();

        final PlainActionFuture<ProfileService.VersionedDocument> future1 = new PlainActionFuture<>();
        profileService.getVersionedDocument(subject1, future1);
        final ProfileService.VersionedDocument versionedDocument1 = future1.actionGet();
        assertThat(versionedDocument1, notNullValue());
        assertThat(versionedDocument1.doc().uid(), equalTo(uid1));

        // Scenario 2
        // The recorded realm_name_1 is no longer part of a domain.
        // Authentication for realm_name_2 (which is still part of domainA) does not work for retrieving the profile document
        final RealmDomain realmDomain1 = new RealmDomain("domainA", Set.of(realmIdentifier2));
        final Authentication authentication2 = new Authentication(
            new User("foo"),
            new Authentication.RealmRef(realmIdentifier2.getName(), realmIdentifier2.getType(), nodeName, realmDomain1),
            null
        );
        final Subject subject2 = AuthenticationContext.fromAuthentication(authentication2).getEffectiveSubject();

        final PlainActionFuture<ProfileService.VersionedDocument> future2 = new PlainActionFuture<>();
        profileService.getVersionedDocument(subject2, future2);
        assertThat(future2.actionGet(), nullValue());

        // Scenario 3
        // Both recorded realm_name_1 and the authentication realm_name_2 are no longer part of a domain.
        final Authentication authentication3 = new Authentication(
            new User("foo"),
            new Authentication.RealmRef(realmIdentifier2.getName(), realmIdentifier2.getType(), nodeName),
            null
        );
        final Subject subject3 = AuthenticationContext.fromAuthentication(authentication3).getEffectiveSubject();

        final PlainActionFuture<ProfileService.VersionedDocument> future3 = new PlainActionFuture<>();
        profileService.getVersionedDocument(subject3, future3);
        assertThat(future3.actionGet(), nullValue());
    }

    public void testGetProfileByAuthenticationWillNotCheckRealmNameForFileOrNativeRealm() {
        // File and native realms are under the same domain, activate the profile from either the realm
        final Profile profile1;
        if (randomBoolean()) {
            profile1 = doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);
        } else {
            profile1 = doActivateProfile(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD);
        }
        final ProfileService profileService = node().injector().getInstance(ProfileService.class);

        final String realmName = randomAlphaOfLengthBetween(3, 8);
        final String realmType = randomBoolean() ? FileRealmSettings.TYPE : NativeRealmSettings.TYPE;
        final RealmDomain realmDomain = new RealmDomain(
            randomAlphaOfLengthBetween(3, 8),
            Set.of(
                new RealmConfig.RealmIdentifier(realmType, realmName),
                new RealmConfig.RealmIdentifier(
                    FileRealmSettings.TYPE.equals(realmType) ? NativeRealmSettings.TYPE : FileRealmSettings.TYPE,
                    randomAlphaOfLengthBetween(3, 8)
                )
            )
        );
        final Authentication.RealmRef authenticatedBy = new Authentication.RealmRef(
            realmName,
            realmType,
            randomAlphaOfLengthBetween(3, 8),
            realmDomain
        );
        final Authentication authentication1 = Authentication.newRealmAuthentication(new User(RAC_USER_NAME), authenticatedBy);

        final PlainActionFuture<Profile> future1 = new PlainActionFuture<>();
        profileService.activateProfile(authentication1, future1);
        assertThat(future1.actionGet().uid(), equalTo(profile1.uid()));
    }

    private String indexDocument() {
        final String uid = randomAlphaOfLength(20);
        final String source = ProfileServiceTests.SAMPLE_PROFILE_DOCUMENT_TEMPLATE.formatted(uid, Instant.now().toEpochMilli());
        client().prepareIndex(randomFrom(INTERNAL_SECURITY_PROFILE_INDEX_8, SECURITY_PROFILE_ALIAS))
            .setId("profile_" + uid)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
            .setSource(source, XContentType.JSON)
            .get();
        return uid;
    }
}
