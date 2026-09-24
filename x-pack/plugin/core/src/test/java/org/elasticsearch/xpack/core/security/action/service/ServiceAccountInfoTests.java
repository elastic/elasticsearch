/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ServiceAccountInfoTests extends AbstractWireSerializingTestCase<ServiceAccountInfo> {

    @Override
    protected Writeable.Reader<ServiceAccountInfo> instanceReader() {
        return ServiceAccountInfo::readFrom;
    }

    @Override
    protected ServiceAccountInfo createTestInstance() {
        return randomBoolean() ? randomBuiltIn() : randomUserManaged();
    }

    @Override
    protected ServiceAccountInfo mutateInstance(ServiceAccountInfo instance) {
        return switch (instance) {
            // Mutating within a kind as well as across kinds, so that the fields only one kind carries take part.
            case ServiceAccountInfo.BuiltIn builtIn -> randomFrom(
                new ServiceAccountInfo.BuiltIn(randomValueOtherThan(builtIn.principal(), this::randomPrincipal), builtIn.roleDescriptor()),
                new ServiceAccountInfo.BuiltIn(builtIn.principal(), roleDescriptorWithCluster(builtIn.principal(), "manage_security")),
                randomUserManaged()
            );
            case ServiceAccountInfo.UserManaged userManaged -> randomFrom(
                withPrincipal(userManaged, randomValueOtherThan(userManaged.principal(), this::randomPrincipal)),
                withRoles(
                    userManaged,
                    randomValueOtherThan(userManaged.roles(), () -> randomList(0, 3, () -> randomAlphaOfLengthBetween(3, 8)))
                ),
                withEnabled(userManaged, userManaged.enabled() == false),
                withDescription(userManaged, randomValueOtherThan(userManaged.description(), ServiceAccountInfoTests::randomDescription)),
                withCreator(userManaged, randomValueOtherThan(userManaged.creator(), ServiceAccountInfoTests::randomOptionalAuthor)),
                withCreatedAt(userManaged, randomValueOtherThan(userManaged.createdAt(), ServiceAccountInfoTests::randomOptionalInstant)),
                withEditor(userManaged, randomValueOtherThan(userManaged.editor(), ServiceAccountInfoTests::randomOptionalAuthor)),
                withEditedAt(userManaged, randomValueOtherThan(userManaged.editedAt(), ServiceAccountInfoTests::randomOptionalInstant)),
                randomBuiltIn()
            );
        };
    }

    private static ServiceAccountInfo.UserManaged withPrincipal(ServiceAccountInfo.UserManaged info, String principal) {
        return new ServiceAccountInfo.UserManaged(
            principal,
            info.roles(),
            info.enabled(),
            info.description(),
            info.creator(),
            info.createdAt(),
            info.editor(),
            info.editedAt()
        );
    }

    private static ServiceAccountInfo.UserManaged withRoles(ServiceAccountInfo.UserManaged info, List<String> roles) {
        return new ServiceAccountInfo.UserManaged(
            info.principal(),
            roles,
            info.enabled(),
            info.description(),
            info.creator(),
            info.createdAt(),
            info.editor(),
            info.editedAt()
        );
    }

    private static ServiceAccountInfo.UserManaged withEnabled(ServiceAccountInfo.UserManaged info, boolean enabled) {
        return new ServiceAccountInfo.UserManaged(
            info.principal(),
            info.roles(),
            enabled,
            info.description(),
            info.creator(),
            info.createdAt(),
            info.editor(),
            info.editedAt()
        );
    }

    private static ServiceAccountInfo.UserManaged withDescription(ServiceAccountInfo.UserManaged info, String description) {
        return new ServiceAccountInfo.UserManaged(
            info.principal(),
            info.roles(),
            info.enabled(),
            description,
            info.creator(),
            info.createdAt(),
            info.editor(),
            info.editedAt()
        );
    }

    private static ServiceAccountInfo.UserManaged withCreator(ServiceAccountInfo.UserManaged info, ServiceAccountAuthor creator) {
        return new ServiceAccountInfo.UserManaged(
            info.principal(),
            info.roles(),
            info.enabled(),
            info.description(),
            creator,
            info.createdAt(),
            info.editor(),
            info.editedAt()
        );
    }

    private static ServiceAccountInfo.UserManaged withCreatedAt(ServiceAccountInfo.UserManaged info, Instant createdAt) {
        return new ServiceAccountInfo.UserManaged(
            info.principal(),
            info.roles(),
            info.enabled(),
            info.description(),
            info.creator(),
            createdAt,
            info.editor(),
            info.editedAt()
        );
    }

    private static ServiceAccountInfo.UserManaged withEditor(ServiceAccountInfo.UserManaged info, ServiceAccountAuthor editor) {
        return new ServiceAccountInfo.UserManaged(
            info.principal(),
            info.roles(),
            info.enabled(),
            info.description(),
            info.creator(),
            info.createdAt(),
            editor,
            info.editedAt()
        );
    }

    private static ServiceAccountInfo.UserManaged withEditedAt(ServiceAccountInfo.UserManaged info, Instant editedAt) {
        return new ServiceAccountInfo.UserManaged(
            info.principal(),
            info.roles(),
            info.enabled(),
            info.description(),
            info.creator(),
            info.createdAt(),
            info.editor(),
            editedAt
        );
    }

    public void testTypeNamesTheKind() {
        assertThat(randomBuiltIn().type(), equalTo(ServiceAccountType.BUILT_IN));
        assertThat(randomUserManaged().type(), equalTo(ServiceAccountType.USER_MANAGED));
    }

    public void testRolesAreCopiedOnConstruction() {
        final List<String> roles = new ArrayList<>(List.of("role-a"));
        final ServiceAccountInfo.UserManaged info = new ServiceAccountInfo.UserManaged("my-team/worker", roles, true, null);
        roles.add("role-b");
        assertThat(info.roles(), equalTo(List.of("role-a")));
    }

    public void testBuiltInAccountsStillSerializeToNodesWithoutUserManagedAccounts() throws IOException {
        final ServiceAccountInfo.BuiltIn builtIn = randomBuiltIn();
        assertThat(copyInstance(builtIn, beforeUserManagedAccountInfo()), equalTo(builtIn));
    }

    public void testUserManagedAccountsRefuseToSerializeToNodesWithoutThem() {
        final ServiceAccountInfo.UserManaged userManaged = randomUserManaged();
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> copyInstance(userManaged, beforeUserManagedAccountInfo())
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "cannot send information about the user-managed service account ["
                    + userManaged.principal()
                    + "] to a node that does not support user-managed service accounts"
            )
        );
    }

    /**
     * A node that knows user-managed accounts but not their descriptions is sent the account without one, rather than
     * being refused the account altogether: the description carries no meaning, so the account is still whole.
     */
    public void testTheDescriptionIsDroppedForNodesThatDoNotKnowIt() throws IOException {
        final ServiceAccountInfo.UserManaged userManaged = new ServiceAccountInfo.UserManaged(
            randomPrincipal(),
            randomList(0, 3, () -> randomAlphaOfLengthBetween(3, 8)),
            randomBoolean(),
            randomAlphaOfLengthBetween(1, 20)
        );
        // The version just before the description was added still knows user-managed accounts.
        final TransportVersion beforeDescription = TransportVersionUtils.getPreviousVersion(
            ServiceAccountInfo.USER_MANAGED_SERVICE_ACCOUNT_DESCRIPTION
        );
        assertTrue(beforeDescription.supports(ServiceAccountInfo.USER_MANAGED_SERVICE_ACCOUNT_INFO));
        final ServiceAccountInfo copy = copyInstance(userManaged, beforeDescription);
        assertThat(
            copy,
            equalTo(new ServiceAccountInfo.UserManaged(userManaged.principal(), userManaged.roles(), userManaged.enabled(), null))
        );
        assertThat(((ServiceAccountInfo.UserManaged) copy).description(), nullValue());
    }

    /**
     * As with the description, a node that does not know the attribution is sent the account without it.
     */
    public void testTheAttributionIsDroppedForNodesThatDoNotKnowIt() throws IOException {
        final ServiceAccountInfo.UserManaged userManaged = new ServiceAccountInfo.UserManaged(
            randomPrincipal(),
            randomList(0, 3, () -> randomAlphaOfLengthBetween(3, 8)),
            randomBoolean(),
            randomDescription(),
            ServiceAccountAuthorTests.randomAuthor(),
            randomInstant(),
            ServiceAccountAuthorTests.randomAuthor(),
            randomInstant()
        );
        final TransportVersion beforeAttribution = TransportVersionUtils.getPreviousVersion(
            ServiceAccountInfo.USER_MANAGED_SERVICE_ACCOUNT_ATTRIBUTION
        );
        assertTrue(beforeAttribution.supports(ServiceAccountInfo.USER_MANAGED_SERVICE_ACCOUNT_DESCRIPTION));
        final ServiceAccountInfo copy = copyInstance(userManaged, beforeAttribution);
        assertThat(
            copy,
            equalTo(
                new ServiceAccountInfo.UserManaged(
                    userManaged.principal(),
                    userManaged.roles(),
                    userManaged.enabled(),
                    userManaged.description()
                )
            )
        );
    }

    /**
     * The creator and editor are rendered as nested objects, the timestamps as epoch milliseconds, and whatever is
     * unknown is left out.
     */
    public void testAttributionIsRenderedAsNestedObjectsAndEpochMillis() throws IOException {
        final ServiceAccountAuthor creator = new ServiceAccountAuthor("alice", "Alice", null, "native1", "native", null);
        final Instant createdAt = Instant.ofEpochMilli(1_700_000_000_000L);
        final ServiceAccountInfo.UserManaged created = new ServiceAccountInfo.UserManaged(
            "apps/worker",
            List.of("role-a"),
            true,
            null,
            creator,
            createdAt,
            null,
            null
        );
        final Map<String, Object> createdMap = innerToMap(created);
        assertThat(
            createdMap,
            equalTo(
                Map.of(
                    "type",
                    "user_managed",
                    "roles",
                    List.of("role-a"),
                    "enabled",
                    true,
                    "creator",
                    Map.of("principal", "alice", "full_name", "Alice", "realm", "native1", "realm_type", "native"),
                    "created_at",
                    1_700_000_000_000L
                )
            )
        );
        assertThat(createdMap, not(hasKey("editor")));
        assertThat(createdMap, not(hasKey("edited_at")));

        final ServiceAccountAuthor editor = new ServiceAccountAuthor("bob", null, "bob@example.com", "ldap1", "ldap", null);
        final Map<String, Object> editedMap = innerToMap(
            new ServiceAccountInfo.UserManaged(
                "apps/worker",
                List.of("role-a"),
                true,
                null,
                creator,
                createdAt,
                editor,
                Instant.ofEpochMilli(1_700_000_001_000L)
            )
        );
        assertThat(
            editedMap.get("editor"),
            equalTo(Map.of("principal", "bob", "email", "bob@example.com", "realm", "ldap1", "realm_type", "ldap"))
        );
        assertThat(editedMap.get("edited_at"), equalTo(1_700_000_001_000L));
        assertThat(editedMap.get("creator"), equalTo(createdMap.get("creator")));

        final Map<String, Object> unattributed = innerToMap(new ServiceAccountInfo.UserManaged("apps/worker", List.of(), false, null));
        assertThat(unattributed, equalTo(Map.of("type", "user_managed", "roles", List.of(), "enabled", false)));
    }

    private static Map<String, Object> innerToMap(ServiceAccountInfo info) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        info.innerToXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
    }

    private static TransportVersion beforeUserManagedAccountInfo() {
        return TransportVersionUtils.getPreviousVersion(ServiceAccountInfo.USER_MANAGED_SERVICE_ACCOUNT_INFO);
    }

    private ServiceAccountInfo.BuiltIn randomBuiltIn() {
        final String principal = randomPrincipal();
        return new ServiceAccountInfo.BuiltIn(principal, roleDescriptorWithCluster(principal, "monitor"));
    }

    private ServiceAccountInfo.UserManaged randomUserManaged() {
        return new ServiceAccountInfo.UserManaged(
            randomPrincipal(),
            randomList(0, 3, () -> randomAlphaOfLengthBetween(3, 8)),
            randomBoolean(),
            randomDescription(),
            randomOptionalAuthor(),
            randomOptionalInstant(),
            randomOptionalAuthor(),
            randomOptionalInstant()
        );
    }

    private static String randomDescription() {
        return randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20);
    }

    private static ServiceAccountAuthor randomOptionalAuthor() {
        return randomBoolean() ? null : ServiceAccountAuthorTests.randomAuthor();
    }

    private static Instant randomOptionalInstant() {
        return randomBoolean() ? null : randomInstant();
    }

    private static Instant randomInstant() {
        return Instant.ofEpochMilli(randomLongBetween(0, 4_000_000_000_000L));
    }

    private String randomPrincipal() {
        return randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8);
    }

    private static RoleDescriptor roleDescriptorWithCluster(String name, String clusterPrivilege) {
        return new RoleDescriptor(name, new String[] { clusterPrivilege }, null, null);
    }
}
