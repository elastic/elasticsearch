/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.FakeRestRequest.Builder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.audit.logfile.CapturingLogger;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.AuditEventMetaInfo;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrailTests.MockRequest;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrailTests.RestContent;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.rest.RemoteHostHeader;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;
import static org.elasticsearch.xpack.security.authc.ApiKeyServiceTests.Utils.createApiKeyAuthentication;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoggingAuditTrailFilterTests extends ESTestCase {

    private static final String FILTER_MARKER = "filterMarker_";
    private static final String UNFILTER_MARKER = "nofilter_";

    private Settings settings;
    private DiscoveryNode localNode;
    private ClusterService clusterService;
    private ApiKeyService apiKeyService;

    @Before
    public void init() throws Exception {
        settings = Settings.builder()
                .put(LoggingAuditTrail.EMIT_HOST_ADDRESS_SETTING.getKey(), randomBoolean())
                .put(LoggingAuditTrail.EMIT_HOST_NAME_SETTING.getKey(), randomBoolean())
                .put(LoggingAuditTrail.EMIT_NODE_NAME_SETTING.getKey(), randomBoolean())
                .put(LoggingAuditTrail.INCLUDE_REQUEST_BODY.getKey(), randomBoolean())
                .put(LoggingAuditTrail.INCLUDE_EVENT_SETTINGS.getKey(), "_all")
                .build();
        localNode = mock(DiscoveryNode.class);
        when(localNode.getHostAddress()).thenReturn(buildNewFakeTransportAddress().toString());
        clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(localNode);
        final ClusterSettings clusterSettings = mockClusterSettings();
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        Mockito.doAnswer((Answer) invocation -> {
            final LoggingAuditTrail arg0 = (LoggingAuditTrail) invocation.getArguments()[0];
            arg0.updateLocalNodeInfo(localNode);
            return null;
        }).when(clusterService).addListener(Mockito.isA(LoggingAuditTrail.class));
        apiKeyService = new ApiKeyService(settings, Clock.systemUTC(), mock(Client.class), new XPackLicenseState(settings, () -> 0),
                mock(SecurityIndexManager.class), clusterService, mock(ThreadPool.class));
    }

    public void testPolicyDoesNotMatchNullValuesInEvent() throws Exception {
        final Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final Settings.Builder settingsBuilder = Settings.builder().put(settings);
        // filter by username
        final List<String> filteredUsernames = randomNonEmptyListOfFilteredNames();
        final List<User> filteredUsers = filteredUsernames.stream().map(u -> {
            if (randomBoolean()) {
                return new User(u);
            } else {
                return new User(new User(u), new User(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 4)));
            }
        }).collect(Collectors.toList());
        settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.userPolicy.users", filteredUsernames);
        // filter by realms
        final List<String> filteredRealms = randomNonEmptyListOfFilteredNames();
        settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.realmsPolicy.realms", filteredRealms);
        // filter by roles
        final List<String> filteredRoles = randomNonEmptyListOfFilteredNames();
        settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.rolesPolicy.roles", filteredRoles);
        // filter by indices
        final List<String> filteredIndices = randomNonEmptyListOfFilteredNames();
        settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.indicesPolicy.indices", filteredIndices);

        final LoggingAuditTrail auditTrail = new LoggingAuditTrail(settingsBuilder.build(), clusterService, logger, threadContext);

        // user field matches
        assertTrue("Matches the user filter predicate.", auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(
                new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)), Optional.empty(), Optional.empty(), Optional.empty())));
        final User unfilteredUser = mock(User.class);
        if (randomBoolean()) {
            when(unfilteredUser.authenticatedUser()).thenReturn(new User(randomFrom(filteredUsernames)));
        }
        // null user field does NOT match
        assertFalse("Does not match the user filter predicate because of null username.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate()
                        .test(new AuditEventMetaInfo(Optional.of(unfilteredUser), Optional.empty(), Optional.empty(), Optional.empty())));
        // realm field matches
        assertTrue("Matches the realm filter predicate.", auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(
                new AuditEventMetaInfo(Optional.empty(), Optional.of(randomFrom(filteredRealms)), Optional.empty(), Optional.empty())));
        // null realm field does NOT match
        assertFalse("Does not match the realm filter predicate because of null realm.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate()
                        .test(new AuditEventMetaInfo(Optional.empty(), Optional.ofNullable(null), Optional.empty(), Optional.empty())));
        // role field matches
        assertTrue("Matches the role filter predicate.", auditTrail.eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.empty(), Optional.empty(),
                        Optional.of(authzInfo(
                            randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles).toArray(new String[0]))),
                        Optional.empty())));
        final List<String> unfilteredRoles = new ArrayList<>();
        unfilteredRoles.add(null);
        unfilteredRoles.addAll(randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles));
        // null role among roles field does NOT match
        assertFalse("Does not match the role filter predicate because of null role.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.empty(), Optional.empty(),
                        Optional.of(authzInfo(unfilteredRoles.toArray(new String[0]))), Optional.empty())));
        // indices field matches
        assertTrue("Matches the index filter predicate.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.empty(), Optional.empty(),
                        Optional.empty(),
                        Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        final List<String> unfilteredIndices = new ArrayList<>();
        unfilteredIndices.add(null);
        unfilteredIndices.addAll(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices));
        // null index among indices field does NOT match
        assertFalse("Does not match the indices filter predicate because of null index.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.of(unfilteredIndices.toArray(new String[0])))));
    }

    public void testSingleCompletePolicyPredicate() throws Exception {
        final Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        // create complete filter policy
        final Settings.Builder settingsBuilder = Settings.builder().put(settings);
        // filter by username
        final List<String> filteredUsernames = randomNonEmptyListOfFilteredNames();
        final List<User> filteredUsers = filteredUsernames.stream().map(u -> {
            if (randomBoolean()) {
                return new User(u);
            } else {
                return new User(new User(u), new User(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 4)));
            }
        }).collect(Collectors.toList());
        settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.completeFilterPolicy.users", filteredUsernames);
        // filter by realms
        final List<String> filteredRealms = randomNonEmptyListOfFilteredNames();
        settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.completeFilterPolicy.realms", filteredRealms);
        // filter by roles
        final List<String> filteredRoles = randomNonEmptyListOfFilteredNames();
        settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.completeFilterPolicy.roles", filteredRoles);
        // filter by indices
        final List<String> filteredIndices = randomNonEmptyListOfFilteredNames();
        settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.completeFilterPolicy.indices", filteredIndices);

        final LoggingAuditTrail auditTrail = new LoggingAuditTrail(settingsBuilder.build(), clusterService, logger, threadContext);

        // all fields match
        assertTrue("Matches the filter predicate.", auditTrail.eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(
                Optional.of(randomFrom(filteredUsers)), Optional.of(randomFrom(filteredRealms)),
                Optional.of(authzInfo(randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles).toArray(new String[0]))),
                Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        final User unfilteredUser;
        if (randomBoolean()) {
            unfilteredUser = new User(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 8));
        } else {
            unfilteredUser = new User(new User(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 8)),
                    new User(randomFrom(filteredUsers).principal()));
        }
        // one field does not match or is empty
        assertFalse("Does not match the filter predicate because of the user.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(unfilteredUser),
                        Optional.of(randomFrom(filteredRealms)),
                        Optional.of(authzInfo(randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles)
                            .toArray(new String[0]))),
                        Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        assertFalse("Does not match the filter predicate because of the empty user.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.empty(),
                        Optional.of(randomFrom(filteredRealms)),
                        Optional.of(authzInfo(randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles)
                            .toArray(new String[0]))),
                        Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        assertFalse("Does not match the filter predicate because of the realm.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)),
                        Optional.of(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 8)),
                        Optional.of(authzInfo(randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles)
                            .toArray(new String[0]))),
                        Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        assertFalse("Does not match the filter predicate because of the empty realm.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)),
                        Optional.empty(),
                        Optional.of(authzInfo(randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles)
                                .toArray(new String[0]))),
                        Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        final List<String> someRolesDoNotMatch = new ArrayList<>(randomSubsetOf(randomIntBetween(0, filteredRoles.size()), filteredRoles));
        for (int i = 0; i < randomIntBetween(1, 8); i++) {
            someRolesDoNotMatch.add(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 8));
        }
        assertFalse("Does not match the filter predicate because of some of the roles.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)),
                        Optional.of(randomFrom(filteredRealms)), Optional.of(authzInfo(someRolesDoNotMatch.toArray(new String[0]))),
                        Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        final Optional<AuthorizationInfo> emptyRoles = randomBoolean() ? Optional.empty() : Optional.of(authzInfo(new String[0]));
        assertFalse("Does not match the filter predicate because of the empty roles.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)),
                        Optional.of(randomFrom(filteredRealms)), emptyRoles,
                        Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        final List<String> someIndicesDoNotMatch = new ArrayList<>(
                randomSubsetOf(randomIntBetween(0, filteredIndices.size()), filteredIndices));
        for (int i = 0; i < randomIntBetween(1, 8); i++) {
            someIndicesDoNotMatch.add(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 8));
        }
        assertFalse("Does not match the filter predicate because of some of the indices.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)), Optional.of(randomFrom(filteredRealms)),
                        Optional.of(authzInfo(randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles)
                                .toArray(new String[0]))),
                        Optional.of(someIndicesDoNotMatch.toArray(new String[0])))));
        final Optional<String[]> emptyIndices = randomBoolean() ? Optional.empty() : Optional.of(new String[0]);
        assertFalse("Does not match the filter predicate because of the empty indices.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)), Optional.of(randomFrom(filteredRealms)),
                        Optional.of(authzInfo(randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles)
                            .toArray(new String[0]))),
                        emptyIndices)));
    }

    public void testSingleCompleteWithEmptyFieldPolicyPredicate() throws Exception {
        final Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        // create complete filter policy
        final Settings.Builder settingsBuilder = Settings.builder().put(settings);
        // filter by username
        final List<String> filteredUsernames = randomNonEmptyListOfFilteredNames();
        final List<User> filteredUsers = filteredUsernames.stream().map(u -> {
            if (randomBoolean()) {
                return new User(u);
            } else {
                return new User(new User(u), new User(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 4)));
            }
        }).collect(Collectors.toList());
        filteredUsernames.add(""); // filter by missing user name
        settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.completeFilterPolicy.users", filteredUsernames);
        // filter by realms
        final List<String> filteredRealms = randomNonEmptyListOfFilteredNames();
        filteredRealms.add(""); // filter by missing realm name
        settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.completeFilterPolicy.realms", filteredRealms);
        filteredRealms.remove("");
        // filter by roles
        final List<String> filteredRoles = randomNonEmptyListOfFilteredNames();
        filteredRoles.add(""); // filter by missing role name
        settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.completeFilterPolicy.roles", filteredRoles);
        filteredRoles.remove("");
        // filter by indices
        final List<String> filteredIndices = randomNonEmptyListOfFilteredNames();
        filteredIndices.add(""); // filter by missing index name
        settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.completeFilterPolicy.indices", filteredIndices);
        filteredIndices.remove("");

        final LoggingAuditTrail auditTrail = new LoggingAuditTrail(settingsBuilder.build(), clusterService, logger, threadContext);

        // all fields match
        assertTrue("Matches the filter predicate.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)),
                        Optional.of(randomFrom(filteredRealms)),
                        Optional.of(authzInfo(randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles)
                            .toArray(new String[0]))),
                        Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        final User unfilteredUser;
        if (randomBoolean()) {
            unfilteredUser = new User(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 8));
        } else {
            unfilteredUser = new User(new User(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 8)),
                    new User(randomFrom(filteredUsers).principal()));
        }
        // one field does not match or is empty
        assertFalse("Does not match the filter predicate because of the user.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(unfilteredUser),
                        Optional.of(randomFrom(filteredRealms)),
                        Optional.of(authzInfo(randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles)
                            .toArray(new String[0]))),
                        Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        assertTrue("Matches the filter predicate because of the empty user.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.empty(),
                        Optional.of(randomFrom(filteredRealms)),
                        Optional.of(authzInfo(randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles)
                            .toArray(new String[0]))),
                        Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        assertFalse("Does not match the filter predicate because of the realm.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)),
                        Optional.of(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 8)),
                        Optional.of(authzInfo(randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles)
                            .toArray(new String[0]))),
                        Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        assertTrue("Matches the filter predicate because of the empty realm.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)),
                        Optional.empty(),
                        Optional.of(authzInfo(randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles)
                            .toArray(new String[0]))),
                        Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        final List<String> someRolesDoNotMatch = new ArrayList<>(randomSubsetOf(randomIntBetween(0, filteredRoles.size()), filteredRoles));
        for (int i = 0; i < randomIntBetween(1, 8); i++) {
            someRolesDoNotMatch.add(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 8));
        }
        assertFalse("Does not match the filter predicate because of some of the roles.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)),
                        Optional.of(randomFrom(filteredRealms)), Optional.of(authzInfo(someRolesDoNotMatch.toArray(new String[0]))),
                        Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        final Optional<AuthorizationInfo> emptyRoles = randomBoolean() ? Optional.empty() : Optional.of(authzInfo(new String[0]));
        assertTrue("Matches the filter predicate because of the empty roles.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)),
                        Optional.of(randomFrom(filteredRealms)), emptyRoles,
                        Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        final List<String> someIndicesDoNotMatch = new ArrayList<>(
                randomSubsetOf(randomIntBetween(0, filteredIndices.size()), filteredIndices));
        for (int i = 0; i < randomIntBetween(1, 8); i++) {
            someIndicesDoNotMatch.add(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 8));
        }
        assertFalse("Does not match the filter predicate because of some of the indices.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)), Optional.of(randomFrom(filteredRealms)),
                        Optional.of(authzInfo(randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles)
                            .toArray(new String[0]))),
                        Optional.of(someIndicesDoNotMatch.toArray(new String[0])))));
        assertTrue("Matches the filter predicate because of the empty indices.", auditTrail.eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)), Optional.of(randomFrom(filteredRealms)),
                        Optional.of(authzInfo(
                            randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles).toArray(new String[0]))),
                        Optional.empty())));
        assertTrue("Matches the filter predicate because of the empty indices.", auditTrail.eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)), Optional.of(randomFrom(filteredRealms)),
                        Optional.of(authzInfo(
                            randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles).toArray(new String[0]))),
                        Optional.of(new String[0]))));
        assertTrue("Matches the filter predicate because of the empty indices.", auditTrail.eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)), Optional.of(randomFrom(filteredRealms)),
                        Optional.of(authzInfo(
                            randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles).toArray(new String[0]))),
                        Optional.of(new String[] { null }))));
    }

    public void testTwoPolicyPredicatesWithMissingFields() throws Exception {
        final Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final Settings.Builder settingsBuilder = Settings.builder().put(settings);
        // first policy: realms and roles filters
        final List<String> filteredRealms = randomNonEmptyListOfFilteredNames();
        settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.firstPolicy.realms", filteredRealms);
        final List<String> filteredRoles = randomNonEmptyListOfFilteredNames();
        settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.firstPolicy.roles", filteredRoles);
        // second policy: users and indices filters
        final List<String> filteredUsernames = randomNonEmptyListOfFilteredNames();
        final List<User> filteredUsers = filteredUsernames.stream().map(u -> {
            if (randomBoolean()) {
                return new User(u);
            } else {
                return new User(new User(u), new User(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 4)));
            }
        }).collect(Collectors.toList());
        settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.secondPolicy.users", filteredUsernames);
        // filter by indices
        final List<String> filteredIndices = randomNonEmptyListOfFilteredNames();
        settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.secondPolicy.indices", filteredIndices);

        final LoggingAuditTrail auditTrail = new LoggingAuditTrail(settingsBuilder.build(), clusterService, logger, threadContext);

        final User unfilteredUser;
        if (randomBoolean()) {
            unfilteredUser = new User(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 8));
        } else {
            unfilteredUser = new User(new User(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 8)),
                    new User(randomFrom(filteredUsers).principal()));
        }
        final List<String> someRolesDoNotMatch = new ArrayList<>(randomSubsetOf(randomIntBetween(0, filteredRoles.size()), filteredRoles));
        for (int i = 0; i < randomIntBetween(1, 8); i++) {
            someRolesDoNotMatch.add(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 8));
        }
        final List<String> someIndicesDoNotMatch = new ArrayList<>(
                randomSubsetOf(randomIntBetween(0, filteredIndices.size()), filteredIndices));
        for (int i = 0; i < randomIntBetween(1, 8); i++) {
            someIndicesDoNotMatch.add(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 8));
        }
        // matches both the first and the second policies
        assertTrue("Matches both the first and the second filter predicates.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)),
                        Optional.of(randomFrom(filteredRealms)),
                        Optional.of(authzInfo(randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles)
                            .toArray(new String[0]))),
                        Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        // matches first policy but not the second
        assertTrue("Matches the first filter predicate but not the second.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(unfilteredUser),
                        Optional.of(randomFrom(filteredRealms)),
                        Optional.of(authzInfo(randomSubsetOf(randomIntBetween(1, filteredRoles.size()), filteredRoles)
                            .toArray(new String[0]))),
                        Optional.of(someIndicesDoNotMatch.toArray(new String[0])))));
        // matches the second policy but not the first
        assertTrue("Matches the second filter predicate but not the first.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(randomFrom(filteredUsers)),
                        Optional.of(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 8)),
                        Optional.of(authzInfo(someRolesDoNotMatch.toArray(new String[0]))),
                        Optional.of(randomSubsetOf(randomIntBetween(1, filteredIndices.size()), filteredIndices).toArray(new String[0])))));
        // matches neither the first nor the second policies
        assertFalse("Matches neither the first nor the second filter predicates.",
                auditTrail.eventFilterPolicyRegistry.ignorePredicate()
                        .test(new AuditEventMetaInfo(Optional.of(unfilteredUser),
                        Optional.of(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 8)),
                        Optional.of(authzInfo(someRolesDoNotMatch.toArray(new String[0]))),
                        Optional.of(someIndicesDoNotMatch.toArray(new String[0])))));
    }

    public void testUsersFilter() throws Exception {
        final Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final List<String> allFilteredUsers = new ArrayList<>();
        final Settings.Builder settingsBuilder = Settings.builder().put(settings);
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            final List<String> filteredUsers = randomNonEmptyListOfFilteredNames();
            allFilteredUsers.addAll(filteredUsers);
            settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.policy" + i + ".users", filteredUsers);
        }
        // a filter for a field consisting of an empty string ("") or an empty list([])
        // will match events that lack that field
        final boolean filterMissingUser = randomBoolean();
        if (filterMissingUser) {
            if (randomBoolean()) {
                final List<String> filteredUsers = randomNonEmptyListOfFilteredNames();
                // possibly renders list empty
                filteredUsers.remove(0);
                allFilteredUsers.addAll(filteredUsers);
                filteredUsers.add("");
                settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.missingPolicy.users", filteredUsers);
            } else {
                settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.missingPolicy.users",
                        Collections.emptyList());
            }
        }
        Authentication filteredAuthentication;
        if (randomBoolean()) {
            filteredAuthentication = createAuthentication(
                    new User(randomFrom(allFilteredUsers), new String[] { "r1" }, new User("authUsername", new String[] { "r2" })),
                    "effectiveRealmName");
        } else {
            filteredAuthentication = createAuthentication(new User(randomFrom(allFilteredUsers), new String[] { "r1" }),
                    "effectiveRealmName");
        }
        if (randomBoolean()) {
            filteredAuthentication = createApiKeyAuthentication(apiKeyService, filteredAuthentication);
        }
        Authentication unfilteredAuthentication;
        if (randomBoolean()) {
            unfilteredAuthentication = createAuthentication(new User(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 4),
                    new String[] { "r1" }, new User("authUsername", new String[] { "r2" })), "effectiveRealmName");
        } else {
            unfilteredAuthentication = createAuthentication(
                    new User(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 4), new String[] { "r1" }), "effectiveRealmName");
        }
        if (randomBoolean()) {
            unfilteredAuthentication = createApiKeyAuthentication(apiKeyService, unfilteredAuthentication);
        }
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext)
                : new MockIndicesRequest(threadContext, new String[] { "idx1", "idx2" });
        final MockToken filteredToken = new MockToken(randomFrom(allFilteredUsers));
        final MockToken unfilteredToken = new MockToken(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 4));

        final LoggingAuditTrail auditTrail = new LoggingAuditTrail(settingsBuilder.build(), clusterService, logger, threadContext);
        final List<String> logOutput = CapturingLogger.output(logger.getName(), Level.INFO);
        // anonymous accessDenied
        auditTrail.anonymousAccessDenied(randomAlphaOfLength(8), "_action", request);
        if (filterMissingUser) {
            assertThat("Anonymous message: not filtered out by the missing user filter", logOutput.size(), is(0));
        } else {
            assertThat("Anonymous message: filtered out by the user filters", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.anonymousAccessDenied(randomAlphaOfLength(8), getRestRequest());
        if (filterMissingUser) {
            assertThat("Anonymous rest request: not filtered out by the missing user filter", logOutput.size(), is(0));
        } else {
            assertThat("Anonymous rest request: filtered out by user filters", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // authenticationFailed
        auditTrail.authenticationFailed(randomAlphaOfLength(8), getRestRequest());
        if (filterMissingUser) {
            assertThat("AuthenticationFailed no token rest request: not filtered out by the missing user filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationFailed no token rest request: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), unfilteredToken, "_action", request);
        assertThat("AuthenticationFailed token request: unfiltered user is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), filteredToken, "_action", request);
        assertThat("AuthenticationFailed token request: filtered user is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), "_action", request);
        if (filterMissingUser) {
            assertThat("AuthenticationFailed no token message: not filtered out by the missing user filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationFailed no token message: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), unfilteredToken, getRestRequest());
        assertThat("AuthenticationFailed rest request: unfiltered user is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), filteredToken, getRestRequest());
        assertThat("AuthenticationFailed rest request: filtered user is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), "_realm", unfilteredToken, "_action", request);
        assertThat("AuthenticationFailed realm message: unfiltered user is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), "_realm", filteredToken, "_action", request);
        assertThat("AuthenticationFailed realm message: filtered user is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), "_realm", unfilteredToken, getRestRequest());
        assertThat("AuthenticationFailed realm rest request: unfiltered user is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), "_realm", filteredToken, getRestRequest());
        assertThat("AuthenticationFailed realm rest request: filtered user is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        // accessGranted
        auditTrail.accessGranted(randomAlphaOfLength(8), unfilteredAuthentication, "_action", request, authzInfo(new String[] { "role1" }));
        assertThat("AccessGranted message: unfiltered user is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessGranted(randomAlphaOfLength(8), filteredAuthentication, "_action", request, authzInfo(new String[] { "role1" }));
        assertThat("AccessGranted message: filtered user is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessGranted(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE, "effectiveRealmName"),
            "internal:_action", request, authzInfo(new String[] { "role1" }));
        assertThat("AccessGranted internal message: system user is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessGranted(randomAlphaOfLength(8), unfilteredAuthentication, "internal:_action", request,
            authzInfo(new String[] { "role1" }));
        assertThat("AccessGranted internal message: unfiltered user is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessGranted(randomAlphaOfLength(8), filteredAuthentication, "internal:_action", request,
            authzInfo(new String[] { "role1" }));
        assertThat("AccessGranted internal message: filtered user is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        // accessDenied
        auditTrail.accessDenied(randomAlphaOfLength(8), unfilteredAuthentication, "_action", request,
            authzInfo(new String[] { "role1" }));
        assertThat("AccessDenied message: unfiltered user is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessDenied(randomAlphaOfLength(8), filteredAuthentication, "_action", request,
            authzInfo(new String[] { "role1" }));
        assertThat("AccessDenied message: filtered user is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessDenied(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE, "effectiveRealmName"), "internal:_action",
                request, authzInfo(new String[] { "role1" }));
        assertThat("AccessDenied internal message: system user is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessDenied(randomAlphaOfLength(8), unfilteredAuthentication, "internal:_action", request,
            authzInfo(new String[] { "role1" }));
        assertThat("AccessDenied internal message: unfiltered user is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessDenied(randomAlphaOfLength(8), filteredAuthentication, "internal:_action", request,
            authzInfo(new String[] { "role1" }));
        assertThat("AccessDenied internal request: filtered user is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        // tamperedRequest
        auditTrail.tamperedRequest(randomAlphaOfLength(8), getRestRequest());
        if (filterMissingUser) {
            assertThat("Tampered rest: is not filtered out by the missing user filter", logOutput.size(), is(0));
        } else {
            assertThat("Tampered rest: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.tamperedRequest(randomAlphaOfLength(8), "_action", request);
        if (filterMissingUser) {
            assertThat("Tampered message: is not filtered out by the missing user filter", logOutput.size(), is(0));
        } else {
            assertThat("Tampered message: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.tamperedRequest(randomAlphaOfLength(8), unfilteredAuthentication, "_action", request);
        assertThat("Tampered message: unfiltered user is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.tamperedRequest(randomAlphaOfLength(8), filteredAuthentication, "_action", request);
        assertThat("Tampered message: filtered user is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        // connection denied
        auditTrail.connectionDenied(InetAddress.getLoopbackAddress(), "default", new SecurityIpFilterRule(false, "_all"));
        if (filterMissingUser) {
            assertThat("Connection denied: is not filtered out by the missing user filter", logOutput.size(), is(0));
        } else {
            assertThat("Connection denied: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // connection granted
        auditTrail.connectionGranted(InetAddress.getLoopbackAddress(), "default", new SecurityIpFilterRule(false, "_all"));
        if (filterMissingUser) {
            assertThat("Connection granted: is not filtered out by the missing user filter", logOutput.size(), is(0));
        } else {
            assertThat("Connection granted: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // runAsGranted
        auditTrail.runAsGranted(randomAlphaOfLength(8), unfilteredAuthentication, "_action", new MockRequest(threadContext),
            authzInfo(new String[] { "role1" }));
        assertThat("RunAsGranted message: unfiltered user is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsGranted(randomAlphaOfLength(8), filteredAuthentication, "_action", new MockRequest(threadContext),
            authzInfo(new String[] { "role1" }));
        assertThat("RunAsGranted message: filtered user is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        // runAsDenied
        auditTrail.runAsDenied(randomAlphaOfLength(8), unfilteredAuthentication, "_action", new MockRequest(threadContext),
            authzInfo(new String[] { "role1" }));
        assertThat("RunAsDenied message: unfiltered user is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsDenied(randomAlphaOfLength(8), filteredAuthentication, "_action", new MockRequest(threadContext),
            authzInfo(new String[] { "role1" }));
        assertThat("RunAsDenied message: filtered user is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsDenied(randomAlphaOfLength(8), unfilteredAuthentication, getRestRequest(), authzInfo(new String[] { "role1" }));
        assertThat("RunAsDenied rest request: unfiltered user is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsDenied(randomAlphaOfLength(8), filteredAuthentication, getRestRequest(), authzInfo(new String[] { "role1" }));
        assertThat("RunAsDenied rest request: filtered user is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        // authentication Success
        auditTrail.authenticationSuccess(randomAlphaOfLength(8), unfilteredAuthentication, getRestRequest());
        assertThat("AuthenticationSuccess rest request: unfiltered user is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationSuccess(randomAlphaOfLength(8), filteredAuthentication, getRestRequest());
        assertThat("AuthenticationSuccess rest request: filtered user is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationSuccess(randomAlphaOfLength(8), unfilteredAuthentication, "_action", request);
        assertThat("AuthenticationSuccess message: unfiltered user is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationSuccess(randomAlphaOfLength(8), filteredAuthentication, "_action", request);
        assertThat("AuthenticationSuccess message: filtered user is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();
    }

    public void testRealmsFilter() throws Exception {
        final Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final List<String> allFilteredRealms = new ArrayList<>();
        final Settings.Builder settingsBuilder = Settings.builder().put(settings);
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            final List<String> filteredRealms = randomNonEmptyListOfFilteredNames();
            allFilteredRealms.addAll(filteredRealms);
            settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.policy" + i + ".realms", filteredRealms);
        }
        // a filter for a field consisting of an empty string ("") or an empty list([])
        // will match events that lack that field
        final boolean filterMissingRealm = randomBoolean();
        if (filterMissingRealm) {
            if (randomBoolean()) {
                final List<String> filteredRealms = randomNonEmptyListOfFilteredNames();
                // possibly renders list empty
                filteredRealms.remove(0);
                allFilteredRealms.addAll(filteredRealms);
                filteredRealms.add("");
                settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.missingPolicy.realms", filteredRealms);
            } else {
                settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.missingPolicy.realms",
                        Collections.emptyList());
            }
        }
        final String filteredRealm = randomFrom(allFilteredRealms);
        final String unfilteredRealm = UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 4);
        User user;
        if (randomBoolean()) {
            user = new User("user1", new String[] { "r1" }, new User("authUsername", new String[] { "r2" }));
        } else {
            user = new User("user1", new String[] { "r1" });
        }
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext)
                : new MockIndicesRequest(threadContext, new String[] { "idx1", "idx2" });
        final MockToken authToken = new MockToken("token1");

        final LoggingAuditTrail auditTrail = new LoggingAuditTrail(settingsBuilder.build(), clusterService, logger, threadContext);
        final List<String> logOutput = CapturingLogger.output(logger.getName(), Level.INFO);
        // anonymous accessDenied
        auditTrail.anonymousAccessDenied(randomAlphaOfLength(8), "_action", request);
        if (filterMissingRealm) {
            assertThat("Anonymous message: not filtered out by the missing realm filter", logOutput.size(), is(0));
        } else {
            assertThat("Anonymous message: filtered out by the realm filters", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.anonymousAccessDenied(randomAlphaOfLength(8), getRestRequest());
        if (filterMissingRealm) {
            assertThat("Anonymous rest request: not filtered out by the missing realm filter", logOutput.size(), is(0));
        } else {
            assertThat("Anonymous rest request: filtered out by realm filters", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // authenticationFailed
        auditTrail.authenticationFailed(randomAlphaOfLength(8), getRestRequest());
        if (filterMissingRealm) {
            assertThat("AuthenticationFailed no token rest request: not filtered out by the missing realm filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationFailed no token rest request: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), authToken, "_action", request);
        if (filterMissingRealm) {
            assertThat("AuthenticationFailed token request: not filtered out by the missing realm filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationFailed token request: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), "_action", request);
        if (filterMissingRealm) {
            assertThat("AuthenticationFailed no token message: not filtered out by the missing realm filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationFailed no token message: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), authToken, getRestRequest());
        if (filterMissingRealm) {
            assertThat("AuthenticationFailed rest request: not filtered out by the missing realm filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationFailed rest request: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), unfilteredRealm, authToken, "_action", request);
        assertThat("AuthenticationFailed realm message: unfiltered realm is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), filteredRealm, authToken, "_action", request);
        assertThat("AuthenticationFailed realm message: filtered realm is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), unfilteredRealm, authToken, getRestRequest());
        assertThat("AuthenticationFailed realm rest request: unfiltered realm is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), filteredRealm, authToken, getRestRequest());
        assertThat("AuthenticationFailed realm rest request: filtered realm is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        // accessGranted
        Authentication authentication = randomBoolean() ? createAuthentication(user, filteredRealm) :
                createApiKeyAuthentication(apiKeyService, createAuthentication(user, filteredRealm));
        auditTrail.accessGranted(randomAlphaOfLength(8), authentication, "_action", request, authzInfo(new String[]{"role1"}));
        if (authentication.getAuthenticationType() == Authentication.AuthenticationType.API_KEY &&
                false == authentication.getMetadata().containsKey(ApiKeyService.API_KEY_CREATOR_REALM_NAME)) {
            if (filterMissingRealm) {
                assertThat("AccessGranted message: not filtered out by the missing realm filter", logOutput.size(), is(0));
            } else {
                assertThat("AccessGranted message: filtered out by the realm filters", logOutput.size(), is(1));
            }
        } else {
            assertThat("AccessGranted message: filtered realm is not filtered out", logOutput.size(), is(0));
        }
        logOutput.clear();
        threadContext.stashContext();

        authentication = randomBoolean() ? createAuthentication(user, unfilteredRealm) :
                createApiKeyAuthentication(apiKeyService, createAuthentication(user, unfilteredRealm));
        auditTrail.accessGranted(randomAlphaOfLength(8), authentication, "_action", request, authzInfo(new String[]{"role1"}));
        if (authentication.getAuthenticationType() == Authentication.AuthenticationType.API_KEY &&
                false == authentication.getMetadata().containsKey(ApiKeyService.API_KEY_CREATOR_REALM_NAME)) {
            if (filterMissingRealm) {
                assertThat("AccessGranted message: not filtered out by the missing realm filter", logOutput.size(), is(0));
            } else {
                assertThat("AccessGranted message: filtered out by the realm filters", logOutput.size(), is(1));
            }
        } else {
            assertThat("AccessGranted message: unfiltered realm is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessGranted(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE, filteredRealm), "internal:_action",
            request, authzInfo(new String[] { "role1" }));
        assertThat("AccessGranted internal message system user: filtered realm is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessGranted(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE, unfilteredRealm), "internal:_action",
            request, authzInfo(new String[] { "role1" }));
        assertThat("AccessGranted internal message system user: unfiltered realm is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        authentication = randomBoolean() ? createAuthentication(user, filteredRealm) :
                createApiKeyAuthentication(apiKeyService, createAuthentication(user, filteredRealm));
        auditTrail.accessGranted(randomAlphaOfLength(8), authentication, "internal:_action", request, authzInfo(new String[]{"role1"}));
        if (authentication.getAuthenticationType() == Authentication.AuthenticationType.API_KEY &&
                false == authentication.getMetadata().containsKey(ApiKeyService.API_KEY_CREATOR_REALM_NAME)) {
            if (filterMissingRealm) {
                assertThat("AccessGranted internal message: not filtered out by the missing realm filter", logOutput.size(), is(0));
            } else {
                assertThat("AccessGranted internal message: filtered out by the realm filters", logOutput.size(), is(1));
            }
        } else {
            assertThat("AccessGranted internal message: filtered realm is not filtered out", logOutput.size(), is(0));
        }
        logOutput.clear();
        threadContext.stashContext();

        authentication = randomBoolean() ? createAuthentication(user, unfilteredRealm) :
                createApiKeyAuthentication(apiKeyService, createAuthentication(user, unfilteredRealm));
        auditTrail.accessGranted(randomAlphaOfLength(8), authentication, "internal:_action", request, authzInfo(new String[] { "role1" }));
        if (authentication.getAuthenticationType() == Authentication.AuthenticationType.API_KEY &&
                false == authentication.getMetadata().containsKey(ApiKeyService.API_KEY_CREATOR_REALM_NAME)) {
            if (filterMissingRealm) {
                assertThat("AccessGranted internal message: not filtered out by the missing realm filter", logOutput.size(), is(0));
            } else {
                assertThat("AccessGranted internal message: filtered out by the realm filters", logOutput.size(), is(1));
            }
        } else {
            assertThat("AccessGranted internal message: unfiltered realm is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // accessDenied
        authentication = randomBoolean() ? createAuthentication(user, filteredRealm) :
                createApiKeyAuthentication(apiKeyService, createAuthentication(user, filteredRealm));
        auditTrail.accessDenied(randomAlphaOfLength(8), authentication, "_action", request, authzInfo(new String[]{"role1"}));
        if (authentication.getAuthenticationType() == Authentication.AuthenticationType.API_KEY &&
                false == authentication.getMetadata().containsKey(ApiKeyService.API_KEY_CREATOR_REALM_NAME)) {
            if (filterMissingRealm) {
                assertThat("AccessDenied message: not filtered out by the missing realm filter", logOutput.size(), is(0));
            } else {
                assertThat("AccessDenied message: filtered out by the realm filters", logOutput.size(), is(1));
            }
        } else {
            assertThat("AccessDenied message: filtered realm is not filtered out", logOutput.size(), is(0));
        }
        logOutput.clear();
        threadContext.stashContext();

        authentication = randomBoolean() ? createAuthentication(user, unfilteredRealm) :
                createApiKeyAuthentication(apiKeyService, createAuthentication(user, unfilteredRealm));
        auditTrail.accessDenied(randomAlphaOfLength(8), authentication, "_action", request, authzInfo(new String[]{"role1"}));
        if (authentication.getAuthenticationType() == Authentication.AuthenticationType.API_KEY &&
                false == authentication.getMetadata().containsKey(ApiKeyService.API_KEY_CREATOR_REALM_NAME)) {
            if (filterMissingRealm) {
                assertThat("AccessDenied message: not filtered out by the missing realm filter", logOutput.size(), is(0));
            } else {
                assertThat("AccessDenied message: filtered out by the realm filters", logOutput.size(), is(1));
            }
        } else {
            assertThat("AccessDenied message: unfiltered realm is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessDenied(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE, filteredRealm), "internal:_action",
            request, authzInfo(new String[] { "role1" }));
        assertThat("AccessDenied internal message system user: filtered realm is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessDenied(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE, unfilteredRealm), "internal:_action",
            request, authzInfo(new String[] { "role1" }));
        assertThat("AccessDenied internal message system user: unfiltered realm is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        authentication = randomBoolean() ? createAuthentication(user, filteredRealm) :
                createApiKeyAuthentication(apiKeyService, createAuthentication(user, filteredRealm));
        auditTrail.accessDenied(randomAlphaOfLength(8), authentication, "internal:_action", request, authzInfo(new String[]{"role1"}));
        if (authentication.getAuthenticationType() == Authentication.AuthenticationType.API_KEY &&
                false == authentication.getMetadata().containsKey(ApiKeyService.API_KEY_CREATOR_REALM_NAME)) {
            if (filterMissingRealm) {
                assertThat("AccessDenied internal message: not filtered out by the missing realm filter", logOutput.size(), is(0));
            } else {
                assertThat("AccessDenied internal message: filtered out by the realm filters", logOutput.size(), is(1));
            }
        } else {
            assertThat("AccessDenied internal message: filtered realm is filtered out", logOutput.size(), is(0));
        }
        logOutput.clear();
        threadContext.stashContext();

        authentication = randomBoolean() ? createAuthentication(user, unfilteredRealm) :
                createApiKeyAuthentication(apiKeyService, createAuthentication(user, unfilteredRealm));
        auditTrail.accessDenied(randomAlphaOfLength(8), authentication, "internal:_action",
                request, authzInfo(new String[]{"role1"}));
        if (authentication.getAuthenticationType() == Authentication.AuthenticationType.API_KEY &&
                false == authentication.getMetadata().containsKey(ApiKeyService.API_KEY_CREATOR_REALM_NAME)) {
            if (filterMissingRealm) {
                assertThat("AccessDenied internal message: not filtered out by the missing realm filter", logOutput.size(), is(0));
            } else {
                assertThat("AccessDenied internal message: filtered out by the realm filters", logOutput.size(), is(1));
            }
        } else {
            assertThat("AccessDenied internal message: unfiltered realm is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // tamperedRequest
        auditTrail.tamperedRequest(randomAlphaOfLength(8), getRestRequest());
        if (filterMissingRealm) {
            assertThat("Tampered rest: is not filtered out by the missing realm filter", logOutput.size(), is(0));
        } else {
            assertThat("Tampered rest: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.tamperedRequest(randomAlphaOfLength(8), "_action", request);
        if (filterMissingRealm) {
            assertThat("Tampered message: is not filtered out by the missing realm filter", logOutput.size(), is(0));
        } else {
            assertThat("Tampered message: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        authentication = randomBoolean() ? createAuthentication(user, filteredRealm) :
                createApiKeyAuthentication(apiKeyService, createAuthentication(user, filteredRealm));
        auditTrail.tamperedRequest(randomAlphaOfLength(8), authentication, "_action", request);
        if (authentication.getAuthenticationType() == Authentication.AuthenticationType.API_KEY &&
                false == authentication.getMetadata().containsKey(ApiKeyService.API_KEY_CREATOR_REALM_NAME)) {
            if (filterMissingRealm) {
                assertThat("Tampered message: not filtered out by the missing realm filter", logOutput.size(), is(0));
            } else {
                assertThat("Tampered message: filtered out by the realm filters", logOutput.size(), is(1));
            }
        } else {
            assertThat("Tampered message: filtered realm is not filtered out", logOutput.size(), is(0));
        }
        logOutput.clear();
        threadContext.stashContext();

        authentication = randomBoolean() ? createAuthentication(user, unfilteredRealm) :
                createApiKeyAuthentication(apiKeyService, createAuthentication(user, unfilteredRealm));
        auditTrail.tamperedRequest(randomAlphaOfLength(8), authentication, "_action", request);
        if (authentication.getAuthenticationType() == Authentication.AuthenticationType.API_KEY &&
                false == authentication.getMetadata().containsKey(ApiKeyService.API_KEY_CREATOR_REALM_NAME)) {
            if (filterMissingRealm) {
                assertThat("Tampered message: not filtered out by the missing realm filter", logOutput.size(), is(0));
            } else {
                assertThat("Tampered message: filtered out by the realm filters", logOutput.size(), is(1));
            }
        } else {
            assertThat("Tampered message: unfiltered realm is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // connection denied
        auditTrail.connectionDenied(InetAddress.getLoopbackAddress(), "default", new SecurityIpFilterRule(false, "_all"));
        if (filterMissingRealm) {
            assertThat("Connection denied: is not filtered out by the missing realm filter", logOutput.size(), is(0));
        } else {
            assertThat("Connection denied: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // connection granted
        auditTrail.connectionGranted(InetAddress.getLoopbackAddress(), "default", new SecurityIpFilterRule(false, "_all"));
        if (filterMissingRealm) {
            assertThat("Connection granted: is not filtered out by the missing realm filter", logOutput.size(), is(0));
        } else {
            assertThat("Connection granted: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // runAsGranted
        auditTrail.runAsGranted(randomAlphaOfLength(8), createAuthentication(user, filteredRealm), "_action",
            new MockRequest(threadContext), authzInfo(new String[] { "role1" }));
        assertThat("RunAsGranted message: filtered realm is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsGranted(randomAlphaOfLength(8), createAuthentication(user, unfilteredRealm), "_action",
            new MockRequest(threadContext), authzInfo(new String[] { "role1" }));
        assertThat("RunAsGranted message: unfiltered realm is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        // runAsDenied
        auditTrail.runAsDenied(randomAlphaOfLength(8), createAuthentication(user, filteredRealm), "_action", new MockRequest(threadContext),
                authzInfo(new String[] { "role1" }));
        assertThat("RunAsDenied message: filtered realm is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsDenied(randomAlphaOfLength(8), createAuthentication(user, unfilteredRealm), "_action",
            new MockRequest(threadContext), authzInfo(new String[] { "role1" }));
        assertThat("RunAsDenied message: unfiltered realm is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsDenied(randomAlphaOfLength(8), createAuthentication(user, filteredRealm), getRestRequest(),
            authzInfo(new String[] { "role1" }));
        assertThat("RunAsDenied rest request: filtered realm is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsDenied(randomAlphaOfLength(8), createAuthentication(user, unfilteredRealm), getRestRequest(),
            authzInfo(new String[] { "role1" }));
        assertThat("RunAsDenied rest request: unfiltered realm is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        // authentication Success
        auditTrail.authenticationSuccess(randomAlphaOfLength(8), createAuthentication(user, unfilteredRealm), getRestRequest());
        assertThat("AuthenticationSuccess rest request: unfiltered realm is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationSuccess(randomAlphaOfLength(8), createAuthentication(user, filteredRealm), getRestRequest());
        assertThat("AuthenticationSuccess rest request: filtered realm is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationSuccess(randomAlphaOfLength(8), createAuthentication(user, unfilteredRealm), "_action", request);
        assertThat("AuthenticationSuccess message: unfiltered realm is filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationSuccess(randomAlphaOfLength(8), createAuthentication(user, filteredRealm), "_action", request);
        assertThat("AuthenticationSuccess message: filtered realm is not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();
    }

    public void testRolesFilter() throws Exception {
        final Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final List<List<String>> allFilteredRoles = new ArrayList<>();
        final Settings.Builder settingsBuilder = Settings.builder().put(settings);
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            final List<String> filteredRoles = randomNonEmptyListOfFilteredNames();
            allFilteredRoles.add(new ArrayList<>(filteredRoles));
            settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.policy" + i + ".roles", filteredRoles);
        }
        // a filter for a field consisting of an empty string ("") or an empty list([])
        // will match events that lack that field
        final boolean filterMissingRoles = randomBoolean();
        if (filterMissingRoles) {
            if (randomBoolean()) {
                final List<String> filteredRoles = randomNonEmptyListOfFilteredNames();
                // possibly renders list empty
                filteredRoles.remove(0);
                if (filteredRoles.isEmpty() == false) {
                    allFilteredRoles.add(new ArrayList<>(filteredRoles));
                }
                filteredRoles.add("");
                settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.missingPolicy.roles", filteredRoles);
            } else {
                settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.missingPolicy.roles",
                        Collections.emptyList());
            }
        }
        // filtered roles are a subset of the roles of any policy
        final List<String> filterPolicy = randomFrom(allFilteredRoles);
        final String[] filteredRoles = randomListFromLengthBetween(filterPolicy, 1, filterPolicy.size()).toArray(new String[0]);
        // unfiltered role sets either have roles distinct from any other policy or are
        // a mix of roles from 2 or more policies
        final List<String> unfilteredPolicy = randomFrom(allFilteredRoles);
        List<String> _unfilteredRoles;
        if (randomBoolean()) {
            _unfilteredRoles = randomListFromLengthBetween(unfilteredPolicy, 0, unfilteredPolicy.size());
            // add roles distinct from any role in any filter policy
            for (int i = 0; i < randomIntBetween(1, 4); i++) {
                _unfilteredRoles.add(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 4));
            }
        } else {
            _unfilteredRoles = randomListFromLengthBetween(unfilteredPolicy, 1, unfilteredPolicy.size());
            // add roles from other filter policies
            final List<String> otherRoles = randomNonEmptyListOfFilteredNames("other");
            _unfilteredRoles.addAll(randomListFromLengthBetween(otherRoles, 1, otherRoles.size()));
            settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.otherPolicy.roles", otherRoles);
        }
        final String[] unfilteredRoles = _unfilteredRoles.toArray(new String[0]);
        Authentication authentication;
        if (randomBoolean()) {
            authentication = createAuthentication(new User("user1", new String[] { "r1" }, new User("authUsername", new String[] { "r2" })),
                    "effectiveRealmName");
        } else {
            authentication = createAuthentication(new User("user1", new String[] { "r1" }), "effectiveRealmName");
        }
        if (randomBoolean()) {
            authentication = createApiKeyAuthentication(apiKeyService, authentication);
        }
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext)
                : new MockIndicesRequest(threadContext, new String[] { "idx1", "idx2" });
        final MockToken authToken = new MockToken("token1");

        final LoggingAuditTrail auditTrail = new LoggingAuditTrail(settingsBuilder.build(), clusterService, logger, threadContext);
        final List<String> logOutput = CapturingLogger.output(logger.getName(), Level.INFO);
        // anonymous accessDenied
        auditTrail.anonymousAccessDenied(randomAlphaOfLength(8), "_action", request);
        if (filterMissingRoles) {
            assertThat("Anonymous message: not filtered out by the missing roles filter", logOutput.size(), is(0));
        } else {
            assertThat("Anonymous message: filtered out by the roles filters", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.anonymousAccessDenied(randomAlphaOfLength(8), getRestRequest());
        if (filterMissingRoles) {
            assertThat("Anonymous rest request: not filtered out by the missing roles filter", logOutput.size(), is(0));
        } else {
            assertThat("Anonymous rest request: filtered out by roles filters", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // authenticationFailed
        auditTrail.authenticationFailed(randomAlphaOfLength(8), getRestRequest());
        if (filterMissingRoles) {
            assertThat("AuthenticationFailed no token rest request: not filtered out by the missing roles filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationFailed no token rest request: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), authToken, "_action", request);
        if (filterMissingRoles) {
            assertThat("AuthenticationFailed token request: not filtered out by the missing roles filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationFailed token request: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), "_action", request);
        if (filterMissingRoles) {
            assertThat("AuthenticationFailed no token message: not filtered out by the missing roles filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationFailed no token message: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), authToken, getRestRequest());
        if (filterMissingRoles) {
            assertThat("AuthenticationFailed rest request: not filtered out by the missing roles filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationFailed rest request: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), "_realm", authToken, "_action", request);
        if (filterMissingRoles) {
            assertThat("AuthenticationFailed realm message: not filtered out by the missing roles filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationFailed realm message: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), "_realm", authToken, getRestRequest());
        if (filterMissingRoles) {
            assertThat("AuthenticationFailed realm rest request: not filtered out by the missing roles filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationFailed realm rest request: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // accessGranted
        auditTrail.accessGranted(randomAlphaOfLength(8), authentication, "_action", request, authzInfo(unfilteredRoles));
        assertThat("AccessGranted message: unfiltered roles filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessGranted(randomAlphaOfLength(8), authentication, "_action", request, authzInfo(filteredRoles));
        assertThat("AccessGranted message: filtered roles not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessGranted(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE, "effectiveRealmName"),
            "internal:_action", request, authzInfo(unfilteredRoles));
        assertThat("AccessGranted internal message system user: unfiltered roles filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessGranted(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE, "effectiveRealmName"),
            "internal:_action", request, authzInfo(filteredRoles));
        assertThat("AccessGranted internal message system user: filtered roles not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessGranted(randomAlphaOfLength(8), authentication, "internal:_action", request, authzInfo(unfilteredRoles));
        assertThat("AccessGranted internal message: unfiltered roles filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessGranted(randomAlphaOfLength(8), authentication, "internal:_action", request, authzInfo(filteredRoles));
        assertThat("AccessGranted internal message: filtered roles not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        // accessDenied
        auditTrail.accessDenied(randomAlphaOfLength(8), authentication, "_action", request, authzInfo(unfilteredRoles));
        assertThat("AccessDenied message: unfiltered roles filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessDenied(randomAlphaOfLength(8), authentication, "_action", request, authzInfo(filteredRoles));
        assertThat("AccessDenied message: filtered roles not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessDenied(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE, "effectiveRealmName"), "internal:_action",
            request, authzInfo(unfilteredRoles));
        assertThat("AccessDenied internal message system user: unfiltered roles filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessDenied(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE, "effectiveRealmName"), "internal:_action",
            request, authzInfo(filteredRoles));
        assertThat("AccessDenied internal message system user: filtered roles not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessDenied(randomAlphaOfLength(8), authentication, "internal:_action", request, authzInfo(unfilteredRoles));
        assertThat("AccessDenied internal message: unfiltered roles filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessDenied(randomAlphaOfLength(8), authentication, "internal:_action", request, authzInfo(filteredRoles));
        assertThat("AccessDenied internal message: filtered roles not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        // connection denied
        auditTrail.connectionDenied(InetAddress.getLoopbackAddress(), "default", new SecurityIpFilterRule(false, "_all"));
        if (filterMissingRoles) {
            assertThat("Connection denied: is not filtered out by the missing roles filter", logOutput.size(), is(0));
        } else {
            assertThat("Connection denied: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // connection granted
        auditTrail.connectionGranted(InetAddress.getLoopbackAddress(), "default", new SecurityIpFilterRule(false, "_all"));
        if (filterMissingRoles) {
            assertThat("Connection granted: is not filtered out by the missing roles filter", logOutput.size(), is(0));
        } else {
            assertThat("Connection granted: is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // runAsGranted
        auditTrail.runAsGranted(randomAlphaOfLength(8), authentication, "_action", new MockRequest(threadContext),
            authzInfo(unfilteredRoles));
        assertThat("RunAsGranted message: unfiltered roles filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsGranted(randomAlphaOfLength(8), authentication, "_action", new MockRequest(threadContext),
            authzInfo(filteredRoles));
        assertThat("RunAsGranted message: filtered roles not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        // runAsDenied
        auditTrail.runAsDenied(randomAlphaOfLength(8), authentication, "_action", new MockRequest(threadContext),
            authzInfo(unfilteredRoles));
        assertThat("RunAsDenied message: unfiltered roles filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsDenied(randomAlphaOfLength(8), authentication, "_action", new MockRequest(threadContext), authzInfo(filteredRoles));
        assertThat("RunAsDenied message: filtered roles not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsDenied(randomAlphaOfLength(8), authentication, getRestRequest(), authzInfo(unfilteredRoles));
        assertThat("RunAsDenied rest request: unfiltered roles filtered out", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsDenied(randomAlphaOfLength(8), authentication, getRestRequest(), authzInfo(filteredRoles));
        assertThat("RunAsDenied rest request: filtered roles not filtered out", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        // authentication Success
        auditTrail.authenticationSuccess(randomAlphaOfLength(8), authentication, getRestRequest());
        if (filterMissingRoles) {
            assertThat("AuthenticationSuccess rest request: is not filtered out by the missing roles filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationSuccess rest request: unfiltered realm is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationSuccess(randomAlphaOfLength(8), authentication, "_action", request);
        if (filterMissingRoles) {
            assertThat("AuthenticationSuccess message: is not filtered out by the missing roles filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationSuccess message: unfiltered realm is filtered out", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();
    }

    public void testIndicesFilter() throws Exception {
        final Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final List<List<String>> allFilteredIndices = new ArrayList<>();
        final Settings.Builder settingsBuilder = Settings.builder().put(settings);
        for (int i = 0; i < randomIntBetween(1, 3); i++) {
            final List<String> filteredIndices = randomNonEmptyListOfFilteredNames();
            allFilteredIndices.add(new ArrayList<>(filteredIndices));
            settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.policy" + i + ".indices", filteredIndices);
        }
        // a filter for a field consisting of an empty string ("") or an empty list([])
        // will match events that lack that field
        final boolean filterMissingIndices = randomBoolean();
        if (filterMissingIndices) {
            if (randomBoolean()) {
                final List<String> filteredIndices = randomNonEmptyListOfFilteredNames();
                // possibly renders list empty
                filteredIndices.remove(0);
                if (filteredIndices.isEmpty() == false) {
                    allFilteredIndices.add(new ArrayList<>(filteredIndices));
                }
                filteredIndices.add("");
                settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.missingPolicy.indices", filteredIndices);
            } else {
                settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.missingPolicy.indices",
                        Collections.emptyList());
            }
        }
        // filtered indices are a subset of the indices of any policy
        final List<String> filterPolicy = randomFrom(allFilteredIndices);
        final String[] filteredIndices = randomListFromLengthBetween(filterPolicy, 1, filterPolicy.size()).toArray(new String[0]);
        // unfiltered index sets either have indices distinct from any other in any
        // policy or are a mix of indices from 2 or more policies
        final List<String> unfilteredPolicy = randomFrom(allFilteredIndices);
        List<String> _unfilteredIndices;
        if (randomBoolean()) {
            _unfilteredIndices = randomListFromLengthBetween(unfilteredPolicy, 0, unfilteredPolicy.size());
            // add indices distinct from any index in any filter policy
            for (int i = 0; i < randomIntBetween(1, 4); i++) {
                _unfilteredIndices.add(UNFILTER_MARKER + randomAlphaOfLengthBetween(1, 4));
            }
        } else {
            _unfilteredIndices = randomListFromLengthBetween(unfilteredPolicy, 1, unfilteredPolicy.size());
            // add indices from other filter policies
            final List<String> otherIndices = randomNonEmptyListOfFilteredNames("other");
            _unfilteredIndices.addAll(randomListFromLengthBetween(otherIndices, 1, otherIndices.size()));
            settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters.otherPolicy.indices", otherIndices);
        }
        final String[] unfilteredIndices = _unfilteredIndices.toArray(new String[0]);
        Authentication authentication;
        if (randomBoolean()) {
            authentication = createAuthentication(new User("user1", new String[] { "r1" }, new User("authUsername", new String[] { "r2" })),
                    "effectiveRealmName");
        } else {
            authentication = createAuthentication(new User("user1", new String[] { "r1" }), "effectiveRealmName");
        }
        if (randomBoolean()) {
            authentication = createApiKeyAuthentication(apiKeyService, authentication);
        }
        final MockToken authToken = new MockToken("token1");
        final TransportRequest noIndexRequest = new MockRequest(threadContext);

        final LoggingAuditTrail auditTrail = new LoggingAuditTrail(settingsBuilder.build(), clusterService, logger, threadContext);
        final List<String> logOutput = CapturingLogger.output(logger.getName(), Level.INFO);
        // anonymous accessDenied
        auditTrail.anonymousAccessDenied(randomAlphaOfLength(8), "_action", noIndexRequest);
        if (filterMissingIndices) {
            assertThat("Anonymous message no index: not filtered out by the missing indices filter", logOutput.size(), is(0));
        } else {
            assertThat("Anonymous message no index: filtered out by indices filters", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.anonymousAccessDenied(randomAlphaOfLength(8), "_action", new MockIndicesRequest(threadContext, unfilteredIndices));
        assertThat("Anonymous message unfiltered indices: filtered out by indices filters", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.anonymousAccessDenied(randomAlphaOfLength(8), "_action", new MockIndicesRequest(threadContext, filteredIndices));
        assertThat("Anonymous message filtered indices: not filtered out by indices filters", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.anonymousAccessDenied(randomAlphaOfLength(8), getRestRequest());
        if (filterMissingIndices) {
            assertThat("Anonymous rest request: not filtered out by the missing indices filter", logOutput.size(), is(0));
        } else {
            assertThat("Anonymous rest request: filtered out by indices filters", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // authenticationFailed
        auditTrail.authenticationFailed(randomAlphaOfLength(8), getRestRequest());
        if (filterMissingIndices) {
            assertThat("AuthenticationFailed no token rest request: not filtered out by the missing indices filter", logOutput.size(),
                    is(0));
        } else {
            assertThat("AuthenticationFailed no token rest request: filtered out by indices filters", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), authToken, "_action", noIndexRequest);
        if (filterMissingIndices) {
            assertThat("AuthenticationFailed token request no index: not filtered out by the missing indices filter", logOutput.size(),
                    is(0));
        } else {
            assertThat("AuthenticationFailed token request no index: filtered out by indices filter", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), authToken, "_action",
            new MockIndicesRequest(threadContext, unfilteredIndices));
        assertThat("AuthenticationFailed token request unfiltered indices: filtered out by indices filter", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), authToken, "_action",
            new MockIndicesRequest(threadContext, filteredIndices));
        assertThat("AuthenticationFailed token request filtered indices: not filtered out by indices filter", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), "_action", noIndexRequest);
        if (filterMissingIndices) {
            assertThat("AuthenticationFailed no token message no index: not filtered out by the missing indices filter", logOutput.size(),
                    is(0));
        } else {
            assertThat("AuthenticationFailed no token message: filtered out by indices filter", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), "_action", new MockIndicesRequest(threadContext, unfilteredIndices));
        assertThat("AuthenticationFailed no token request unfiltered indices: filtered out by indices filter", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), "_action", new MockIndicesRequest(threadContext, filteredIndices));
        assertThat("AuthenticationFailed no token request filtered indices: not filtered out by indices filter", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), authToken, getRestRequest());
        if (filterMissingIndices) {
            assertThat("AuthenticationFailed rest request: not filtered out by the missing indices filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationFailed rest request: filtered out by indices filter", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), "_realm", authToken, "_action", noIndexRequest);
        if (filterMissingIndices) {
            assertThat("AuthenticationFailed realm message no index: not filtered out by the missing indices filter", logOutput.size(),
                    is(0));
        } else {
            assertThat("AuthenticationFailed realm message no index: filtered out by indices filter", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), "_realm", authToken, "_action",
            new MockIndicesRequest(threadContext, unfilteredIndices));
        assertThat("AuthenticationFailed realm message unfiltered indices: filtered out by indices filter", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), "_realm", authToken, "_action",
            new MockIndicesRequest(threadContext, filteredIndices));
        assertThat("AuthenticationFailed realm message filtered indices: not filtered out by indices filter", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationFailed(randomAlphaOfLength(8), "_realm", authToken, getRestRequest());
        if (filterMissingIndices) {
            assertThat("AuthenticationFailed realm rest request: not filtered out by the missing indices filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationFailed realm rest request: filtered out by indices filter", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // accessGranted
        auditTrail.accessGranted(randomAlphaOfLength(8), authentication, "_action", noIndexRequest, authzInfo(new String[] { "role1" }));
        if (filterMissingIndices) {
            assertThat("AccessGranted message no index: not filtered out by the missing indices filter", logOutput.size(), is(0));
        } else {
            assertThat("AccessGranted message no index: filtered out by indices filter", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessGranted(randomAlphaOfLength(8), authentication, "_action",
            new MockIndicesRequest(threadContext, unfilteredIndices),
                authzInfo(new String[] { "role1" }));
        assertThat("AccessGranted message unfiltered indices: filtered out by indices filter", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessGranted(randomAlphaOfLength(8), authentication, "_action", new MockIndicesRequest(threadContext, filteredIndices),
                authzInfo(new String[] { "role1" }));
        assertThat("AccessGranted message filtered indices: not filtered out by indices filter", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessGranted(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE, "effectiveRealmName"),
            "internal:_action", noIndexRequest, authzInfo(new String[] { "role1" }));
        if (filterMissingIndices) {
            assertThat("AccessGranted message system user no index: not filtered out by the missing indices filter", logOutput.size(),
                    is(0));
        } else {
            assertThat("AccessGranted message system user no index: filtered out by indices filter", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessGranted(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE, "effectiveRealmName"),
            "internal:_action", new MockIndicesRequest(threadContext, unfilteredIndices), authzInfo(new String[] { "role1" }));
        assertThat("AccessGranted message system user unfiltered indices: filtered out by indices filter", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessGranted(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE, "effectiveRealmName"),
            "internal:_action", new MockIndicesRequest(threadContext, filteredIndices), authzInfo(new String[] { "role1" }));
        assertThat("AccessGranted message system user filtered indices: not filtered out by indices filter", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        // accessDenied
        auditTrail.accessDenied(randomAlphaOfLength(8), authentication, "_action", noIndexRequest, authzInfo(new String[] { "role1" }));
        if (filterMissingIndices) {
            assertThat("AccessDenied message no index: not filtered out by the missing indices filter", logOutput.size(), is(0));
        } else {
            assertThat("AccessDenied message no index: filtered out by indices filter", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessDenied(randomAlphaOfLength(8), authentication, "_action", new MockIndicesRequest(threadContext, unfilteredIndices),
                authzInfo(new String[] { "role1" }));
        assertThat("AccessDenied message unfiltered indices: filtered out by indices filter", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessDenied(randomAlphaOfLength(8), authentication, "_action", new MockIndicesRequest(threadContext, filteredIndices),
                authzInfo(new String[] { "role1" }));
        assertThat("AccessDenied message filtered indices: not filtered out by indices filter", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessDenied(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE, "effectiveRealmName"), "internal:_action",
            noIndexRequest, authzInfo(new String[] { "role1" }));
        if (filterMissingIndices) {
            assertThat("AccessDenied message system user no index: not filtered out by the missing indices filter", logOutput.size(),
                    is(0));
        } else {
            assertThat("AccessDenied message system user no index: filtered out by indices filter", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessDenied(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE, "effectiveRealmName"), "internal:_action",
                new MockIndicesRequest(threadContext, unfilteredIndices),
                authzInfo(new String[] { "role1" }));
        assertThat("AccessDenied message system user unfiltered indices: filtered out by indices filter", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.accessDenied(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE, "effectiveRealmName"), "internal:_action",
                new MockIndicesRequest(threadContext, filteredIndices),
                authzInfo(new String[] { "role1" }));
        assertThat("AccessGranted message system user filtered indices: not filtered out by indices filter", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        // connection denied
        auditTrail.connectionDenied(InetAddress.getLoopbackAddress(), "default", new SecurityIpFilterRule(false, "_all"));
        if (filterMissingIndices) {
            assertThat("Connection denied: not filtered out by missing indices filter", logOutput.size(), is(0));
        } else {
            assertThat("Connection denied: filtered out by indices filter", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // connection granted
        auditTrail.connectionGranted(InetAddress.getLoopbackAddress(), "default", new SecurityIpFilterRule(false, "_all"));
        if (filterMissingIndices) {
            assertThat("Connection granted: not filtered out by missing indices filter", logOutput.size(), is(0));
        } else {
            assertThat("Connection granted: filtered out by indices filter", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // runAsGranted
        auditTrail.runAsGranted(randomAlphaOfLength(8), authentication, "_action", noIndexRequest, authzInfo(new String[] { "role1" }));
        if (filterMissingIndices) {
            assertThat("RunAsGranted message no index: not filtered out by missing indices filter", logOutput.size(), is(0));
        } else {
            assertThat("RunAsGranted message no index: filtered out by indices filter", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsGranted(randomAlphaOfLength(8), authentication, "_action", new MockIndicesRequest(threadContext, unfilteredIndices),
                authzInfo(new String[] { "role1" }));
        assertThat("RunAsGranted message unfiltered indices: filtered out by indices filter", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsGranted(randomAlphaOfLength(8), authentication, "_action", new MockIndicesRequest(threadContext, filteredIndices),
                authzInfo(new String[] { "role1" }));
        assertThat("RunAsGranted message filtered indices: not filtered out by indices filter", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        // runAsDenied
        auditTrail.runAsDenied(randomAlphaOfLength(8), authentication, "_action", noIndexRequest, authzInfo(new String[] { "role1" }));
        if (filterMissingIndices) {
            assertThat("RunAsDenied message no index: not filtered out by missing indices filter", logOutput.size(), is(0));
        } else {
            assertThat("RunAsDenied message no index: filtered out by indices filter", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsDenied(randomAlphaOfLength(8), authentication, "_action", new MockIndicesRequest(threadContext, unfilteredIndices),
                authzInfo(new String[] { "role1" }));
        assertThat("RunAsDenied message unfiltered indices: filtered out by indices filter", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsDenied(randomAlphaOfLength(8), authentication, "_action", new MockIndicesRequest(threadContext, filteredIndices),
                authzInfo(new String[] { "role1" }));
        assertThat("RunAsDenied message filtered indices: not filtered out by indices filter", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.runAsDenied(randomAlphaOfLength(8), authentication, getRestRequest(), authzInfo(new String[] { "role1" }));
        if (filterMissingIndices) {
            assertThat("RunAsDenied rest request: not filtered out by missing indices filter", logOutput.size(), is(0));
        } else {
            assertThat("RunAsDenied rest request: filtered out by indices filter", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        // authentication Success
        auditTrail.authenticationSuccess(randomAlphaOfLength(8), authentication, getRestRequest());
        if (filterMissingIndices) {
            assertThat("AuthenticationSuccess rest request: is not filtered out by the missing indices filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationSuccess rest request: filtered out by indices filter", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationSuccess(randomAlphaOfLength(8), authentication, "_action", noIndexRequest);
        if (filterMissingIndices) {
            assertThat("AuthenticationSuccess message no index: not filtered out by missing indices filter", logOutput.size(), is(0));
        } else {
            assertThat("AuthenticationSuccess message no index: filtered out by indices filter", logOutput.size(), is(1));
        }
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationSuccess(randomAlphaOfLength(8), authentication, "_action", new MockIndicesRequest(threadContext,
                unfilteredIndices));
        assertThat("AuthenticationSuccess message unfiltered indices: filtered out by indices filter", logOutput.size(), is(1));
        logOutput.clear();
        threadContext.stashContext();

        auditTrail.authenticationSuccess(randomAlphaOfLength(8), authentication, "_action",
                new MockIndicesRequest(threadContext, filteredIndices));
        assertThat("AuthenticationSuccess message filtered indices: not filtered out by indices filter", logOutput.size(), is(0));
        logOutput.clear();
        threadContext.stashContext();
    }

    private <T> List<T> randomListFromLengthBetween(List<T> l, int min, int max) {
        assert (min >= 0) && (min <= max) && (max <= l.size());
        final int len = randomIntBetween(min, max);
        final List<T> ans = new ArrayList<>(len);
        while (ans.size() < len) {
            ans.add(randomFrom(l));
        }
        return ans;
    }

    private static Authentication createAuthentication(User user, String effectiveRealmName) {
        if (user.isRunAs()) {
            return new Authentication(user,
                    new RealmRef(UNFILTER_MARKER + randomAlphaOfLength(4), "test", "foo"), new RealmRef(effectiveRealmName, "up", "by"));
        } else {
            return new Authentication(user, new RealmRef(effectiveRealmName, "test", "foo"), null);
        }
    }

    private ClusterSettings mockClusterSettings() {
        final List<Setting<?>> settingsList = new ArrayList<>();
        LoggingAuditTrail.registerSettings(settingsList);
        settingsList.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new ClusterSettings(settings, new HashSet<>(settingsList));
    }

    private List<String> randomNonEmptyListOfFilteredNames(String... namePrefix) {
        final List<String> filtered = new ArrayList<>(4);
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            filtered.add(FILTER_MARKER + Strings.arrayToCommaDelimitedString(namePrefix) + randomAlphaOfLengthBetween(1, 4));
        }
        return filtered;
    }

    private RestRequest getRestRequest() throws IOException {
        final RestContent content = randomFrom(RestContent.values());
        final FakeRestRequest.Builder builder = new Builder(NamedXContentRegistry.EMPTY);
        if (content.hasContent()) {
            builder.withContent(content.content(), XContentType.JSON);
        }
        builder.withPath("_uri");
        final byte address[] = InetAddress.getByName(randomBoolean() ? "127.0.0.1" : "::1").getAddress();
        builder.withRemoteAddress(new InetSocketAddress(InetAddress.getByAddress("_hostname", address), 9200));
        builder.withParams(Collections.emptyMap());
        return builder.build();
    }

    private static class MockToken implements AuthenticationToken {
        private final String principal;

        MockToken(String principal) {
            this.principal = principal;
        }

        @Override
        public String principal() {
            return this.principal;
        }

        @Override
        public Object credentials() {
            fail("it's not allowed to print the credentials of the auth token");
            return null;
        }

        @Override
        public void clearCredentials() {

        }
    }

    static class MockIndicesRequest extends org.elasticsearch.action.MockIndicesRequest {

        MockIndicesRequest(ThreadContext threadContext, String... indices) throws IOException {
            super(IndicesOptions.strictExpandOpenAndForbidClosed(), indices);
            if (randomBoolean()) {
                remoteAddress(buildNewFakeTransportAddress());
            }
            if (randomBoolean()) {
                RemoteHostHeader.putRestRemoteAddress(threadContext, new InetSocketAddress(forge("localhost", "127.0.0.1"), 1234));
            }
        }

        /** creates address without any lookups. hostname can be null, for missing */
        private InetAddress forge(String hostname, String address) throws IOException {
            final byte bytes[] = InetAddress.getByName(address).getAddress();
            return InetAddress.getByAddress(hostname, bytes);
        }

        @Override
        public String toString() {
            return "mock-message";
        }
    }

    private static AuthorizationInfo authzInfo(String[] roles) {
        return () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, roles);
    }
}
