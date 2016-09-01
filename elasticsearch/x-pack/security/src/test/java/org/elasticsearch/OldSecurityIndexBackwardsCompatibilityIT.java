/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch;

import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.security.action.role.GetRolesResponse;
import org.elasticsearch.xpack.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.security.action.user.GetUsersResponse;
import org.elasticsearch.xpack.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.client.SecurityClient;
import org.elasticsearch.xpack.security.user.User;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.OldIndexUtils.copyIndex;
import static org.elasticsearch.test.OldIndexUtils.loadIndexesList;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordTokenTests.basicAuthHeaderValue;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;

/**
 * Backwards compatibility test that loads some data from a pre-5.0 cluster and attempts to do some basic security stuff with it. It
 * contains:
 * <ul>
 *  <li>This user: {@code {"username": "bwc_test_user", "roles" : [ "bwc_test_role" ], "password" : "9876543210"}}</li>
 *  <li>This role: {@code {"name": "bwc_test_role", "cluster": ["all"]}, "run_as": [ "other_user" ], "indices": [{
 *    "names": [ "index1", "index2" ],
 *    "privileges": ["all"],
 *    "fields": [ "title", "body" ],
 *    "query": "{\"match\": {\"title\": \"foo\"}}"
 *   }]}</li>
 *  <li>This document in {@code index1}: {@code {
 *   "title": "foo",
 *   "body": "bwc_test_user should be able to see this field",
 *   "secured_body": "bwc_test_user should not be able to see this field"}}</li>
 *  <li>This document in {@code index1}: {@code {"title": "bwc_test_user should not be able to see this document"}}</li>
 *  <li>This document in {@code index2}: {@code {
 *   "title": "foo",
 *   "body": "bwc_test_user should be able to see this field",
 *   "secured_body": "bwc_test_user should not be able to see this field"}}</li>
 *  <li>This document in {@code index2}: {@code {"title": "bwc_test_user should not be able to see this document"}}</li>
 *  <li>This document in {@code index3}: {@code {"title": "bwc_test_user should not see this index"}}</li>
 * </ul>
 **/
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0) // We'll start the nodes manually
public class OldSecurityIndexBackwardsCompatibilityIT extends SecurityIntegTestCase {

    List<String> indexes;
    static String importingNodeName;
    static Path dataPath;

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Before
    public void initIndexesList() throws Exception {
        indexes = loadIndexesList("x-pack", getBwcIndicesPath());
    }

    @AfterClass
    public static void tearDownStatics() {
        importingNodeName = null;
        dataPath = null;
    }

    @Override
    public Settings nodeSettings(int ord) {
        Settings settings = super.nodeSettings(ord);
        // speed up recoveries
        return Settings.builder()
                .put(ThrottlingAllocationDecider
                        .CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 30)
                .put(ThrottlingAllocationDecider
                        .CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 30)
                .put(settings).build();
    }

    @Override
    protected int maxNumberOfNodes() {
        try {
            return SecurityIntegTestCase.defaultMaxNumberOfNodes() + loadIndexesList("x-pack", getBwcIndicesPath()).size();
        } catch (IOException e) {
            throw new RuntimeException("couldn't enumerate bwc indices", e);
        }
    }

    void setupCluster(String pathToZipFile) throws Exception {
        // shutdown any nodes from previous zip files
        while (internalCluster().size() > 0) {
            internalCluster().stopRandomNode(s -> true);
        }
        // first create the data directory and unzip the data there
        // we put the whole cluster state and indexes because if we only copy indexes and import them as dangling then
        // the native realm services will start because there is no security index and nothing is recovering
        // but we want them to not start!
        dataPath = createTempDir();
        Settings.Builder nodeSettings = Settings.builder()
                .put("path.data", dataPath.toAbsolutePath());
        // unzip data
        Path backwardsIndex = getBwcIndicesPath().resolve(pathToZipFile);
        // decompress the index
        try (InputStream stream = Files.newInputStream(backwardsIndex)) {
            logger.info("unzipping {}", backwardsIndex.toString());
            TestUtil.unzip(stream, dataPath);
            // now we need to copy the whole thing so that it looks like an actual data path
            try (Stream<Path> unzippedFiles = Files.list(dataPath.resolve("data"))) {
                Path dataDir = unzippedFiles.findFirst().get();
                // this is not actually an index but the copy does the job anyway
                copyIndex(logger, dataDir.resolve("nodes"), "nodes", dataPath);
                // remove the original unzipped directory
            }
            IOUtils.rm(dataPath.resolve("data"));
        }

        // check it is unique
        assertTrue(Files.exists(dataPath));
        Path[] list = FileSystemUtils.files(dataPath);
        if (list.length != 1) {
            throw new IllegalStateException("Backwards index must contain exactly one node");
        }

        // start the node
        logger.info("--> Data path for importing node: {}", dataPath);
        importingNodeName = internalCluster().startNode(nodeSettings.build());
        Path[] nodePaths = internalCluster().getInstance(NodeEnvironment.class, importingNodeName).nodeDataPaths();
        assertEquals(1, nodePaths.length);
    }

    public void testAllVersionsTested() throws Exception {
        SortedSet<String> expectedVersions = new TreeSet<>();
        for (Version v : VersionUtils.allVersions()) {
            if (v.before(Version.V_2_3_0)) continue; // native realm only supported from 2.3.0 on
            if (v.equals(Version.CURRENT)) continue; // the current version is always compatible with itself
            if (v.isBeta() == true || v.isAlpha() == true || v.isRC() == true) continue; // don't check alphas etc
            expectedVersions.add("x-pack-" + v.toString() + ".zip");
        }
        for (String index : indexes) {
            if (expectedVersions.remove(index) == false) {
                logger.warn("Old indexes tests contain extra index: {}", index);
            }
        }
        if (expectedVersions.isEmpty() == false) {
            StringBuilder msg = new StringBuilder("Old index tests are missing indexes:");
            for (String expected : expectedVersions) {
                msg.append("\n" + expected);
            }
            fail(msg.toString());
        }
    }

    public void testOldIndexes() throws Exception {
        Collections.shuffle(indexes, random());
        for (String index : indexes) {
            setupCluster(index);
            ensureYellow();
            long startTime = System.nanoTime();
            assertBasicSecurityWorks();
            logger.info("--> Done testing {}, took {} millis", index, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
        }
    }

    void assertBasicSecurityWorks() throws Exception {
        // test that user and roles are there
        logger.info("Getting roles...");
        SecurityClient securityClient = new SecurityClient(client());
        assertBusy(() -> {
            assertEquals(NativeRolesStore.State.STARTED, internalCluster().getInstance(NativeRolesStore.class).state());
        });
        GetRolesResponse getRolesResponse = securityClient.prepareGetRoles("bwc_test_role").get();
        assertThat(getRolesResponse.roles(), arrayWithSize(1));
        RoleDescriptor role = getRolesResponse.roles()[0];
        assertEquals("bwc_test_role", role.getName());
        assertThat(role.getIndicesPrivileges(), arrayWithSize(1));
        RoleDescriptor.IndicesPrivileges indicesPrivileges = role.getIndicesPrivileges()[0];
        assertThat(indicesPrivileges.getIndices(), arrayWithSize(2));
        assertArrayEquals(new String[] { "index1", "index2" }, indicesPrivileges.getIndices());
        assertArrayEquals(new String[] { "title", "body" }, indicesPrivileges.getFields());
        assertArrayEquals(new String[] { "all" }, indicesPrivileges.getPrivileges());
        assertEquals("{\"match\": {\"title\": \"foo\"}}", indicesPrivileges.getQuery().utf8ToString());
        assertArrayEquals(new String[] { "all" }, role.getClusterPrivileges());
        assertArrayEquals(new String[] { "other_user" }, role.getRunAs());
        assertEquals("bwc_test_role", role.getName());

        logger.info("Getting users...");
        assertBusy(() -> {
            assertEquals(NativeUsersStore.State.STARTED, internalCluster().getInstance(NativeUsersStore.class).state());
        });
        GetUsersResponse getUsersResponse = securityClient.prepareGetUsers("bwc_test_user").get();
        assertThat(getUsersResponse.users(), arrayWithSize(1));
        User user = getUsersResponse.users()[0];
        assertArrayEquals(new String[] { "bwc_test_role" }, user.roles());
        assertEquals("bwc_test_user", user.principal());

        // check that documents are there
        assertThat(client().prepareSearch().get().getHits().getTotalHits(), equalTo(5L));

        Client bwcTestUserClient = client().filterWithHeader(
                singletonMap(UsernamePasswordToken.BASIC_AUTH_HEADER, basicAuthHeaderValue("bwc_test_user", "9876543210")));
        // check that index permissions work as expected
        SearchResponse searchResponse = bwcTestUserClient.prepareSearch("index1", "index2").get();
        assertEquals(2, searchResponse.getHits().getTotalHits());
        assertEquals("foo", searchResponse.getHits().getHits()[0].getSource().get("title"));
        assertEquals("bwc_test_user should be able to see this field", searchResponse.getHits().getHits()[0].getSource().get("body"));
        assertNull(searchResponse.getHits().getHits()[0].getSource().get("secured_body"));
        assertEquals("foo", searchResponse.getHits().getHits()[1].getSource().get("title"));
        assertEquals("bwc_test_user should be able to see this field", searchResponse.getHits().getHits()[1].getSource().get("body"));
        assertNull(searchResponse.getHits().getHits()[1].getSource().get("secured_body"));

        Exception e = expectThrows(ElasticsearchSecurityException.class, () -> bwcTestUserClient.prepareSearch("index3").get());
        assertEquals("action [indices:data/read/search] is unauthorized for user [bwc_test_user]", e.getMessage());

        // try adding a user
        PutRoleResponse roleResponse = securityClient.preparePutRole("test_role").addIndices(
                new String[] { "index3" },
                new String[] { "all" },
                new String[] { "title", "body" },
                new BytesArray("{\"term\": {\"title\":\"not\"}}")).cluster("all")
                .get();
        assertTrue(roleResponse.isCreated());
        PutUserResponse userResponse = securityClient.preparePutUser("another_bwc_test_user", "123123".toCharArray(), "test_role")
                .email("a@b.c").get();
        assertTrue(userResponse.created());
        searchResponse = client().filterWithHeader(
                Collections.singletonMap(UsernamePasswordToken.BASIC_AUTH_HEADER,
                        basicAuthHeaderValue("another_bwc_test_user", "123123")
                )).prepareSearch("index3").get();
        assertEquals(1, searchResponse.getHits().getTotalHits());
        assertEquals("bwc_test_user should not see this index", searchResponse.getHits().getHits()[0].getSource().get("title"));

        userResponse = securityClient.preparePutUser("meta_bwc_test_user", "123123".toCharArray(), "test_role").email("a@b.c")
                .metadata(singletonMap("test", 1)).get();
        assertTrue(userResponse.created());

        getUsersResponse = securityClient.prepareGetUsers("meta_bwc_test_user").get();
        assertThat(getUsersResponse.users(), arrayWithSize(1));
        user = getUsersResponse.users()[0];
        assertArrayEquals(new String[] { "test_role" }, user.roles());
        assertEquals("meta_bwc_test_user", user.principal());
    }
}
