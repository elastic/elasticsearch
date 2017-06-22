/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.bwcompat;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.VersionTests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.OldIndexUtils;
import org.elasticsearch.test.VersionUtils;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.elasticsearch.test.OldIndexUtils.getIndexDir;

// needs at least 2 nodes since it bumps replicas to 1
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class OldIndexBackwardsCompatibilityIT extends ESIntegTestCase {
    // TODO: test for proper exception on unsupported indexes (maybe via separate test?)
    // We have a 0.20.6.zip etc for this.


    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class);
    }

    List<String> indexes;
    List<String> unsupportedIndexes;
    static String singleDataPathNodeName;
    static String multiDataPathNodeName;
    static Path singleDataPath;
    static Path[] multiDataPath;

    @Before
    public void initIndexesList() throws Exception {
        indexes = OldIndexUtils.loadDataFilesList("index", getBwcIndicesPath());
        unsupportedIndexes = OldIndexUtils.loadDataFilesList("unsupported", getBwcIndicesPath());
    }

    @AfterClass
    public static void tearDownStatics() {
        singleDataPathNodeName = null;
        multiDataPathNodeName = null;
        singleDataPath = null;
        multiDataPath = null;
    }

    @Override
    public Settings nodeSettings(int ord) {
        return OldIndexUtils.getSettings();
    }

    public void testAllVersionsTested() throws Exception {
        SortedSet<String> expectedVersions = new TreeSet<>();
        for (Version v : VersionUtils.allReleasedVersions()) {
            // The current version is in the "released" list even though it isn't released for historical reasons
            if (v == Version.CURRENT) continue;
            if (v.isRelease() == false) continue; // no guarantees for prereleases
            if (v.before(Version.CURRENT.minimumIndexCompatibilityVersion())) continue; // we can only support one major version backward
            if (v.equals(Version.CURRENT)) continue; // the current version is always compatible with itself
            expectedVersions.add("index-" + v.toString() + ".zip");
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

    private static final Version VERSION_5_1_0_UNRELEASED = Version.fromString("5.1.0");

    public void testUnreleasedVersion() {
        VersionTests.assertUnknownVersion(VERSION_5_1_0_UNRELEASED);
    }

    private Path getNodeDir(String indexFile) throws IOException {
        Path unzipDir = createTempDir();
        Path unzipDataDir = unzipDir.resolve("data");

        // decompress the index
        Path backwardsIndex = getBwcIndicesPath().resolve(indexFile);
        try (InputStream stream = Files.newInputStream(backwardsIndex)) {
            TestUtil.unzip(stream, unzipDir);
        }

        // check it is unique
        assertTrue(Files.exists(unzipDataDir));
        Path[] list = FileSystemUtils.files(unzipDataDir);
        if (list.length != 1) {
            throw new IllegalStateException("Backwards index must contain exactly one cluster");
        }

        int zipIndex = indexFile.indexOf(".zip");
        final Version version = Version.fromString(indexFile.substring("index-".length(), zipIndex));
        if (version.before(Version.V_5_0_0_alpha1)) {
            // the bwc scripts packs the indices under this path
            return list[0].resolve("nodes/0/");
        } else {
            // after 5.0.0, data folders do not include the cluster name
            return list[0].resolve("0");
        }
    }

    public void testOldClusterStates() throws Exception {
        // dangling indices do not load the global state, only the per-index states
        // so we make sure we can read them separately
        MetaDataStateFormat<MetaData> globalFormat = new MetaDataStateFormat<MetaData>(XContentType.JSON, "global-") {

            @Override
            public void toXContent(XContentBuilder builder, MetaData state) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public MetaData fromXContent(XContentParser parser) throws IOException {
                return MetaData.Builder.fromXContent(parser);
            }
        };
        MetaDataStateFormat<IndexMetaData> indexFormat = new MetaDataStateFormat<IndexMetaData>(XContentType.JSON, "state-") {

            @Override
            public void toXContent(XContentBuilder builder, IndexMetaData state) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public IndexMetaData fromXContent(XContentParser parser) throws IOException {
                return IndexMetaData.Builder.fromXContent(parser);
            }
        };
        Collections.shuffle(indexes, random());
        for (String indexFile : indexes) {
            String indexName = indexFile.replace(".zip", "").toLowerCase(Locale.ROOT).replace("unsupported-", "index-");
            Path nodeDir = getNodeDir(indexFile);
            logger.info("Parsing cluster state files from index [{}]", indexName);
            final MetaData metaData = globalFormat.loadLatestState(logger, xContentRegistry(), nodeDir);
            assertNotNull(metaData);

            final Version version = Version.fromString(indexName.substring("index-".length()));
            final Path dataDir;
            if (version.before(Version.V_5_0_0_alpha1)) {
                dataDir = nodeDir.getParent().getParent();
            } else {
                dataDir = nodeDir.getParent();
            }
            final Path indexDir = getIndexDir(logger, indexName, indexFile, dataDir);
            assertNotNull(indexFormat.loadLatestState(logger, xContentRegistry(), indexDir));
        }
    }

}
