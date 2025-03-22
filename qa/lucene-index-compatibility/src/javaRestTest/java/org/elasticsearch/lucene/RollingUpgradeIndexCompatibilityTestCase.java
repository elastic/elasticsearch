/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene;

import com.carrotsearch.randomizedtesting.TestMethodAndParams;
import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.util.Version;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_READ_ONLY_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_WRITE_BLOCK;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.INDEX_CLOSED_BLOCK;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING;
import static org.elasticsearch.test.cluster.util.Version.CURRENT;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Test suite for Lucene indices backward compatibility with N-2 versions during rolling upgrades. The test suite creates a cluster in N-2
 * version, then upgrades each node sequentially to N-1 version and finally upgrades each node sequentially to the current version. Test
 * methods are executed after each node upgrade.
 */
@TestCaseOrdering(RollingUpgradeIndexCompatibilityTestCase.TestCaseOrdering.class)
public abstract class RollingUpgradeIndexCompatibilityTestCase extends AbstractIndexCompatibilityTestCase {

    private final List<Version> nodesVersions;

    public RollingUpgradeIndexCompatibilityTestCase(@Name("cluster") List<Version> nodesVersions) {
        this.nodesVersions = nodesVersions;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Stream.of(
            // Begin on N-2
            List.of(VERSION_MINUS_2, VERSION_MINUS_2, VERSION_MINUS_2),
            // Rolling upgrade to VERSION_MINUS_1
            List.of(VERSION_MINUS_1, VERSION_MINUS_2, VERSION_MINUS_2),
            List.of(VERSION_MINUS_1, VERSION_MINUS_1, VERSION_MINUS_2),
            List.of(VERSION_MINUS_1, VERSION_MINUS_1, VERSION_MINUS_1),
            // Rolling upgrade to CURRENT
            List.of(CURRENT, VERSION_MINUS_1, VERSION_MINUS_1),
            List.of(CURRENT, CURRENT, VERSION_MINUS_1),
            List.of(CURRENT, CURRENT, CURRENT)
        ).map(nodesVersion -> new Object[] { nodesVersion }).toList();
    }

    @Override
    protected void maybeUpgrade() throws Exception {
        assertThat(nodesVersions, hasSize(NODES));

        for (int i = 0; i < NODES; i++) {
            var nodeName = cluster().getName(i);

            var expectedNodeVersion = nodesVersions.get(i);
            assertThat(expectedNodeVersion, notNullValue());

            var currentNodeVersion = nodesVersions().get(nodeName);
            assertThat(currentNodeVersion, notNullValue());
            assertThat(currentNodeVersion.onOrBefore(expectedNodeVersion), equalTo(true));

            if (currentNodeVersion.equals(expectedNodeVersion) == false) {
                closeClients();
                cluster().upgradeNodeToVersion(i, expectedNodeVersion);
                initClient();

                ensureHealth((request -> {
                    request.addParameter("timeout", "70s");
                    request.addParameter("wait_for_nodes", String.valueOf(NODES));
                    request.addParameter("wait_for_status", "yellow");
                }));
            }

            currentNodeVersion = nodesVersions().get(nodeName);
            assertThat(currentNodeVersion, equalTo(expectedNodeVersion));
        }
    }

    protected void addAndAssertIndexBlocks(String index, boolean maybeClose) throws Exception {
        final var randomBlocks = randomFrom(
            List.of(IndexMetadata.APIBlock.WRITE, IndexMetadata.APIBlock.READ_ONLY),
            List.of(IndexMetadata.APIBlock.READ_ONLY),
            List.of(IndexMetadata.APIBlock.WRITE)
        );
        for (var randomBlock : randomBlocks) {
            addIndexBlock(index, randomBlock);
            assertThat(indexBlocks(index), hasItem(randomBlock.getBlock()));
        }

        assertThat(indexBlocks(index), maybeClose ? hasItem(INDEX_CLOSED_BLOCK) : not(hasItem(INDEX_CLOSED_BLOCK)));
        assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(maybeClose));
        assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));
        return;
    }

    /**
     * assert that index has either a read-only block or write block, if index is closed also a cloded-block.
     * In case both blocks are present, randomly modify one of them.
     */
    protected void assertAndModifyIndexBlocks(String index, boolean isClosed) throws Exception {
        logger.debug("--> upgraded index [{}] is now in [{}] state", index, isClosed ? "closed" : "open");
        assertThat(
            indexBlocks(index),
            allOf(
                either(hasItem(INDEX_READ_ONLY_BLOCK)).or(hasItem(INDEX_WRITE_BLOCK)),
                isClosed ? hasItem(INDEX_CLOSED_BLOCK) : not(hasItem(INDEX_CLOSED_BLOCK))
            )
        );
        assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(isClosed));
        assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));

        var blocks = indexBlocks(index).stream().filter(c -> c.equals(INDEX_WRITE_BLOCK) || c.equals(INDEX_READ_ONLY_BLOCK)).toList();
        if (blocks.size() == 2) {
            switch (randomInt(2)) {
                case 0:
                    updateIndexSettings(
                        index,
                        Settings.builder()
                            .putNull(IndexMetadata.APIBlock.WRITE.settingName())
                            .put(IndexMetadata.APIBlock.READ_ONLY.settingName(), true)
                    );
                    assertThat(
                        indexBlocks(index),
                        isClosed ? contains(INDEX_CLOSED_BLOCK, INDEX_READ_ONLY_BLOCK) : contains(INDEX_READ_ONLY_BLOCK)
                    );
                    break;
                case 1:
                    updateIndexSettings(
                        index,
                        Settings.builder()
                            .putNull(IndexMetadata.APIBlock.READ_ONLY.settingName())
                            .put(IndexMetadata.APIBlock.WRITE.settingName(), true)
                    );
                    assertThat(
                        indexBlocks(index),
                        isClosed ? contains(INDEX_CLOSED_BLOCK, INDEX_WRITE_BLOCK) : contains(INDEX_WRITE_BLOCK)
                    );
                    break;
                case 2:
                    updateIndexSettings(index, Settings.builder().put(IndexMetadata.APIBlock.READ_ONLY.settingName(), false));
                    assertThat(
                        indexBlocks(index),
                        isClosed ? contains(INDEX_CLOSED_BLOCK, INDEX_WRITE_BLOCK) : contains(INDEX_WRITE_BLOCK)
                    );
                    break;
                default:
                    throw new AssertionError();
            }
        }
    }

    /**
     * ensure we have a write-block. If this is currently not the case, set it.
     */
    protected void ensureWriteBlock(String index, boolean isClosed) throws Exception {
        List<org.elasticsearch.cluster.block.ClusterBlock> blocks = indexBlocks(index).stream()
            .filter(c -> c.equals(INDEX_WRITE_BLOCK) || c.equals(INDEX_READ_ONLY_BLOCK))
            .toList();
        if (blocks.contains(INDEX_READ_ONLY_BLOCK)) {
            logger.debug("--> read_only API block can be replaced by a write block (required for the remaining tests)");
            updateIndexSettings(
                index,
                Settings.builder()
                    .putNull(IndexMetadata.APIBlock.READ_ONLY.settingName())
                    .put(IndexMetadata.APIBlock.WRITE.settingName(), true)
            );
        }

        assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));
        assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(isClosed));
        assertThat(indexBlocks(index), isClosed ? contains(INDEX_CLOSED_BLOCK, INDEX_WRITE_BLOCK) : contains(INDEX_WRITE_BLOCK));
    }

    /**
     * Execute the test suite with the parameters provided by the {@link #parameters()} in nodes versions order.
     */
    public static class TestCaseOrdering implements Comparator<TestMethodAndParams> {
        @Override
        public int compare(TestMethodAndParams o1, TestMethodAndParams o2) {
            List<?> nodesVersions1 = asInstanceOf(List.class, o1.getInstanceArguments().get(0));
            assertThat(nodesVersions1, hasSize(NODES));
            List<?> nodesVersions2 = asInstanceOf(List.class, o2.getInstanceArguments().get(0));
            assertThat(nodesVersions2, hasSize(NODES));
            for (int i = 0; i < NODES; i++) {
                var nodeVersion1 = asInstanceOf(Version.class, nodesVersions1.get(i));
                var nodeVersion2 = asInstanceOf(Version.class, nodesVersions2.get(i));
                var result = nodeVersion1.compareTo(nodeVersion2);
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        }
    }
}
