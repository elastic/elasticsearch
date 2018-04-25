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
package org.elasticsearch.indices.recovery;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class RecoveryStatusTests extends ESSingleNodeTestCase {
    private static final org.apache.lucene.util.Version MIN_SUPPORTED_LUCENE_VERSION = org.elasticsearch.Version.CURRENT
        .minimumIndexCompatibilityVersion().luceneVersion;
    public void testRenameTempFiles() throws IOException {
        IndexService service = createIndex("foo");

        IndexShard indexShard = service.getShardOrNull(0);
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        RecoveryTarget status = new RecoveryTarget(indexShard, node, new PeerRecoveryTargetService.RecoveryListener() {
            @Override
            public void onRecoveryDone(RecoveryState state) {
            }

            @Override
            public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {
            }
        }, version -> {});
        try (IndexOutput indexOutput = status.openAndPutIndexOutput("foo.bar", new StoreFileMetaData("foo.bar", 8 + CodecUtil.footerLength()
            , "9z51nw", MIN_SUPPORTED_LUCENE_VERSION), status.store())) {
            indexOutput.writeInt(1);
            IndexOutput openIndexOutput = status.getOpenIndexOutput("foo.bar");
            assertSame(openIndexOutput, indexOutput);
            openIndexOutput.writeInt(1);
            CodecUtil.writeFooter(indexOutput);
        }

        try {
            status.openAndPutIndexOutput("foo.bar", new StoreFileMetaData("foo.bar", 8 + CodecUtil.footerLength(), "9z51nw",
                MIN_SUPPORTED_LUCENE_VERSION), status.store());
            fail("file foo.bar is already opened and registered");
        } catch (IllegalStateException ex) {
            assertEquals("output for file [foo.bar] has already been created", ex.getMessage());
            // all well = it's already registered
        }
        status.removeOpenIndexOutputs("foo.bar");
        Set<String> strings = Sets.newHashSet(status.store().directory().listAll());
        String expectedFile = null;
        for (String file : strings) {
            if (Pattern.compile("recovery[.][\\w-]+[.]foo[.]bar").matcher(file).matches()) {
                expectedFile = file;
                break;
            }
        }
        assertNotNull(expectedFile);
        indexShard.close("foo", false);// we have to close it here otherwise rename fails since the write.lock is held by the engine
        status.renameAllTempFiles();
        strings = Sets.newHashSet(status.store().directory().listAll());
        assertTrue(strings.toString(), strings.contains("foo.bar"));
        assertFalse(strings.toString(), strings.contains(expectedFile));
        // we must fail the recovery because marking it as done will try to move the shard to POST_RECOVERY, which will fail because it's started
        status.fail(new RecoveryFailedException(status.state(), "end of test. OK.", null), false);
    }
}
