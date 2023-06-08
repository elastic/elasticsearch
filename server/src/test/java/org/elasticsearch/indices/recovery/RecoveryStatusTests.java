/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices.recovery;

import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Pattern;

public class RecoveryStatusTests extends ESSingleNodeTestCase {
    private static final Version MIN_SUPPORTED_LUCENE_VERSION = IndexVersion.MINIMUM_COMPATIBLE.luceneVersion();

    public void testRenameTempFiles() throws IOException {
        IndexService service = createIndex("foo");

        IndexShard indexShard = service.getShardOrNull(0);
        MultiFileWriter multiFileWriter = new MultiFileWriter(
            indexShard.store(),
            indexShard.recoveryState().getIndex(),
            "recovery.test.",
            logger,
            () -> {}
        );
        try (
            IndexOutput indexOutput = multiFileWriter.openAndPutIndexOutput(
                "foo.bar",
                new StoreFileMetadata("foo.bar", 8 + CodecUtil.footerLength(), "9z51nw", MIN_SUPPORTED_LUCENE_VERSION.toString()),
                indexShard.store()
            )
        ) {
            EndiannessReverserUtil.wrapDataOutput(indexOutput).writeInt(1);
            IndexOutput openIndexOutput = multiFileWriter.getOpenIndexOutput("foo.bar");
            assertSame(openIndexOutput, indexOutput);
            EndiannessReverserUtil.wrapDataOutput(openIndexOutput).writeInt(1);
            CodecUtil.writeFooter(indexOutput);
        }

        try {
            multiFileWriter.openAndPutIndexOutput(
                "foo.bar",
                new StoreFileMetadata("foo.bar", 8 + CodecUtil.footerLength(), "9z51nw", MIN_SUPPORTED_LUCENE_VERSION.toString()),
                indexShard.store()
            );
            fail("file foo.bar is already opened and registered");
        } catch (IllegalStateException ex) {
            assertEquals("output for file [foo.bar] has already been created", ex.getMessage());
            // all well = it's already registered
        }
        multiFileWriter.removeOpenIndexOutputs("foo.bar");
        Set<String> strings = Sets.newHashSet(indexShard.store().directory().listAll());
        String expectedFile = null;
        for (String file : strings) {
            if (Pattern.compile("recovery[.][\\w-]+[.]foo[.]bar").matcher(file).matches()) {
                expectedFile = file;
                break;
            }
        }
        assertNotNull(expectedFile);
        indexShard.close("foo", false);// we have to close it here otherwise rename fails since the write.lock is held by the engine
        multiFileWriter.renameAllTempFiles();
        strings = Sets.newHashSet(indexShard.store().directory().listAll());
        assertTrue(strings.toString(), strings.contains("foo.bar"));
        assertFalse(strings.toString(), strings.contains(expectedFile));
        multiFileWriter.close();
    }
}
