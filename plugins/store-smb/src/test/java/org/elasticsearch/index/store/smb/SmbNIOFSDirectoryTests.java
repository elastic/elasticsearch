/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store.smb;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.index.store.EsBaseDirectoryTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class SmbNIOFSDirectoryTests extends EsBaseDirectoryTestCase {

    @Override
    protected Directory getDirectory(Path file) throws IOException {
        return new SmbDirectoryWrapper(new NIOFSDirectory(file));
    }

    @Override
    public void testCreateOutputForExistingFile() throws IOException {
        /**
         * This test is disabled because {@link SmbDirectoryWrapper} opens existing file
         * with an explicit StandardOpenOption.TRUNCATE_EXISTING option.
         */
    }

    public void testCopyFromTracksFileViaCreateOutput() throws IOException {
        try (Directory sourceDir = newDirectory(); Directory dir = getDirectory(createTempDir())) {
            try (IndexOutput out = sourceDir.createOutput("src", IOContext.DEFAULT)) {
                out.writeBytes(new byte[8], 8);
            }
            dir.copyFrom(sourceDir, "src", "dest", IOContext.DEFAULT);
            assertTrue(List.of(dir.listAll()).contains("dest"));
        }
    }

    public void testCopyFromCleansUpDestOnFailure() throws IOException {
        try (Directory sourceDir = newDirectory(); Directory dir = getDirectory(createTempDir())) {
            try (IndexOutput out = sourceDir.createOutput("src", IOContext.DEFAULT)) {
                out.writeBytes(new byte[8], 8);
            }
            Directory failingSource = new FilterDirectory(sourceDir) {
                @Override
                public IndexInput openInput(String name, IOContext context) throws IOException {
                    throw new IOException("simulated read failure");
                }
            };
            expectThrows(IOException.class, () -> dir.copyFrom(failingSource, "src", "dest", IOContext.DEFAULT));
            assertFalse("dest should not exist after a failed copy", List.of(dir.listAll()).contains("dest"));
        }
    }
}
