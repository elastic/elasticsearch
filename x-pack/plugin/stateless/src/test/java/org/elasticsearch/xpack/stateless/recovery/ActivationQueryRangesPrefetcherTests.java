/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.cache.Lucene90CompoundEntriesReader.FileEntry;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.recovery.ActivationQueryRangesPrefetcher.Candidate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ActivationQueryRangesPrefetcherTests extends ESTestCase {

    private static final BlobFile BLOB = new BlobFile("stateless_commit_1", new PrimaryTermAndGeneration(1, 1));

    private static Map<String, BlobLocation> files(Object... nameAndLength) {
        final Map<String, BlobLocation> files = new HashMap<>();
        long offset = 0;
        for (int i = 0; i < nameAndLength.length; i += 2) {
            final long length = ((Number) nameAndLength[i + 1]).longValue();
            files.put((String) nameAndLength[i], new BlobLocation(BLOB, offset, length));
            offset += length;
        }
        return files;
    }

    public void testSelectsByExtensionPrioritySmallestFirstWithinTheBound() throws IOException {
        final Map<String, BlobLocation> commitFiles = files(
            "_0.tim",
            5000,
            "_0.tip",
            300,
            "_0.doc",
            100,
            "_1.tip",
            200,
            "_0.fdx",
            64,
            "_0.kdi",
            900,
            "_0.dvd",
            10,
            "_1.tim",
            100_000
        );
        final List<Candidate> all = ActivationQueryRangesPrefetcher.select(
            commitFiles,
            cfe -> { throw new AssertionError("no compound file"); },
            Long.MAX_VALUE
        );
        assertThat(
            all.stream().map(Candidate::fileName).toList(),
            equalTo(List.of("_1.tip", "_0.tip", "_0.fdx", "_0.kdi", "_0.tim", "_1.tim"))
        );
        // _0.tim does not fit after the first four (1464 bytes) but nothing after it fits either
        final List<Candidate> bounded = ActivationQueryRangesPrefetcher.select(
            commitFiles,
            cfe -> { throw new AssertionError("no compound file"); },
            2000
        );
        assertThat(bounded.stream().map(Candidate::fileName).toList(), equalTo(List.of("_1.tip", "_0.tip", "_0.fdx", "_0.kdi")));
        assertThat(ActivationQueryRangesPrefetcher.select(commitFiles, cfe -> Map.of(), 0).size(), equalTo(0));
    }

    public void testSelectsCompoundFileEntries() throws IOException {
        final Map<String, BlobLocation> commitFiles = files("_2.cfe", 200, "_2.cfs", 50_000, "_2.si", 300);
        final List<Candidate> selected = ActivationQueryRangesPrefetcher.select(commitFiles, cfe -> {
            assertThat(cfe, equalTo("_2.cfe"));
            return Map.of(
                "_2_Lucene103_0.tip",
                new FileEntry(100, 400),
                "_2_Lucene103_0.doc",
                new FileEntry(500, 20_000),
                "_2.fdx",
                new FileEntry(20_500, 120),
                "_2_Lucene90_0.kdi",
                new FileEntry(20_620, 80)
            );
        }, 1000);
        assertThat(
            selected,
            equalTo(
                List.of(
                    new Candidate("_2.cfs", LuceneFilesExtensions.TIP, 100, 400),
                    new Candidate("_2.cfs", LuceneFilesExtensions.FDX, 20_500, 120),
                    new Candidate("_2.cfs", LuceneFilesExtensions.KDI, 20_620, 80)
                )
            )
        );
    }

    public void testPrefetchesTheSelectedRangesThroughTheDirectory() throws IOException {
        final List<String> prefetches = new ArrayList<>();
        try (Directory in = new ByteBuffersDirectory()) {
            for (String name : List.of("_0.tip", "_0.kdi", "_0.doc")) {
                try (IndexOutput out = in.createOutput(name, IOContext.DEFAULT)) {
                    out.writeBytes(new byte[100], 100);
                }
            }
            final Directory directory = new FilterDirectory(in) {
                @Override
                public IndexInput openInput(String name, IOContext context) throws IOException {
                    return new FilterIndexInput("prefetch-recording", super.openInput(name, context)) {
                        @Override
                        public void prefetch(long offset, long length) {
                            prefetches.add(name + ":" + offset + ":" + length);
                        }
                    };
                }
            };
            final long requested = ActivationQueryRangesPrefetcher.prefetch(
                directory,
                files("_0.tip", 100, "_0.kdi", 100, "_0.doc", 100),
                150
            );
            assertThat(requested, equalTo(100L));
            assertThat(prefetches, equalTo(List.of("_0.tip:0:100")));
            assertThat(ActivationQueryRangesPrefetcher.prefetch(directory, files("_0.tip", 100), 0), equalTo(0L));
        }
    }
}
