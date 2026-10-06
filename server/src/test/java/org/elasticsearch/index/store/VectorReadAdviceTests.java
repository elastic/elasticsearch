/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoReuseHint;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.Constants;
import org.elasticsearch.index.codec.vectors.VectorReadHintsTests;
import org.elasticsearch.index.codec.vectors.VectorReadHintsTests.Case;
import org.elasticsearch.index.codec.vectors.VectorReadHintsTests.Open;
import org.elasticsearch.index.mapper.MapperServiceTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * What each vectors file says when it is opened or written, and the advice the directory derives for search. The format says
 * whether its raw vectors are reused, and only raw vectors kept to rescore are not; whatever reads or writes a file says how.
 */
public class VectorReadAdviceTests extends MapperServiceTestCase {

    private static final Optional<ReadAdvice> UNADVISED = Optional.of(Constants.DEFAULT_READADVICE);

    public void testOnlyRawVectorsKeptToRescoreAreAdvisedForSearch() throws IOException {
        var advice = FsDirectoryFactory.getReadAdviceFunc();
        for (Case each : VectorReadHintsTests.cases(this)) {
            for (Open open : VectorReadHintsTests.searchOpens(each.codec())) {
                if (open.isVectorData() == false) {
                    continue;
                }
                Optional<ReadAdvice> expected = open.isRawVectors() && each.rescoresFromRaw() ? Optional.of(ReadAdvice.RANDOM) : UNADVISED;
                assertEquals(each + ": " + open, expected, advice.apply(open.name(), open.context()));
            }
        }
    }

    /**
     * A merge reads the raw vectors it copies through a mapping of its own, front to back, and says they are not reused only
     * when the format does. The raw vectors written, by a flush or a merge, say the same, and so does a graph build reading
     * them back.
     */
    public void testAMergeSaysHowItReadsAndWritesEachFile() throws IOException {
        List<String> failures = new ArrayList<>();
        for (Case each : VectorReadHintsTests.cases(this)) {
            if (each.walkedByGraph() == false && each.rescoresFromRaw() == false) {
                continue;
            }
            List<Open> opens = new ArrayList<>();
            List<Open> creates = new ArrayList<>();
            List<Open> flushCreates = new ArrayList<>();
            Set<String> sourceFiles = new HashSet<>();
            try (Directory dir = new RecordingDirectory(newDirectory(), opens, creates)) {
                IndexWriterConfig iwc = new IndexWriterConfig().setCodec(each.codec()).setUseCompoundFile(false);
                try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                    indexTwoSegments(writer);
                    // a search holds the segments open, so the merge reads through the readers searches use
                    try (DirectoryReader reader = DirectoryReader.open(writer)) {
                        for (LeafReaderContext leaf : reader.leaves()) {
                            sourceFiles.addAll(((SegmentReader) leaf.reader()).getSegmentInfo().files());
                        }
                        synchronized (opens) {
                            flushCreates.addAll(creates);
                            opens.clear();
                            creates.clear();
                        }
                        writer.forceMerge(1);
                    }
                }
            }
            synchronized (opens) {
                checkMerge(each, sourceFiles, opens, creates, flushCreates, failures);
            }
        }
        assertEquals(List.of(), failures);
    }

    private static void checkMerge(
        Case each,
        Set<String> sourceFiles,
        List<Open> opens,
        List<Open> creates,
        List<Open> flushCreates,
        List<String> failures
    ) {
        boolean noReuse = each.rescoresFromRaw();
        // Lucene's flat reader says its merge mapping is not reused whatever the format said, until apache/lucene#16769
        boolean mergeNoReuse = noReuse || each.name().endsWith(" over float");

        List<String> sourceRaw = sourceFiles.stream().filter(name -> name.endsWith(".vec")).sorted().toList();
        if (sourceRaw.size() != 2) {
            failures.add(each + ": expected two source raw vectors files in " + sourceFiles);
        }
        for (String raw : sourceRaw) {
            List<Open> streams = opens.stream()
                .filter(o -> o.name().equals(raw) && o.context().context() == IOContext.Context.MERGE)
                .filter(o -> o.context().hints().contains(DataAccessHint.SEQUENTIAL))
                .toList();
            if (streams.isEmpty()) {
                failures.add(each + ": the merge never mapped " + raw + " to read it front to back");
            }
            for (Open stream : streams) {
                if (stream.context().hints().contains(NoReuseHint.INSTANCE) != mergeNoReuse) {
                    failures.add(each + ": the merge mapping of " + raw + " says " + stream.context().hints());
                }
            }
        }

        // a graph build reads the merged vectors back at random, saying what the format says about reuse
        for (Open open : opens) {
            if (open.isRawVectors()
                && sourceFiles.contains(open.name()) == false
                && open.context().hints().contains(DataAccessHint.RANDOM)
                && open.context().hints().contains(NoReuseHint.INSTANCE) != noReuse) {
                failures.add(each + ": the merge read back " + open.name() + " with " + open.context().hints());
            }
        }

        // the raw vectors written say what they are read with
        List<Open> written = new ArrayList<>(flushCreates);
        written.addAll(creates);
        for (Open created : written) {
            if (created.isRawVectors() == false || each.name().startsWith("ES816")) {
                // segments of ES816 are only read: the test writes them with a writer of its own
                continue;
            }
            var hints = created.context().hints();
            boolean streamed = hints.contains(NoReuseHint.INSTANCE) && hints.contains(DataAccessHint.SEQUENTIAL);
            if (streamed != noReuse || hints.contains(NoReuseHint.INSTANCE) != noReuse) {
                failures.add(each + ": " + created.name() + " was written with " + hints);
            }
        }
    }

    private static void indexTwoSegments(IndexWriter writer) throws IOException {
        for (int segment = 0; segment < 2; segment++) {
            for (int i = 0; i < 256; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("field", VectorReadHintsTests.randomVector(), VectorSimilarityFunction.EUCLIDEAN));
                writer.addDocument(doc);
            }
            writer.flush();
        }
    }

    /** Records every open and every file created, in order; inputs are returned as opened, native scorers read the mapping. */
    private static class RecordingDirectory extends FilterDirectory {
        private final List<Open> opens;
        private final List<Open> creates;

        /** Both lists are guarded by {@code opens}. */
        RecordingDirectory(Directory in, List<Open> opens, List<Open> creates) {
            super(in);
            this.opens = opens;
            this.creates = creates;
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            record(creates, name, context);
            return super.createOutput(name, context);
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            IndexOutput output = super.createTempOutput(prefix, suffix, context);
            record(creates, output.getName(), context);
            return output;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            record(opens, name, context);
            return super.openInput(name, context);
        }

        private void record(List<Open> to, String name, IOContext context) {
            synchronized (opens) {
                to.add(new Open(name, context));
            }
        }
    }
}
