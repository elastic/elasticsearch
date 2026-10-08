/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

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
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/** The hints each vectors file is opened and written with, and the advice the directory derives for search. */
public class VectorReadAdviceTests extends MapperServiceTestCase {

    private static final Optional<ReadAdvice> UNADVISED = Optional.of(Constants.DEFAULT_READADVICE);

    private final Case each;

    public VectorReadAdviceTests(Case each) {
        this.each = each;
    }

    @ParametersFactory(argumentFormatting = "%s")
    public static Iterable<Object[]> parameters() {
        return VectorReadHintsTests.parameters();
    }

    public void testOnlyRawVectorsKeptToRescoreAreAdvisedForSearch() throws IOException {
        var advice = FsDirectoryFactory.getReadAdviceFunc();
        for (Open open : VectorReadHintsTests.searchOpens(each.codec(this))) {
            if (open.isVectorData() == false) {
                continue;
            }
            Optional<ReadAdvice> expected = open.isRawVectors() && each.rescoresFromRaw() ? Optional.of(ReadAdvice.RANDOM) : UNADVISED;
            assertThat(open.toString(), advice.apply(open.name(), open.context()), equalTo(expected));
        }
    }

    /**
     * A merge maps the source raw vectors again to read them sequentially, not reused only when the format says so; the raw
     * vectors written and the files read back say the same.
     */
    public void testAMergeSaysHowItReadsAndWritesEachFile() throws IOException {
        assumeTrue("nothing reads the raw vectors at random", each.walkedByGraph() || each.rescoresFromRaw());
        List<Open> opens = new CopyOnWriteArrayList<>();
        List<Open> creates = new CopyOnWriteArrayList<>();
        List<Open> flushCreates = new ArrayList<>();
        Set<String> sourceFiles = new HashSet<>();
        try (Directory dir = new RecordingDirectory(newDirectory(), opens, creates)) {
            IndexWriterConfig iwc = new IndexWriterConfig().setCodec(each.codec(this)).setUseCompoundFile(false);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                indexTwoSegments(writer);
                // a search holds the segments open, so the merge reads through the readers searches use
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    for (LeafReaderContext leaf : reader.leaves()) {
                        sourceFiles.addAll(((SegmentReader) leaf.reader()).getSegmentInfo().files());
                    }
                    flushCreates.addAll(creates);
                    opens.clear();
                    creates.clear();
                    writer.forceMerge(1);
                }
            }
        }
        List<String> failures = new ArrayList<>();
        checkMerge(each, sourceFiles, opens, creates, flushCreates, failures);
        assertThat(failures, empty());
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
                if (stream.context().hints().contains(NoReuseHint.INSTANCE) != noReuse) {
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

        // a disk BBQ merge writes and reads back temp files: only its copy of the raw vectors is not reused
        for (Open open : opens) {
            checkTempFile(each, open, false, failures);
        }
        for (Open created : creates) {
            checkTempFile(each, created, true, failures);
        }
        Set<DataAccessHint> rawCopyReads = new HashSet<>();
        for (Open open : opens) {
            if (open.name().contains("_ivfvec_")) {
                open.context().hints(DataAccessHint.class).forEach(rawCopyReads::add);
            }
        }
        if (opens.stream().anyMatch(o -> o.name().contains("_ivfvec_"))
            && rawCopyReads.equals(Set.of(DataAccessHint.SEQUENTIAL, DataAccessHint.RANDOM)) == false) {
            failures.add(each + ": the merge did not stream its copy of the raw vectors to cluster and read it at random after");
        }
    }

    private static void checkTempFile(Case each, Open open, boolean created, List<String> failures) {
        if (isVectorTempFile(open.name()) == false) {
            return;
        }
        var hints = open.context().hints();
        if (open.context().context() != IOContext.Context.MERGE) {
            failures.add(each + ": the merge used " + open.name() + " without its merge context");
        }
        if (hints.contains(NoReuseHint.INSTANCE) != open.name().contains("_ivfvec_")) {
            failures.add(each + ": " + open.name() + " says " + hints);
        }
        if (created ? hints.contains(DataAccessHint.SEQUENTIAL) == false : open.context().hints(DataAccessHint.class).findAny().isEmpty()) {
            failures.add(each + ": " + open.name() + " does not say how it is " + (created ? "written" : "read") + ": " + hints);
        }
    }

    /** The temp files a disk BBQ merge writes and reads back. */
    private static boolean isVectorTempFile(String name) {
        return name.endsWith(".tmp")
            && (name.contains("_ivfvec_") || name.contains("_ivfdoc_") || name.contains("_civf_") || name.contains("_qvec_"));
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

    /** Records every open and created file, in order; inputs are returned unwrapped so native scorers read the mapping. */
    private static class RecordingDirectory extends FilterDirectory {
        private final List<Open> opens;
        private final List<Open> creates;

        RecordingDirectory(Directory in, List<Open> opens, List<Open> creates) {
            super(in);
            this.opens = opens;
            this.creates = creates;
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            creates.add(new Open(name, context));
            return super.createOutput(name, context);
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            IndexOutput output = super.createTempOutput(prefix, suffix, context);
            creates.add(new Open(output.getName(), context));
            return output;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            opens.add(new Open(name, context));
            return super.openInput(name, context);
        }
    }
}
