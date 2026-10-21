/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfFlushConfigSource;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.es95.ES950DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93FlatVectorFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswBinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswVectorsFormat;
import org.elasticsearch.index.codec.vectors.es94.ES94HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

/**
 * Tests that formats based on {@link DirectIOCapableFlatVectorsFormat} ask for direct I/O with the right
 * {@link IOContext} for the right files, and that nothing else asks for it at all. The contexts are what is
 * asserted, so this runs without direct I/O support, the directory then falling back to buffered I/O; turning a
 * context into direct I/O is FsDirectoryFactoryTests' contract, and what the merge leaves behind is
 * {@link DirectIOMergeRoundTripTests}', where a format added to the table below may also need a case.
 */
public class DirectIOCapableFlatVectorsFormatTests extends BaseDirectIOMergeTestCase {

    /**
     * A format under test with what its merges are expected to do. The format decides; the expectations only
     * select which assertions apply.
     *
     * @param formatWith          builds the format with the given {@code on_disk_merge} flag; the {@code on_disk_rescore}
     *                            choice is baked in
     * @param onDiskMerge         the flag the format under test is built with
     * @param searchDirectIOReads searches open the raw vector data with direct I/O ({@code on_disk_rescore})
     * @param mergeDirectIOReads  a merge reads its sources through the merge-sized direct I/O reader
     * @param mergeDirectIOWrites a merge creates the merged raw vector data with direct I/O
     */
    record Case(
        String name,
        Function<Boolean, KnnVectorsFormat> formatWith,
        boolean onDiskMerge,
        boolean searchDirectIOReads,
        boolean mergeDirectIOReads,
        boolean mergeDirectIOWrites
    ) {
        KnnVectorsFormat format() {
            return formatWith.apply(onDiskMerge);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Stream.of(
            // the scalar-quantized reader chain does not currently propagate getMergeInstance down to the raw reader,
            // so merges read the sources through the page cache; merge-time direct I/O writes do not depend on getMergeInstance
            new Case(
                "int8_hnsw, on_disk_rescore, on_disk_merge",
                odm -> new ES94HnswScalarQuantizedVectorsFormat(16, 100, ElementType.FLOAT, 7, true, 1, null, 0, odm),
                true,
                true,
                false,
                true
            ),
            // on_disk_rescore on, on_disk_merge on: direct I/O everywhere, the bbq reader chain propagating getMergeInstance
            new Case(
                "bbq_hnsw, on_disk_rescore, on_disk_merge",
                odm -> new ES93HnswBinaryQuantizedVectorsFormat(16, 100, ElementType.FLOAT, true, 1, null, 0, odm),
                true,
                true,
                true,
                true
            ),
            new Case(
                "bbq_hnsw, neither",
                odm -> new ES93HnswBinaryQuantizedVectorsFormat(16, 100, ElementType.FLOAT, false, 1, null, 0, odm),
                false,
                false,
                false,
                false
            ),
            // the two decisions are independent: on_disk_merge alone here, on_disk_rescore alone in the next case
            new Case(
                "bbq_hnsw, on_disk_merge",
                odm -> new ES93HnswBinaryQuantizedVectorsFormat(16, 100, ElementType.FLOAT, false, 1, null, 0, odm),
                true,
                false,
                true,
                true
            ),
            new Case(
                "bbq_hnsw, on_disk_rescore",
                odm -> new ES93HnswBinaryQuantizedVectorsFormat(16, 100, ElementType.FLOAT, true, 1, null, 0, odm),
                false,
                true,
                false,
                false
            ),
            // bbq_disk holds its raw vector format directly rather than through the generic wrapper, so it is the format
            // that would get the read side without the write side if the two were not tied together at the raw format
            new Case(
                "bbq_disk, on_disk_merge",
                odm -> new ES950DiskBBQVectorsFormat(
                    QuantEncoding.ONE_BIT_4BIT_QUERY,
                    64,
                    ES950DiskBBQVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
                    ElementType.FLOAT,
                    false,
                    null,
                    1,
                    false,
                    ES950DiskBBQVectorsFormat.DEFAULT_PRECONDITIONING_BLOCK_DIMENSION,
                    ES950DiskBBQVectorsFormat.defaultFlatThreshold(64),
                    IvfFlushConfigSource.empty(),
                    IvfMergeConfigResolver.useCodecDefault(),
                    odm
                ),
                true,
                false,
                true,
                true
            ),
            // plain HNSW declines the write side, see ES93GenericFlatVectorsFormat#withBufferedMergeWrites; the graph
            // threshold of 0 makes the merge build a graph at this size, so the read-back it protects does happen. The
            // decline does not depend on the element type, so one row covers it: bfloat16, the only case that opens its
            // own raw reader from a direct I/O merge context
            new Case(
                "hnsw bfloat16, on_disk_merge",
                odm -> new ES93HnswVectorsFormat(16, 100, ElementType.BFLOAT16, 1, null, 0, odm),
                true,
                false,
                true,
                false
            ),
            // as with int8_hnsw above, no getMergeInstance override, so the flat type's merges read the sources through the
            // page cache; bfloat16 is the element type with a raw writer of its own, float32's is the one the rows above drive
            new Case("flat bfloat16, on_disk_merge", odm -> new ES93FlatVectorFormat(ElementType.BFLOAT16, odm), true, false, false, true)
        ).map(c -> new Object[] { c }).toList();
    }

    private final Case testCase;

    public DirectIOCapableFlatVectorsFormatTests(@Name("format") Case testCase) {
        this.testCase = testCase;
    }

    public void testMergeAsksForTheExpectedIOContexts() throws IOException {
        try (
            IORecordingDirectory dir = newRecordingDirectory(createTempDir("directIOMerge"));
            IndexWriter writer = new IndexWriter(dir, newConfig(testCase.format()))
        ) {
            addSegment(writer, 64);
            addSegment(writer, 64);
            mergeWithReaderOpen(writer);

            List<FileIO> opens = dir.recorded.stream().filter(io -> io.op() == Op.OPEN).toList();
            List<FileIO> vecOpens = opens.stream().filter(io -> io.name().endsWith(".vec")).toList();
            assertThat("expected at least one open of a raw vector file", vecOpens, not(empty()));
            if (testCase.searchDirectIOReads()) {
                assertTrue(
                    "expected the search-time reader to open the raw vector file with direct IO",
                    vecOpens.stream().anyMatch(o -> o.context() == IOContext.Context.DEFAULT && o.directIO())
                );
                if (testCase.mergeDirectIOReads()) {
                    assertTrue(
                        "a search-time (DEFAULT context) reader opened a raw vector file without requesting direct IO",
                        vecOpens.stream().filter(o -> o.context() == IOContext.Context.DEFAULT).allMatch(FileIO::directIO)
                    );
                } else if (testCase.onDiskMerge() == false) {
                    // the merge must not borrow the random-access direct I/O reader: it opens a plain reader of its
                    // own from the pooled (DEFAULT context) reader's state. The graph build's read-back of the merged
                    // output is a MERGE-context open and does not count
                    assertTrue(
                        "expected the merge to open a source raw vector file through a plain (non direct IO) reader",
                        vecOpens.stream().anyMatch(o -> o.context() == IOContext.Context.DEFAULT && o.directIO() == false)
                    );
                }
            } else {
                assertTrue(
                    "did not expect any search-time (DEFAULT context) open to request direct IO",
                    opens.stream().noneMatch(o -> o.context() == IOContext.Context.DEFAULT && o.directIO())
                );
            }
            if (testCase.mergeDirectIOReads()) {
                assertTrue(
                    "expected the merge to open a raw vector file with a merge-hinted direct IO context",
                    vecOpens.stream().anyMatch(FileIO::mergeDirectIO)
                );
            } else {
                assertTrue("did not expect any merge-hinted direct IO open", opens.stream().noneMatch(FileIO::mergeDirectIO));
            }
            assertTrue(
                "a file other than the raw vector data file was opened with a direct IO hint",
                opens.stream().filter(FileIO::directIO).allMatch(o -> o.name().endsWith(".vec"))
            );

            List<FileIO> creates = dir.recorded.stream().filter(io -> io.op() == Op.CREATE).toList();
            // only a merge hints, and only the raw vector file and its metadata sibling (which HybridDirectory keeps
            // buffered): flush segments, quantized vectors, HNSW graph, format metadata and temp files stay page-cache-warm
            assertTrue(
                "a file other than a merge-time raw vector data/meta output was created with a direct IO hint",
                creates.stream()
                    .filter(FileIO::directIO)
                    .allMatch(c -> c.mergeDirectIO() && (c.name().endsWith(".vec") || c.name().endsWith(".vemf")))
            );
            if (testCase.mergeDirectIOWrites()) {
                assertTrue(
                    "expected the merge to create the raw vector data file with a merge-hinted direct IO context",
                    creates.stream().anyMatch(c -> c.name().endsWith(".vec") && c.mergeDirectIO())
                );
            } else {
                assertTrue("did not expect any output to be created with a direct IO hint", creates.stream().noneMatch(FileIO::directIO));
            }
        }
    }
}
