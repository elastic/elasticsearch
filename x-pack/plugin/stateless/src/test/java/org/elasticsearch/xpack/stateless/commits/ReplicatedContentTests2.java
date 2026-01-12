/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.cache.Lucene90CompoundEntriesReader;
import co.elastic.elasticsearch.stateless.commits.InternalFilesReplicatedRanges.InternalFileReplicatedRange;
import co.elastic.elasticsearch.stateless.commits.ReplicatedContent.InternalFileRangeReader;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.InternalFile;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.store.BytesReferenceIndexInput;
import org.elasticsearch.core.Streams;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.stateless.commits.InternalFilesReplicatedRanges.REPLICATED_CONTENT_FOOTER_SIZE;
import static co.elastic.elasticsearch.stateless.commits.InternalFilesReplicatedRanges.REPLICATED_CONTENT_HEADER_SIZE;
import static co.elastic.elasticsearch.stateless.commits.ReplicatedContent.ALWAYS_REPLICATE;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

public class ReplicatedContentTests extends ESTestCase {

    public void testReplicatesContent() throws IOException {
        try (var directory = LuceneTestCase.newDirectory()) {
            var smallFile = new InternalFile(
                "small-file",
                randomLongBetween(1, REPLICATED_CONTENT_HEADER_SIZE + REPLICATED_CONTENT_FOOTER_SIZE)
            );
            var bigFile = new InternalFile(
                "big-file",
                randomLongBetween(REPLICATED_CONTENT_HEADER_SIZE + REPLICATED_CONTENT_FOOTER_SIZE + 1, Long.MAX_VALUE)
            );
            var content = ReplicatedContent.create(true, List.of(smallFile, bigFile), directory, ALWAYS_REPLICATE);

            assertThat(
                content.header(),
                equalTo(
                    new InternalFilesReplicatedRanges(
                        List.of(
                            // first small file is merged with the following big file header
                            new InternalFileReplicatedRange(0, (short) (smallFile.length() + REPLICATED_CONTENT_HEADER_SIZE)),
                            new InternalFileReplicatedRange(
                                smallFile.length() + bigFile.length() - REPLICATED_CONTENT_FOOTER_SIZE,
                                REPLICATED_CONTENT_FOOTER_SIZE
                            )
                        ),
                        smallFile.length() + REPLICATED_CONTENT_HEADER_SIZE + REPLICATED_CONTENT_FOOTER_SIZE
                    )
                )
            );
            assertThat(
                content.readers(),
                contains(
                    new InternalFileRangeReader(smallFile.name(), directory, 0, smallFile.length()),
                    new InternalFileRangeReader(bigFile.name(), directory, 0, REPLICATED_CONTENT_HEADER_SIZE),
                    new InternalFileRangeReader(
                        bigFile.name(),
                        directory,
                        bigFile.length() - REPLICATED_CONTENT_FOOTER_SIZE,
                        REPLICATED_CONTENT_FOOTER_SIZE
                    )
                )
            );
        }
    }

    public void testReplicatesNonCompoundFileContent() throws IOException {
        try (var directory = LuceneTestCase.newDirectory()) {
            var conf = new IndexWriterConfig().setUseCompoundFile(false);
            try (var writer = new IndexWriter(directory, conf)) {
                for (int i = 0; i < randomIntBetween(50, 100); i++) {
                    writer.addDocument(createDocument());
                }
            }

            assertThat(List.of(directory.listAll()), not(hasItems("_0.cfe", "_0.cfs")));
            var internalFiles = createInternalFilesFrom(directory);
            var content = ReplicatedContent.create(true, internalFiles, directory, ALWAYS_REPLICATE);

            var smallFilesCount = internalFiles.stream()
                .filter(file -> file.length() <= REPLICATED_CONTENT_HEADER_SIZE + REPLICATED_CONTENT_FOOTER_SIZE)
                .count();
            var bigFilesCount = internalFiles.stream()
                .filter(file -> file.length() > REPLICATED_CONTENT_HEADER_SIZE + REPLICATED_CONTENT_FOOTER_SIZE)
                .count();

            // some ranges are merged, so overall number of ranges should be smaller than total readers
            assertThat(content.header().replicatedRanges().size(), lessThan((int) (smallFilesCount + 2 * bigFilesCount)));
            assertThat(content.readers(), hasSize((int) (smallFilesCount + 2 * bigFilesCount)));

            verifyReplicatedContent(content);
        }
    }

    public void testReplicatesCompoundFileContent() throws IOException {
        try (var directory = LuceneTestCase.newDirectory()) {
            var conf = new IndexWriterConfig().setUseCompoundFile(true);
            try (var writer = new IndexWriter(directory, conf)) {
                for (int i = 0; i < randomIntBetween(50, 100); i++) {
                    writer.addDocument(createDocument());
                }
            }

            assertThat(List.of(directory.listAll()), hasItems("_0.cfe", "_0.cfs"));
            assertThat(directory.fileLength("_0.cfs"), greaterThan(1024L + 16L));
            var internalFiles = createInternalFilesFrom(directory);
            var compoundSegmentsFileOffset = fileOffsetIn(internalFiles, "_0.cfs");
            var content = ReplicatedContent.create(true, internalFiles, directory, ALWAYS_REPLICATE);
            var compoundEntries = Lucene90CompoundEntriesReader.readEntries(directory, "_0.cfe").values();

            // max number of ranges that should be replicated
            // the number is 2 (header and footer of the top level file, assuming it is > 1024+16 bytes) + ranges of every nested file
            var replicatedRangesInCompoundSegment = 2 + (int) compoundEntries.stream()
                .mapToLong(entry -> entry.length() > REPLICATED_CONTENT_HEADER_SIZE + REPLICATED_CONTENT_FOOTER_SIZE ? 2 : 1)
                .sum();
            var compoundSegmentReaders = content.readers().stream().filter(reader -> Objects.equals(reader.filename(), "_0.cfs")).toList();

            assertThat(
                compoundSegmentReaders.size(),
                allOf(
                    greaterThan(2), // more that a single header and footer
                    lessThan(replicatedRangesInCompoundSegment) // less than total count since some ranges are merged
                )
            );

            // we replicate first 1024 bytes of every file
            // with compound file this means that first entry (or even entries) are likely going to be present in this range.
            // this ensures we reuse this range rather than duplicating it
            var overlappingEntries = compoundEntries.stream().filter(entry -> entry.offset() < 1024).toList();
            assertThat(overlappingEntries.size(), greaterThanOrEqualTo(1));
            // each of such segments is contained by a single first cfs reader
            for (var entry : overlappingEntries) {
                var entryReaders = compoundSegmentReaders.stream().filter(reader -> {
                    var entryStartPosition = entry.offset();
                    var entryEndPosition = entry.offset() + entry.length();
                    var startOfTheEntryIsInRange = reader.rangeOffset() <= entryStartPosition
                        && entryStartPosition < reader.rangeOffset() + reader.rangeLength();
                    var endOfTheEntryIsInRange = reader.rangeOffset() <= entryEndPosition
                        && entryEndPosition < reader.rangeOffset() + reader.rangeLength();
                    return startOfTheEntryIsInRange || endOfTheEntryIsInRange;
                }).toList();
                assertThat(entryReaders, Matchers.<Collection<?>>allOf(hasSize(1), contains(compoundSegmentReaders.getFirst())));
            }
            // all of such entries are mapped to the same header range
            var correspondingReplicatedRanges = new HashSet<>();
            correspondingReplicatedRanges.add(findRange(content.header(), compoundSegmentsFileOffset, REPLICATED_CONTENT_HEADER_SIZE));
            for (var entry : overlappingEntries) {
                correspondingReplicatedRanges.add(findRange(content.header(), compoundSegmentsFileOffset + entry.offset(), entry.length()));
            }
            assertThat(correspondingReplicatedRanges, hasSize(1));

            // In principle, it is possible to have multiple small files prior to CFS file.
            // In an unlucky case they could be collapsed (along with cfs header) into small file
            // but may not fit completely the nested segment range that overlaps with the compound file header.
            // | small files | cfs |
            // | 1024 <-- cfs file header range
            // | |h|h.f|h..f|h..f|h......f|h.......f|f| <-- actual entries headers and footers
            // |-----------------| <-- collapsed adjacent ranges
            // ^^^ first cfs entry file that is partially located in cfs header range,
            // but can not be completely added to the range due to the overflow
            // To ensure this never happens every compound file creates a new range
            assertTrue(
                "There should be a range starting at a compound segments position",
                content.header().replicatedRanges().stream().anyMatch(range -> range.position() == compoundSegmentsFileOffset)
            );

            verifyReplicatedContent(content);
        }
    }

    public void testHandlesLotsOfSmallFiles() throws IOException {
        try (var directory = LuceneTestCase.newDirectory()) {
            for (int i = 0; i < randomIntBetween(25, 100); i++) {
                var conf = new IndexWriterConfig().setUseCompoundFile(false).setMergePolicy(NoMergePolicy.INSTANCE);
                try (var writer = new IndexWriter(directory, conf)) {
                    for (int j = 0; j < randomIntBetween(3, 5); j++) {
                        writer.addDocument(createDocument());
                    }
                }
            }
            var internalFiles = createInternalFilesFrom(directory);
            var content = ReplicatedContent.create(true, internalFiles, directory, ALWAYS_REPLICATE);

            var totalSmallFilesSize = internalFiles.stream()
                .filter(file -> file.length() <= REPLICATED_CONTENT_HEADER_SIZE + REPLICATED_CONTENT_FOOTER_SIZE)
                .mapToLong(InternalFile::length)
                .sum();

            assertThat(totalSmallFilesSize, greaterThan((long) Short.MAX_VALUE));
            assertThat(content.header().replicatedRanges().size(), greaterThan(1));

            verifyReplicatedContent(content);
        }
    }

    private static List<IndexableField> createDocument() {
        return List.of(new TextField("id", randomIdentifier(), Field.Store.YES), new IntField("value", randomInt(), Field.Store.YES));
    }

    private static List<InternalFile> createInternalFilesFrom(Directory directory) throws IOException {
        return Stream.of(directory.listAll())
            .filter(filename -> Objects.equals(filename, "write.lock") == false)
            .filter(filename -> Objects.equals(filename, "extra0") == false)
            .map(filename -> new InternalFile(filename, sizeOfFileUnchecked(filename, directory)))
            .sorted(Comparator.comparing(InternalFile::length))
            .toList();
    }

    private static long fileOffsetIn(List<InternalFile> internalFiles, String filename) {
        var position = 0L;
        for (InternalFile internalFile : internalFiles) {
            if (Objects.equals(internalFile.name(), filename)) {
                return position;
            }
            position += internalFile.length();
        }
        throw new AssertionError("File [" + filename + "] is not found");
    }

    private static InternalFileReplicatedRange findRange(InternalFilesReplicatedRanges ranges, long position, long length) {
        return ranges.replicatedRanges()
            .stream()
            .filter(range -> range.position() <= position && position + length <= range.position() + range.length())
            .findFirst()
            .get();
    }

    private static long sizeOfFileUnchecked(String filename, Directory directory) {
        try {
            return directory.fileLength(filename);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void verifyReplicatedContent(ReplicatedContent content) throws IOException {
        try (var output = new BytesStreamOutput()) {
            for (var reader : content.readers()) {
                try (var in = reader.getInputStream(0, Long.MAX_VALUE)) {
                    Streams.copy(in, output, false);
                }
            }

            var input = new BytesReferenceIndexInput("test", output.bytes());
            assertThat(input.length(), equalTo(content.header().dataSizeInBytes()));
            var position = 0L;
            for (var range : content.header().replicatedRanges()) {
                input.seek(position);
                // every range should start as a header or footer
                assertThat(CodecUtil.readBEInt(input), anyOf(equalTo(CodecUtil.CODEC_MAGIC), equalTo(CodecUtil.FOOTER_MAGIC)));
                position += range.length();
            }
        }
    }
}
