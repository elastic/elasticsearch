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

package org.elasticsearch.common.lucene;

import com.google.common.collect.Iterables;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.Version;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.lucene.search.NoopCollector.NOOP_COLLECTOR;

/**
 *
 */
public class Lucene {

    // TODO: remove VERSION, and have users use Version.LATEST.
    public static final Version VERSION = Version.LATEST;
    public static final Version ANALYZER_VERSION = VERSION;
    public static final Version QUERYPARSER_VERSION = VERSION;
    public static final String LATEST_DOC_VALUES_FORMAT = "Lucene50";
    public static final String LATEST_POSTINGS_FORMAT = "Lucene50";
    public static final String LATEST_CODEC = "Lucene50";

    static {
        Deprecated annotation = PostingsFormat.forName(LATEST_POSTINGS_FORMAT).getClass().getAnnotation(Deprecated.class);
        assert annotation == null : "PostingsFromat " + LATEST_POSTINGS_FORMAT + " is deprecated" ;
        annotation = DocValuesFormat.forName(LATEST_DOC_VALUES_FORMAT).getClass().getAnnotation(Deprecated.class);
        assert annotation == null : "DocValuesFormat " + LATEST_DOC_VALUES_FORMAT + " is deprecated" ;
    }

    public static final NamedAnalyzer STANDARD_ANALYZER = new NamedAnalyzer("_standard", AnalyzerScope.GLOBAL, new StandardAnalyzer());
    public static final NamedAnalyzer KEYWORD_ANALYZER = new NamedAnalyzer("_keyword", AnalyzerScope.GLOBAL, new KeywordAnalyzer());

    public static final ScoreDoc[] EMPTY_SCORE_DOCS = new ScoreDoc[0];

    public static final TopDocs EMPTY_TOP_DOCS = new TopDocs(0, EMPTY_SCORE_DOCS, 0.0f);

    @SuppressWarnings("deprecation")
    public static Version parseVersion(@Nullable String version, Version defaultVersion, ESLogger logger) {
        if (version == null) {
            return defaultVersion;
        }
        try {
            return Version.parse(version);
        } catch (ParseException e) {
            logger.warn("no version match {}, default to {}", version, defaultVersion, e);
            return defaultVersion;
        }
    }

    /**
     * Reads the segments infos, failing if it fails to load
     */
    public static SegmentInfos readSegmentInfos(Directory directory) throws IOException {
        return SegmentInfos.readLatestCommit(directory);
    }

    /**
     * Returns an iterable that allows to iterate over all files in this segments info
     */
    public static Iterable<String> files(SegmentInfos infos) throws IOException {
        final List<Collection<String>> list = new ArrayList<>();
        list.add(Collections.singleton(infos.getSegmentsFileName()));
        for (SegmentCommitInfo info : infos) {
            list.add(info.files());
        }
        return Iterables.concat(list.toArray(new Collection[0]));
    }

    /**
     * Reads the segments infos from the given commit, failing if it fails to load
     */
    public static SegmentInfos readSegmentInfos(IndexCommit commit, Directory directory) throws IOException {
        return SegmentInfos.readCommit(directory, commit.getSegmentsFileName());
    }

    /**
     * Reads the segments infos from the given segments file name, failing if it fails to load
     */
    private static SegmentInfos readSegmentInfos(String segmentsFileName, Directory directory) throws IOException {
        return SegmentInfos.readCommit(directory, segmentsFileName);
    }

    /**
     * This method removes all files from the given directory that are not referenced by the given segments file.
     * This method will open an IndexWriter and relies on index file deleter to remove all unreferenced files. Segment files
     * that are newer than the given segments file are removed forcefully to prevent problems with IndexWriter opening a potentially
     * broken commit point / leftover.
     * <b>Note:</b> this method will fail if there is another IndexWriter open on the given directory. This method will also acquire
     * a write lock from the directory while pruning unused files. This method expects an existing index in the given directory that has
     * the given segments file.
     */
    public static SegmentInfos pruneUnreferencedFiles(String segmentsFileName, Directory directory) throws IOException {
        final SegmentInfos si = readSegmentInfos(segmentsFileName, directory);
        try (Lock writeLock = directory.makeLock(IndexWriter.WRITE_LOCK_NAME)) {
            if (!writeLock.obtain(IndexWriterConfig.getDefaultWriteLockTimeout())) { // obtain write lock
                throw new LockObtainFailedException("Index locked for write: " + writeLock);
            }
            int foundSegmentFiles = 0;
            for (final String file : directory.listAll()) {
                /**
                 * we could also use a deletion policy here but in the case of snapshot and restore
                 * sometimes we restore an index and override files that were referenced by a "future"
                 * commit. If such a commit is opened by the IW it would likely throw a corrupted index exception
                 * since checksums don's match anymore. that's why we prune the name here directly.
                 * We also want the caller to know if we were not able to remove a segments_N file.
                 */
                if (file.startsWith(IndexFileNames.SEGMENTS) || file.equals(IndexFileNames.OLD_SEGMENTS_GEN)) {
                    foundSegmentFiles++;
                    if (file.equals(si.getSegmentsFileName()) == false) {
                        directory.deleteFile(file); // remove all segment_N files except of the one we wanna keep
                    }
                }
            }
            assert SegmentInfos.getLastCommitSegmentsFileName(directory).equals(segmentsFileName);
            if (foundSegmentFiles == 0) {
                throw new IllegalStateException("no commit found in the directory");
            }
        }
        final CommitPoint cp = new CommitPoint(si, directory);
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(Lucene.STANDARD_ANALYZER)
                .setIndexCommit(cp)
                .setCommitOnClose(false)
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setOpenMode(IndexWriterConfig.OpenMode.APPEND))) {
            // do nothing and close this will kick of IndexFileDeleter which will remove all pending files
        }
        return si;
    }

    /**
     * This method removes all lucene files from the given directory. It will first try to delete all commit points / segments
     * files to ensure broken commits or corrupted indices will not be opened in the future. If any of the segment files can't be deleted
     * this operation fails.
     */
    public static void cleanLuceneIndex(Directory directory) throws IOException {
        try (Lock writeLock = directory.makeLock(IndexWriter.WRITE_LOCK_NAME)) {
            if (!writeLock.obtain(IndexWriterConfig.getDefaultWriteLockTimeout())) { // obtain write lock
                throw new LockObtainFailedException("Index locked for write: " + writeLock);
            }
            for (final String file : directory.listAll()) {
                if (file.startsWith(IndexFileNames.SEGMENTS) || file.equals(IndexFileNames.OLD_SEGMENTS_GEN)) {
                    directory.deleteFile(file); // remove all segment_N files
                }
            }
        }
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(Lucene.STANDARD_ANALYZER)
                .setMergePolicy(NoMergePolicy.INSTANCE) // no merges
                .setCommitOnClose(false) // no commits
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE))) // force creation - don't append...
        {
            // do nothing and close this will kick of IndexFileDeleter which will remove all pending files
        }
    }

    public static void checkSegmentInfoIntegrity(final Directory directory) throws IOException {
        new SegmentInfos.FindSegmentsFile(directory) {

            @Override
            protected Object doBody(String segmentFileName) throws IOException {
                try (IndexInput input = directory.openInput(segmentFileName, IOContext.READ)) {
                    final int format = input.readInt();
                    final int actualFormat;
                    if (format == CodecUtil.CODEC_MAGIC) {
                        // 4.0+
                        actualFormat = CodecUtil.checkHeaderNoMagic(input, "segments", SegmentInfos.VERSION_40, Integer.MAX_VALUE);
                        if (actualFormat >= SegmentInfos.VERSION_48) {
                            CodecUtil.checksumEntireFile(input);
                        }
                    }
                    // legacy....
                }
                return null;
            }
        }.run();
    }

    public static long count(IndexSearcher searcher, Query query) throws IOException {
        return searcher.count(query);
    }

    /**
     * Performs a count on the <code>searcher</code> for <code>query</code>. Terminates
     * early when the count has reached <code>terminateAfter</code>
     */
    public static long count(IndexSearcher searcher, Query query, int terminateAfterCount) throws IOException {
        EarlyTerminatingCollector countCollector = createCountBasedEarlyTerminatingCollector(terminateAfterCount);
        countWithEarlyTermination(searcher, query, countCollector);
        return countCollector.count();
    }

    /**
     * Creates count based early termination collector with a threshold of <code>maxCountHits</code>
     */
    public final static EarlyTerminatingCollector createCountBasedEarlyTerminatingCollector(int maxCountHits) {
        return new EarlyTerminatingCollector(maxCountHits);
    }

    /**
     * Wraps <code>delegate</code> with count based early termination collector with a threshold of <code>maxCountHits</code>
     */
    public final static EarlyTerminatingCollector wrapCountBasedEarlyTerminatingCollector(final Collector delegate, int maxCountHits) {
        return new EarlyTerminatingCollector(delegate, maxCountHits);
    }

    /**
     * Wraps <code>delegate</code> with a time limited collector with a timeout of <code>timeoutInMillis</code>
     */
    public final static TimeLimitingCollector wrapTimeLimitingCollector(final Collector delegate, final Counter counter, long timeoutInMillis) {
        return new TimeLimitingCollector(delegate, counter, timeoutInMillis);
    }

    /**
     * Performs an exists (count > 0) query on the <code>searcher</code> for <code>query</code>
     * with <code>filter</code> using the given <code>collector</code>
     *
     * The <code>collector</code> can be instantiated using <code>Lucene.createExistsCollector()</code>
     */
    public static boolean exists(IndexSearcher searcher, Query query, Filter filter,
                                 EarlyTerminatingCollector collector) throws IOException {
        collector.reset();
        countWithEarlyTermination(searcher, filter, query, collector);
        return collector.exists();
    }


    /**
     * Performs an exists (count > 0) query on the <code>searcher</code> for <code>query</code>
     * using the given <code>collector</code>
     *
     * The <code>collector</code> can be instantiated using <code>Lucene.createExistsCollector()</code>
     */
    public static boolean exists(IndexSearcher searcher, Query query, EarlyTerminatingCollector collector) throws IOException {
        collector.reset();
        countWithEarlyTermination(searcher, query, collector);
        return collector.exists();
    }

    /**
     * Calls <code>countWithEarlyTermination(searcher, null, query, collector)</code>
     */
    public static boolean countWithEarlyTermination(IndexSearcher searcher, Query query,
                                                  EarlyTerminatingCollector collector) throws IOException {
        return countWithEarlyTermination(searcher, null, query, collector);
    }

    /**
     * Performs a count on <code>query</code> and <code>filter</code> with early termination using <code>searcher</code>.
     * The early termination threshold is specified by the provided <code>collector</code>
     */
    public static boolean countWithEarlyTermination(IndexSearcher searcher, Filter filter, Query query,
                                                        EarlyTerminatingCollector collector) throws IOException {
        try {
            if (filter == null) {
                searcher.search(query, collector);
            } else {
                searcher.search(query, filter, collector);
            }
        } catch (EarlyTerminationException e) {
            // early termination
            return true;
        }
        return false;
    }

    /**
     * Creates an {@link org.elasticsearch.common.lucene.Lucene.EarlyTerminatingCollector}
     * with a threshold of <code>1</code>
     */
    public final static EarlyTerminatingCollector createExistsCollector() {
        return createCountBasedEarlyTerminatingCollector(1);
    }

    /**
     * Closes the index writer, returning <tt>false</tt> if it failed to close.
     */
    public static boolean safeClose(IndexWriter writer) {
        if (writer == null) {
            return true;
        }
        try {
            writer.close();
            return true;
        } catch (Throwable e) {
            return false;
        }
    }

    public static TopDocs readTopDocs(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            int totalHits = in.readVInt();
            float maxScore = in.readFloat();

            SortField[] fields = new SortField[in.readVInt()];
            for (int i = 0; i < fields.length; i++) {
                String field = null;
                if (in.readBoolean()) {
                    field = in.readString();
                }
                fields[i] = new SortField(field, readSortType(in), in.readBoolean());
            }

            FieldDoc[] fieldDocs = new FieldDoc[in.readVInt()];
            for (int i = 0; i < fieldDocs.length; i++) {
                fieldDocs[i] = readFieldDoc(in);
            }
            return new TopFieldDocs(totalHits, fieldDocs, fields, maxScore);
        } else {
            int totalHits = in.readVInt();
            float maxScore = in.readFloat();

            ScoreDoc[] scoreDocs = new ScoreDoc[in.readVInt()];
            for (int i = 0; i < scoreDocs.length; i++) {
                scoreDocs[i] = new ScoreDoc(in.readVInt(), in.readFloat());
            }
            return new TopDocs(totalHits, scoreDocs, maxScore);
        }
    }

    public static FieldDoc readFieldDoc(StreamInput in) throws IOException {
        Comparable[] cFields = new Comparable[in.readVInt()];
        for (int j = 0; j < cFields.length; j++) {
            byte type = in.readByte();
            if (type == 0) {
                cFields[j] = null;
            } else if (type == 1) {
                cFields[j] = in.readString();
            } else if (type == 2) {
                cFields[j] = in.readInt();
            } else if (type == 3) {
                cFields[j] = in.readLong();
            } else if (type == 4) {
                cFields[j] = in.readFloat();
            } else if (type == 5) {
                cFields[j] = in.readDouble();
            } else if (type == 6) {
                cFields[j] = in.readByte();
            } else if (type == 7) {
                cFields[j] = in.readShort();
            } else if (type == 8) {
                cFields[j] = in.readBoolean();
            } else if (type == 9) {
                cFields[j] = in.readBytesRef();
            } else {
                throw new IOException("Can't match type [" + type + "]");
            }
        }
        return new FieldDoc(in.readVInt(), in.readFloat(), cFields);
    }

    public static ScoreDoc readScoreDoc(StreamInput in) throws IOException {
        return new ScoreDoc(in.readVInt(), in.readFloat());
    }

    public static void writeTopDocs(StreamOutput out, TopDocs topDocs) throws IOException {
        if (topDocs instanceof TopFieldDocs) {
            out.writeBoolean(true);
            TopFieldDocs topFieldDocs = (TopFieldDocs) topDocs;

            out.writeVInt(topDocs.totalHits);
            out.writeFloat(topDocs.getMaxScore());

            out.writeVInt(topFieldDocs.fields.length);
            for (SortField sortField : topFieldDocs.fields) {
                if (sortField.getField() == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    out.writeString(sortField.getField());
                }
                if (sortField.getComparatorSource() != null) {
                    writeSortType(out, ((IndexFieldData.XFieldComparatorSource) sortField.getComparatorSource()).reducedType());
                } else {
                    writeSortType(out, sortField.getType());
                }
                out.writeBoolean(sortField.getReverse());
            }

            out.writeVInt(topDocs.scoreDocs.length);
            for (ScoreDoc doc : topFieldDocs.scoreDocs) {
                writeFieldDoc(out, (FieldDoc) doc);
            }
        } else {
            out.writeBoolean(false);
            out.writeVInt(topDocs.totalHits);
            out.writeFloat(topDocs.getMaxScore());

            out.writeVInt(topDocs.scoreDocs.length);
            for (ScoreDoc doc : topDocs.scoreDocs) {
                writeScoreDoc(out, doc);
            }
        }
    }

    public static void writeFieldDoc(StreamOutput out, FieldDoc fieldDoc) throws IOException {
        out.writeVInt(fieldDoc.fields.length);
        for (Object field : fieldDoc.fields) {
            if (field == null) {
                out.writeByte((byte) 0);
            } else {
                Class type = field.getClass();
                if (type == String.class) {
                    out.writeByte((byte) 1);
                    out.writeString((String) field);
                } else if (type == Integer.class) {
                    out.writeByte((byte) 2);
                    out.writeInt((Integer) field);
                } else if (type == Long.class) {
                    out.writeByte((byte) 3);
                    out.writeLong((Long) field);
                } else if (type == Float.class) {
                    out.writeByte((byte) 4);
                    out.writeFloat((Float) field);
                } else if (type == Double.class) {
                    out.writeByte((byte) 5);
                    out.writeDouble((Double) field);
                } else if (type == Byte.class) {
                    out.writeByte((byte) 6);
                    out.writeByte((Byte) field);
                } else if (type == Short.class) {
                    out.writeByte((byte) 7);
                    out.writeShort((Short) field);
                } else if (type == Boolean.class) {
                    out.writeByte((byte) 8);
                    out.writeBoolean((Boolean) field);
                } else if (type == BytesRef.class) {
                    out.writeByte((byte) 9);
                    out.writeBytesRef((BytesRef) field);
                } else {
                    throw new IOException("Can't handle sort field value of type [" + type + "]");
                }
            }
        }
        out.writeVInt(fieldDoc.doc);
        out.writeFloat(fieldDoc.score);
    }

    public static void writeScoreDoc(StreamOutput out, ScoreDoc scoreDoc) throws IOException {
        if (!scoreDoc.getClass().equals(ScoreDoc.class)) {
            throw new ElasticsearchIllegalArgumentException("This method can only be used to serialize a ScoreDoc, not a " + scoreDoc.getClass());
        }
        out.writeVInt(scoreDoc.doc);
        out.writeFloat(scoreDoc.score);
    }

    // LUCENE 4 UPGRADE: We might want to maintain our own ordinal, instead of Lucene's ordinal
    public static SortField.Type readSortType(StreamInput in) throws IOException {
        return SortField.Type.values()[in.readVInt()];
    }

    public static void writeSortType(StreamOutput out, SortField.Type sortType) throws IOException {
        out.writeVInt(sortType.ordinal());
    }

    public static Explanation readExplanation(StreamInput in) throws IOException {
        boolean match = in.readBoolean();
        String description = in.readString();
        final Explanation[] subExplanations = new Explanation[in.readVInt()];
        for (int i = 0; i < subExplanations.length; ++i) {
            subExplanations[i] = readExplanation(in);
        }
        if (match) {
            return Explanation.match(in.readFloat(), description, subExplanations);
        } else {
            return Explanation.noMatch(description, subExplanations);
        }
    }

    public static void writeExplanation(StreamOutput out, Explanation explanation) throws IOException {
        out.writeBoolean(explanation.isMatch());
        out.writeString(explanation.getDescription());
        Explanation[] subExplanations = explanation.getDetails();
        out.writeVInt(subExplanations.length);
        for (Explanation subExp : subExplanations) {
            writeExplanation(out, subExp);
        }
        if (explanation.isMatch()) {
            out.writeFloat(explanation.getValue());
        }
    }

    /**
     * This exception is thrown when {@link org.elasticsearch.common.lucene.Lucene.EarlyTerminatingCollector}
     * reaches early termination
     * */
    public final static class EarlyTerminationException extends ElasticsearchException {

        public EarlyTerminationException(String msg) {
            super(msg);
        }
    }

    /**
     * A collector that terminates early by throwing {@link org.elasticsearch.common.lucene.Lucene.EarlyTerminationException}
     * when count of matched documents has reached <code>maxCountHits</code>
     */
    public final static class EarlyTerminatingCollector extends SimpleCollector {

        private final int maxCountHits;
        private final Collector delegate;

        private int count = 0;
        private LeafCollector leafCollector;

        EarlyTerminatingCollector(int maxCountHits) {
            this.maxCountHits = maxCountHits;
            this.delegate = NOOP_COLLECTOR;
        }

        EarlyTerminatingCollector(final Collector delegate, int maxCountHits) {
            this.maxCountHits = maxCountHits;
            this.delegate = (delegate == null) ? NOOP_COLLECTOR : delegate;
        }

        public void reset() {
            count = 0;
        }
        public int count() {
            return count;
        }

        public boolean exists() {
            return count > 0;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            leafCollector.setScorer(scorer);
        }

        @Override
        public void collect(int doc) throws IOException {
            leafCollector.collect(doc);

            if (++count >= maxCountHits) {
                throw new EarlyTerminationException("early termination [CountBased]");
            }
        }

        @Override
        public void doSetNextReader(LeafReaderContext atomicReaderContext) throws IOException {
            leafCollector = delegate.getLeafCollector(atomicReaderContext);
        }

        @Override
        public boolean needsScores() {
            return delegate.needsScores();
        }
    }

    private Lucene() {

    }

    public static final boolean indexExists(final Directory directory) throws IOException {
        return DirectoryReader.indexExists(directory);
    }

    /**
     * Wait for an index to exist for up to {@code timeLimitMillis}. Returns
     * true if the index eventually exists, false if not.
     *
     * Will retry the directory every second for at least {@code timeLimitMillis}
     */
    public static final boolean waitForIndex(final Directory directory, final long timeLimitMillis)
            throws IOException {
        final long DELAY = 1000;
        long waited = 0;
        try {
            while (true) {
                if (waited >= timeLimitMillis) {
                    break;
                }
                if (indexExists(directory)) {
                    return true;
                }
                Thread.sleep(DELAY);
                waited += DELAY;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        // one more try after all retries
        return indexExists(directory);
    }

    /**
     * Returns <tt>true</tt> iff the given exception or
     * one of it's causes is an instance of {@link CorruptIndexException}, 
     * {@link IndexFormatTooOldException}, or {@link IndexFormatTooNewException} otherwise <tt>false</tt>.
     */
    public static boolean isCorruptionException(Throwable t) {
        return ExceptionsHelper.unwrapCorruption(t) != null;
    }

    /**
     * Parses the version string lenient and returns the the default value if the given string is null or emtpy
     */
    public static Version parseVersionLenient(String toParse, Version defaultValue) {
        return LenientParser.parse(toParse, defaultValue);
    }

    @SuppressForbidden(reason = "Version#parseLeniently() used in a central place")
    private static final class LenientParser {
        public static Version parse(String toParse, Version defaultValue) {
            if (Strings.hasLength(toParse)) {
                try {
                    return Version.parseLeniently(toParse);
                } catch (ParseException e) {
                    // pass to default
                }
            }
            return defaultValue;
        }
    }

    /**
     * Return a Scorer that throws an ElasticsearchIllegalStateException
     * on all operations with the given message.
     */
    public static Scorer illegalScorer(final String message) {
        return new Scorer(null) {
            @Override
            public float score() throws IOException {
                throw new ElasticsearchIllegalStateException(message);
            }
            @Override
            public int freq() throws IOException {
                throw new ElasticsearchIllegalStateException(message);
            }
            @Override
            public int advance(int arg0) throws IOException {
                throw new ElasticsearchIllegalStateException(message);
            }
            @Override
            public long cost() {
                throw new ElasticsearchIllegalStateException(message);
            }
            @Override
            public int docID() {
                throw new ElasticsearchIllegalStateException(message);
            }
            @Override
            public int nextDoc() throws IOException {
                throw new ElasticsearchIllegalStateException(message);
            }
        };
    }

    private static final class CommitPoint extends IndexCommit {
        private String segmentsFileName;
        private final Collection<String> files;
        private final Directory dir;
        private final long generation;
        private final Map<String,String> userData;
        private final int segmentCount;

        private CommitPoint(SegmentInfos infos, Directory dir) throws IOException {
            segmentsFileName = infos.getSegmentsFileName();
            this.dir = dir;
            userData = infos.getUserData();
            files = Collections.unmodifiableCollection(infos.files(dir, true));
            generation = infos.getGeneration();
            segmentCount = infos.size();
        }

        @Override
        public String toString() {
            return "DirectoryReader.ReaderCommit(" + segmentsFileName + ")";
        }

        @Override
        public int getSegmentCount() {
            return segmentCount;
        }

        @Override
        public String getSegmentsFileName() {
            return segmentsFileName;
        }

        @Override
        public Collection<String> getFileNames() {
            return files;
        }

        @Override
        public Directory getDirectory() {
            return dir;
        }

        @Override
        public long getGeneration() {
            return generation;
        }

        @Override
        public boolean isDeleted() {
            return false;
        }

        @Override
        public Map<String,String> getUserData() {
            return userData;
        }

        @Override
        public void delete() {
            throw new UnsupportedOperationException("This IndexCommit does not support deletions");
        }
    }
}
