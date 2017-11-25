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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.iterable.Iterables;
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

public class Lucene {
    public static final String LATEST_DOC_VALUES_FORMAT = "Lucene70";
    public static final String LATEST_POSTINGS_FORMAT = "Lucene50";
    public static final String LATEST_CODEC = "Lucene70";

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

    public static Version parseVersion(@Nullable String version, Version defaultVersion, Logger logger) {
        if (version == null) {
            return defaultVersion;
        }
        try {
            return Version.parse(version);
        } catch (ParseException e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("no version match {}, default to {}", version, defaultVersion), e);
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
        return Iterables.flatten(list);
    }

    /**
     * Returns the number of documents in the index referenced by this {@link SegmentInfos}
     */
    public static int getNumDocs(SegmentInfos info) {
        int numDocs = 0;
        for (SegmentCommitInfo si : info) {
            numDocs += si.info.maxDoc() - si.getDelCount();
        }
        return numDocs;
    }

    /**
     * Reads the segments infos from the given commit, failing if it fails to load
     */
    public static SegmentInfos readSegmentInfos(IndexCommit commit) throws IOException {
        // Using commit.getSegmentsFileName() does NOT work here, have to
        // manually create the segment filename
        String filename = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", commit.getGeneration());
        return SegmentInfos.readCommit(commit.getDirectory(), filename);
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
        try (Lock writeLock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
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
        try (Lock writeLock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
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
                    CodecUtil.checksumEntireFile(input);
                }
                return null;
            }
        }.run();
    }

    /**
     * Check whether there is one or more documents matching the provided query.
     */
    public static boolean exists(IndexSearcher searcher, Query query) throws IOException {
        final Weight weight = searcher.createNormalizedWeight(query, false);
        // the scorer API should be more efficient at stopping after the first
        // match than the bulk scorer API
        for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
            final Scorer scorer = weight.scorer(context);
            if (scorer == null) {
                continue;
            }
            final Bits liveDocs = context.reader().getLiveDocs();
            final DocIdSetIterator iterator = scorer.iterator();
            for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                if (liveDocs == null || liveDocs.get(doc)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static TopDocs readTopDocs(StreamInput in) throws IOException {
        byte type = in.readByte();
        if (type == 0) {
            long totalHits = in.readVLong();
            float maxScore = in.readFloat();

            ScoreDoc[] scoreDocs = new ScoreDoc[in.readVInt()];
            for (int i = 0; i < scoreDocs.length; i++) {
                scoreDocs[i] = new ScoreDoc(in.readVInt(), in.readFloat());
            }
            return new TopDocs(totalHits, scoreDocs, maxScore);
        } else if (type == 1) {
            long totalHits = in.readVLong();
            float maxScore = in.readFloat();

            SortField[] fields = new SortField[in.readVInt()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = readSortField(in);
            }

            FieldDoc[] fieldDocs = new FieldDoc[in.readVInt()];
            for (int i = 0; i < fieldDocs.length; i++) {
                fieldDocs[i] = readFieldDoc(in);
            }
            return new TopFieldDocs(totalHits, fieldDocs, fields, maxScore);
        } else if (type == 2) {
            long totalHits = in.readVLong();
            float maxScore = in.readFloat();

            String field = in.readString();
            SortField[] fields = new SortField[in.readVInt()];
            for (int i = 0; i < fields.length; i++) {
               fields[i] = readSortField(in);
            }
            int size = in.readVInt();
            Object[] collapseValues = new Object[size];
            FieldDoc[] fieldDocs = new FieldDoc[size];
            for (int i = 0; i < fieldDocs.length; i++) {
                fieldDocs[i] = readFieldDoc(in);
                collapseValues[i] = readSortValue(in);
            }
            return new CollapseTopFieldDocs(field, totalHits, fieldDocs, fields, collapseValues, maxScore);
        } else {
            throw new IllegalStateException("Unknown type " + type);
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

    private static Comparable readSortValue(StreamInput in) throws IOException {
        byte type = in.readByte();
        if (type == 0) {
            return null;
        } else if (type == 1) {
            return in.readString();
        } else if (type == 2) {
            return in.readInt();
        } else if (type == 3) {
            return in.readLong();
        } else if (type == 4) {
            return in.readFloat();
        } else if (type == 5) {
            return in.readDouble();
        } else if (type == 6) {
            return in.readByte();
        } else if (type == 7) {
            return in.readShort();
        } else if (type == 8) {
            return in.readBoolean();
        } else if (type == 9) {
            return in.readBytesRef();
        } else {
            throw new IOException("Can't match type [" + type + "]");
        }
    }

    public static ScoreDoc readScoreDoc(StreamInput in) throws IOException {
        return new ScoreDoc(in.readVInt(), in.readFloat());
    }

    private static final Class<?> GEO_DISTANCE_SORT_TYPE_CLASS = LatLonDocValuesField.newDistanceSort("some_geo_field", 0, 0).getClass();

    public static void writeTopDocs(StreamOutput out, TopDocs topDocs) throws IOException {
        if (topDocs instanceof CollapseTopFieldDocs) {
            out.writeByte((byte) 2);
            CollapseTopFieldDocs collapseDocs = (CollapseTopFieldDocs) topDocs;

            out.writeVLong(topDocs.totalHits);
            out.writeFloat(topDocs.getMaxScore());

            out.writeString(collapseDocs.field);

            out.writeVInt(collapseDocs.fields.length);
            for (SortField sortField : collapseDocs.fields) {
               writeSortField(out, sortField);
            }

            out.writeVInt(topDocs.scoreDocs.length);
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                ScoreDoc doc = collapseDocs.scoreDocs[i];
                writeFieldDoc(out, (FieldDoc) doc);
                writeSortValue(out, collapseDocs.collapseValues[i]);
            }
        } else if (topDocs instanceof TopFieldDocs) {
            out.writeByte((byte) 1);
            TopFieldDocs topFieldDocs = (TopFieldDocs) topDocs;

            out.writeVLong(topDocs.totalHits);
            out.writeFloat(topDocs.getMaxScore());

            out.writeVInt(topFieldDocs.fields.length);
            for (SortField sortField : topFieldDocs.fields) {
              writeSortField(out, sortField);
            }

            out.writeVInt(topDocs.scoreDocs.length);
            for (ScoreDoc doc : topFieldDocs.scoreDocs) {
                writeFieldDoc(out, (FieldDoc) doc);
            }
        } else {
            out.writeByte((byte) 0);
            out.writeVLong(topDocs.totalHits);
            out.writeFloat(topDocs.getMaxScore());

            out.writeVInt(topDocs.scoreDocs.length);
            for (ScoreDoc doc : topDocs.scoreDocs) {
                writeScoreDoc(out, doc);
            }
        }
    }

    private static void writeMissingValue(StreamOutput out, Object missingValue) throws IOException {
        if (missingValue == SortField.STRING_FIRST) {
            out.writeByte((byte) 1);
        } else if (missingValue == SortField.STRING_LAST) {
            out.writeByte((byte) 2);
        } else {
            out.writeByte((byte) 0);
            out.writeGenericValue(missingValue);
        }
    }

    private static Object readMissingValue(StreamInput in) throws IOException {
        final byte id = in.readByte();
        switch (id) {
        case 0:
            return in.readGenericValue();
        case 1:
            return SortField.STRING_FIRST;
        case 2:
            return SortField.STRING_LAST;
        default:
            throw new IOException("Unknown missing value id: " + id);
        }
    }


    private static void writeSortValue(StreamOutput out, Object field) throws IOException {
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

    public static void writeFieldDoc(StreamOutput out, FieldDoc fieldDoc) throws IOException {
        out.writeVInt(fieldDoc.fields.length);
        for (Object field : fieldDoc.fields) {
            writeSortValue(out, field);
        }
        out.writeVInt(fieldDoc.doc);
        out.writeFloat(fieldDoc.score);
    }

    public static void writeScoreDoc(StreamOutput out, ScoreDoc scoreDoc) throws IOException {
        if (!scoreDoc.getClass().equals(ScoreDoc.class)) {
            throw new IllegalArgumentException("This method can only be used to serialize a ScoreDoc, not a " + scoreDoc.getClass());
        }
        out.writeVInt(scoreDoc.doc);
        out.writeFloat(scoreDoc.score);
    }

    // LUCENE 4 UPGRADE: We might want to maintain our own ordinal, instead of Lucene's ordinal
    public static SortField.Type readSortType(StreamInput in) throws IOException {
        return SortField.Type.values()[in.readVInt()];
    }

    public static SortField readSortField(StreamInput in) throws IOException {
        String field = null;
        if (in.readBoolean()) {
            field = in.readString();
        }
        SortField.Type sortType = readSortType(in);
        Object missingValue = readMissingValue(in);
        boolean reverse = in.readBoolean();
        SortField sortField = new SortField(field, sortType, reverse);
        if (missingValue != null) {
            sortField.setMissingValue(missingValue);
        }
        return sortField;
    }

    public static void writeSortType(StreamOutput out, SortField.Type sortType) throws IOException {
        out.writeVInt(sortType.ordinal());
    }

    public static void writeSortField(StreamOutput out, SortField sortField) throws IOException {
        if (sortField.getClass() == GEO_DISTANCE_SORT_TYPE_CLASS) {
            // for geo sorting, we replace the SortField with a SortField that assumes a double field.
            // this works since the SortField is only used for merging top docs
            SortField newSortField = new SortField(sortField.getField(), SortField.Type.DOUBLE);
            newSortField.setMissingValue(sortField.getMissingValue());
            sortField = newSortField;
        } else if (sortField.getClass() == SortedSetSortField.class) {
            // for multi-valued sort field, we replace the SortedSetSortField with a simple SortField.
            // It works because the sort field is only used to merge results from different shards.
            SortField newSortField = new SortField(sortField.getField(), SortField.Type.STRING, sortField.getReverse());
            newSortField.setMissingValue(sortField.getMissingValue());
            sortField = newSortField;
        } else if (sortField.getClass() == SortedNumericSortField.class) {
            // for multi-valued sort field, we replace the SortedSetSortField with a simple SortField.
            // It works because the sort field is only used to merge results from different shards.
            SortField newSortField = new SortField(sortField.getField(),
                ((SortedNumericSortField) sortField).getNumericType(),
                sortField.getReverse());
            newSortField.setMissingValue(sortField.getMissingValue());
            sortField = newSortField;
        }

        if (sortField.getClass() != SortField.class) {
            throw new IllegalArgumentException("Cannot serialize SortField impl [" + sortField + "]");
        }
        if (sortField.getField() == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(sortField.getField());
        }
        if (sortField.getComparatorSource() != null) {
            IndexFieldData.XFieldComparatorSource comparatorSource = (IndexFieldData.XFieldComparatorSource) sortField.getComparatorSource();
            writeSortType(out, comparatorSource.reducedType());
            writeMissingValue(out, comparatorSource.missingValue(sortField.getReverse()));
        } else {
            writeSortType(out, sortField.getType());
            writeMissingValue(out, sortField.getMissingValue());
        }
        out.writeBoolean(sortField.getReverse());
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
     * Parses the version string lenient and returns the default value if the given string is null or empty
     */
    public static Version parseVersionLenient(String toParse, Version defaultValue) {
        return LenientParser.parse(toParse, defaultValue);
    }

    /**
     * Tries to extract a segment reader from the given index reader.
     * If no SegmentReader can be extracted an {@link IllegalStateException} is thrown.
     */
    public static SegmentReader segmentReader(LeafReader reader) {
        if (reader instanceof SegmentReader) {
            return (SegmentReader) reader;
        } else if (reader instanceof FilterLeafReader) {
            final FilterLeafReader fReader = (FilterLeafReader) reader;
            return segmentReader(FilterLeafReader.unwrap(fReader));
        }
        // hard fail - we can't get a SegmentReader
        throw new IllegalStateException("Can not extract segment reader from given index reader [" + reader + "]");
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
                throw new IllegalStateException(message);
            }
            @Override
            public int freq() throws IOException {
                throw new IllegalStateException(message);
            }
            @Override
            public int docID() {
                throw new IllegalStateException(message);
            }
            @Override
            public DocIdSetIterator iterator() {
                throw new IllegalStateException(message);
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
            files = Collections.unmodifiableCollection(infos.files(true));
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

    /**
     * Given a {@link ScorerSupplier}, return a {@link Bits} instance that will match
     * all documents contained in the set. Note that the returned {@link Bits}
     * instance MUST be consumed in order.
     */
    public static Bits asSequentialAccessBits(final int maxDoc, @Nullable ScorerSupplier scorerSupplier) throws IOException {
        if (scorerSupplier == null) {
            return new Bits.MatchNoBits(maxDoc);
        }
        // Since we want bits, we need random-access
        final Scorer scorer = scorerSupplier.get(Long.MAX_VALUE); // this never returns null
        final TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
        final DocIdSetIterator iterator;
        if (twoPhase == null) {
            iterator = scorer.iterator();
        } else {
            iterator = twoPhase.approximation();
        }

        return new Bits() {

            int previous = -1;
            boolean previousMatched = false;

            @Override
            public boolean get(int index) {
                if (index < 0 || index >= maxDoc) {
                    throw new IndexOutOfBoundsException(index + " is out of bounds: [" + 0 + "-" + maxDoc + "[");
                }
                if (index < previous) {
                    throw new IllegalArgumentException("This Bits instance can only be consumed in order. "
                            + "Got called on [" + index + "] while previously called on [" + previous + "]");
                }
                if (index == previous) {
                    // we cache whether it matched because it is illegal to call
                    // twoPhase.matches() twice
                    return previousMatched;
                }
                previous = index;

                int doc = iterator.docID();
                if (doc < index) {
                    try {
                        doc = iterator.advance(index);
                    } catch (IOException e) {
                        throw new IllegalStateException("Cannot advance iterator", e);
                    }
                }
                if (index == doc) {
                    try {
                        return previousMatched = twoPhase == null || twoPhase.matches();
                    } catch (IOException e) {
                        throw new IllegalStateException("Cannot validate match", e);
                    }
                }
                return previousMatched = false;
            }

            @Override
            public int length() {
                return maxDoc;
            }
        };
    }
}
