/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.field.data.FieldDataType;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 *
 */
public class Lucene {

    public static final Version VERSION = Version.LUCENE_35;
    public static final Version ANALYZER_VERSION = VERSION;
    public static final Version QUERYPARSER_VERSION = VERSION;

    public static final NamedAnalyzer STANDARD_ANALYZER = new NamedAnalyzer("_standard", AnalyzerScope.GLOBAL, new StandardAnalyzer(ANALYZER_VERSION));
    public static final NamedAnalyzer KEYWORD_ANALYZER = new NamedAnalyzer("_keyword", AnalyzerScope.GLOBAL, new KeywordAnalyzer());

    public static final int NO_DOC = -1;

    public static ScoreDoc[] EMPTY_SCORE_DOCS = new ScoreDoc[0];

    public static Version parseVersion(@Nullable String version, Version defaultVersion, ESLogger logger) {
        if (version == null) {
            return defaultVersion;
        }
        if ("3.5".equals(version)) {
            return Version.LUCENE_35;
        }
        if ("3.4".equals(version)) {
            return Version.LUCENE_34;
        }
        if ("3.3".equals(version)) {
            return Version.LUCENE_33;
        }
        if ("3.2".equals(version)) {
            return Version.LUCENE_32;
        }
        if ("3.1".equals(version)) {
            return Version.LUCENE_31;
        }
        if ("3.0".equals(version)) {
            return Version.LUCENE_30;
        }
        logger.warn("no version match {}, default to {}", version, defaultVersion);
        return defaultVersion;
    }

    public static long count(IndexSearcher searcher, Query query, float minScore) throws IOException {
        return count(searcher, query, null, minScore);
    }

    public static long count(IndexSearcher searcher, Query query, Filter filter, float minScore) throws IOException {
        CountCollector countCollector = new CountCollector(minScore);
        searcher.search(query, filter, countCollector);
        return countCollector.count();
    }

    public static int docId(IndexReader reader, Term term) throws IOException {
        TermDocs termDocs = reader.termDocs(term);
        try {
            if (termDocs.next()) {
                return termDocs.doc();
            }
            return NO_DOC;
        } finally {
            termDocs.close();
        }
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
        } catch (IOException e) {
            return false;
        }
    }

    public static TopDocs readTopDocs(StreamInput in) throws IOException {
        if (!in.readBoolean()) {
            // no docs
            return null;
        }
        if (in.readBoolean()) {
            int totalHits = in.readVInt();
            float maxScore = in.readFloat();

            SortField[] fields = new SortField[in.readVInt()];
            for (int i = 0; i < fields.length; i++) {
                String field = null;
                if (in.readBoolean()) {
                    field = in.readUTF();
                }
                fields[i] = new SortField(field, in.readVInt(), in.readBoolean());
            }

            FieldDoc[] fieldDocs = new FieldDoc[in.readVInt()];
            for (int i = 0; i < fieldDocs.length; i++) {
                Comparable[] cFields = new Comparable[in.readVInt()];
                for (int j = 0; j < cFields.length; j++) {
                    byte type = in.readByte();
                    if (type == 0) {
                        cFields[j] = null;
                    } else if (type == 1) {
                        cFields[j] = in.readUTF();
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
                    } else {
                        throw new IOException("Can't match type [" + type + "]");
                    }
                }
                fieldDocs[i] = new FieldDoc(in.readVInt(), in.readFloat(), cFields);
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

    public static void writeTopDocs(StreamOutput out, TopDocs topDocs, int from) throws IOException {
        if (topDocs.scoreDocs.length - from < 0) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
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
                    out.writeUTF(sortField.getField());
                }
                if (sortField.getComparatorSource() != null) {
                    out.writeVInt(((FieldDataType.ExtendedFieldComparatorSource) sortField.getComparatorSource()).reducedType());
                } else {
                    out.writeVInt(sortField.getType());
                }
                out.writeBoolean(sortField.getReverse());
            }

            out.writeVInt(topDocs.scoreDocs.length - from);
            int index = 0;
            for (ScoreDoc doc : topFieldDocs.scoreDocs) {
                if (index++ < from) {
                    continue;
                }
                FieldDoc fieldDoc = (FieldDoc) doc;
                out.writeVInt(fieldDoc.fields.length);
                for (Object field : fieldDoc.fields) {
                    if (field == null) {
                        out.writeByte((byte) 0);
                    } else {
                        Class type = field.getClass();
                        if (type == String.class) {
                            out.writeByte((byte) 1);
                            out.writeUTF((String) field);
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
                        } else {
                            throw new IOException("Can't handle sort field value of type [" + type + "]");
                        }
                    }
                }

                out.writeVInt(doc.doc);
                out.writeFloat(doc.score);
            }
        } else {
            out.writeBoolean(false);
            out.writeVInt(topDocs.totalHits);
            out.writeFloat(topDocs.getMaxScore());

            out.writeVInt(topDocs.scoreDocs.length - from);
            int index = 0;
            for (ScoreDoc doc : topDocs.scoreDocs) {
                if (index++ < from) {
                    continue;
                }
                out.writeVInt(doc.doc);
                out.writeFloat(doc.score);
            }
        }
    }

    public static Explanation readExplanation(StreamInput in) throws IOException {
        float value = in.readFloat();
        String description = in.readUTF();
        Explanation explanation = new Explanation(value, description);
        if (in.readBoolean()) {
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                explanation.addDetail(readExplanation(in));
            }
        }
        return explanation;
    }

    public static void writeExplanation(StreamOutput out, Explanation explanation) throws IOException {
        out.writeFloat(explanation.getValue());
        out.writeUTF(explanation.getDescription());
        Explanation[] subExplanations = explanation.getDetails();
        if (subExplanations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(subExplanations.length);
            for (Explanation subExp : subExplanations) {
                writeExplanation(out, subExp);
            }
        }
    }

    private static final Field segmentReaderSegmentInfoField;

    static {
        Field segmentReaderSegmentInfoFieldX = null;
        try {
            segmentReaderSegmentInfoFieldX = SegmentReader.class.getDeclaredField("si");
            segmentReaderSegmentInfoFieldX.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        segmentReaderSegmentInfoField = segmentReaderSegmentInfoFieldX;
    }

    public static SegmentInfo getSegmentInfo(SegmentReader reader) {
        try {
            return (SegmentInfo) segmentReaderSegmentInfoField.get(reader);
        } catch (IllegalAccessException e) {
            return null;
        }
    }

    public static class CountCollector extends Collector {

        private final float minScore;
        private Scorer scorer;
        private long count;

        public CountCollector(float minScore) {
            this.minScore = minScore;
        }

        public long count() {
            return this.count;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
            if (scorer.score() > minScore) {
                count++;
            }
        }

        @Override
        public void setNextReader(IndexReader reader, int docBase) throws IOException {
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return true;
        }
    }

    public static class ExistsCollector extends Collector {

        private boolean exists;

        public void reset() {
            exists = false;
        }

        public boolean exists() {
            return exists;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.exists = false;
        }

        @Override
        public void collect(int doc) throws IOException {
            exists = true;
        }

        @Override
        public void setNextReader(IndexReader reader, int docBase) throws IOException {
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return true;
        }
    }

    private Lucene() {

    }
}
