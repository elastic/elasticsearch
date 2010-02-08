/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.util.lucene;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Version;
import org.elasticsearch.util.gnu.trove.TIntArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class Lucene {

    public static final StandardAnalyzer STANDARD_ANALYZER = new StandardAnalyzer(Version.LUCENE_CURRENT);
    public static final KeywordAnalyzer KEYWORD_ANALYZER = new KeywordAnalyzer();

    public static final int NO_DOC = -1;

    public static long count(IndexSearcher searcher, Query query, float minScore) throws IOException {
        CountCollector countCollector = new CountCollector(minScore);
        searcher.search(query, countCollector);
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

    public static TIntArrayList docIds(IndexReader reader, Term term, int expectedSize) throws IOException {
        TermDocs termDocs = reader.termDocs(term);
        TIntArrayList list = new TIntArrayList(expectedSize);
        try {
            while (termDocs.next()) {
                list.add(termDocs.doc());
            }
        } finally {
            termDocs.close();
        }
        return list;
    }

    /**
     * Closes the index reader, returning <tt>false</tt> if it failed to close.
     */
    public static boolean safeClose(IndexReader reader) {
        if (reader == null) {
            return true;
        }
        try {
            reader.close();
            return true;
        } catch (IOException e) {
            return false;
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

    public static TopDocs readTopDocs(DataInput in) throws IOException {
        if (!in.readBoolean()) {
            // no docs
            return null;
        }
        if (in.readBoolean()) {
            int totalHits = in.readInt();
            float maxScore = in.readFloat();

            SortField[] fields = new SortField[in.readInt()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = new SortField(in.readUTF(), in.readInt(), in.readBoolean());
            }

            FieldDoc[] fieldDocs = new FieldDoc[in.readInt()];
            for (int i = 0; i < fieldDocs.length; i++) {
                Comparable[] cFields = new Comparable[in.readInt()];
                for (int j = 0; j < cFields.length; j++) {
                    byte type = in.readByte();
                    if (type == 0) {
                        cFields[j] = in.readUTF();
                    } else if (type == 1) {
                        cFields[j] = in.readInt();
                    } else if (type == 2) {
                        cFields[j] = in.readLong();
                    } else if (type == 3) {
                        cFields[j] = in.readFloat();
                    } else if (type == 4) {
                        cFields[j] = in.readDouble();
                    } else if (type == 5) {
                        cFields[j] = in.readByte();
                    } else {
                        throw new IOException("Can't match type [" + type + "]");
                    }
                }
                fieldDocs[i] = new FieldDoc(in.readInt(), in.readFloat(), cFields);
            }
            return new TopFieldDocs(totalHits, fieldDocs, fields, maxScore);
        } else {
            int totalHits = in.readInt();
            float maxScore = in.readFloat();

            ScoreDoc[] scoreDocs = new ScoreDoc[in.readInt()];
            for (int i = 0; i < scoreDocs.length; i++) {
                scoreDocs[i] = new ScoreDoc(in.readInt(), in.readFloat());
            }
            return new TopDocs(totalHits, scoreDocs, maxScore);
        }
    }

    public static void writeTopDocs(DataOutput out, TopDocs topDocs, int from) throws IOException {
        if (topDocs.scoreDocs.length - from < 0) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        if (topDocs instanceof TopFieldDocs) {
            out.writeBoolean(true);
            TopFieldDocs topFieldDocs = (TopFieldDocs) topDocs;

            out.writeInt(topDocs.totalHits);
            out.writeFloat(topDocs.getMaxScore());

            out.writeInt(topFieldDocs.fields.length);
            for (SortField sortField : topFieldDocs.fields) {
                out.writeUTF(sortField.getField());
                out.writeInt(sortField.getType());
                out.writeBoolean(sortField.getReverse());
            }

            out.writeInt(topDocs.scoreDocs.length - from);
            int index = 0;
            for (ScoreDoc doc : topFieldDocs.scoreDocs) {
                if (index++ < from) {
                    continue;
                }
                FieldDoc fieldDoc = (FieldDoc) doc;
                out.writeInt(fieldDoc.fields.length);
                for (Comparable field : fieldDoc.fields) {
                    Class type = field.getClass();
                    if (type == String.class) {
                        out.write(0);
                        out.writeUTF((String) field);
                    } else if (type == Integer.class) {
                        out.write(1);
                        out.writeInt((Integer) field);
                    } else if (type == Long.class) {
                        out.write(2);
                        out.writeLong((Long) field);
                    } else if (type == Float.class) {
                        out.write(3);
                        out.writeFloat((Float) field);
                    } else if (type == Double.class) {
                        out.write(4);
                        out.writeDouble((Double) field);
                    } else if (type == Byte.class) {
                        out.write(5);
                        out.write((Byte) field);
                    } else {
                        throw new IOException("Can't handle sort field value of type [" + type + "]");
                    }
                }

                out.writeInt(doc.doc);
                out.writeFloat(doc.score);
            }
        } else {
            out.writeBoolean(false);
            out.writeInt(topDocs.totalHits);
            out.writeFloat(topDocs.getMaxScore());

            out.writeInt(topDocs.scoreDocs.length - from);
            int index = 0;
            for (ScoreDoc doc : topDocs.scoreDocs) {
                if (index++ < from) {
                    continue;
                }
                out.writeInt(doc.doc);
                out.writeFloat(doc.score);
            }
        }
    }

    public static Explanation readExplanation(DataInput in) throws IOException {
        float value = in.readFloat();
        String description = in.readUTF();
        Explanation explanation = new Explanation(value, description);
        if (in.readBoolean()) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                explanation.addDetail(readExplanation(in));
            }
        }
        return explanation;
    }

    public static void writeExplanation(DataOutput out, Explanation explanation) throws IOException {
        out.writeFloat(explanation.getValue());
        out.writeUTF(explanation.getDescription());
        Explanation[] subExplanations = explanation.getDetails();
        if (subExplanations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(subExplanations.length);
            for (Explanation subExp : subExplanations) {
                writeExplanation(out, subExp);
            }
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

        @Override public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override public void collect(int doc) throws IOException {
            if (scorer.score() > minScore) {
                count++;
            }
        }

        @Override public void setNextReader(IndexReader reader, int docBase) throws IOException {
        }

        @Override public boolean acceptsDocsOutOfOrder() {
            return true;
        }
    }

    private Lucene() {

    }
}
