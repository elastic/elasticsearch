/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elasticsearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.lucene.search.postingshighlight;

import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.UnicodeUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.BreakIterator;
import java.util.*;

/*
FORKED from Lucene 4.5 to be able to:
1) support discrete highlighting for multiple values, so that we can return a different snippet per value when highlighting the whole text
2) call the highlightField method directly from subclasses and provide the terms by ourselves
3) Applied LUCENE-4906 to allow PassageFormatter to return arbitrary objects (LUCENE 4.6)

All our changes start with //BEGIN EDIT
 */
public class XPostingsHighlighter {

    //BEGIN EDIT added method to override offset for current value (default 0)
    //we need this to perform discrete highlighting per field
    protected int getOffsetForCurrentValue(String field, int docId) {
        return 0;
    }
    //END EDIT

    //BEGIN EDIT
    //we need this to fix scoring when highlighting every single value separately, since the score depends on the total length of the field (all values rather than only the current one)
    protected int getContentLength(String field, int docId) {
        return -1;
    }
    //END EDIT


    // TODO: maybe allow re-analysis for tiny fields? currently we require offsets,
    // but if the analyzer is really fast and the field is tiny, this might really be
    // unnecessary.

    /** for rewriting: we don't want slow processing from MTQs */
    private static final IndexSearcher EMPTY_INDEXSEARCHER;
    static {
      try {
        IndexReader emptyReader = new MultiReader();
        EMPTY_INDEXSEARCHER = new IndexSearcher(emptyReader);
        EMPTY_INDEXSEARCHER.setQueryCache(null);
      } catch (IOException bogus) {
        throw new RuntimeException(bogus);
      }
    }

    /** Default maximum content size to process. Typically snippets
     *  closer to the beginning of the document better summarize its content */
    public static final int DEFAULT_MAX_LENGTH = 10000;

    private final int maxLength;

    /** Set the first time {@link #getFormatter} is called,
     *  and then reused. */
    private PassageFormatter defaultFormatter;

    /** Set the first time {@link #getScorer} is called,
     *  and then reused. */
    private PassageScorer defaultScorer;

    /**
     * Creates a new highlighter with default parameters.
     */
    public XPostingsHighlighter() {
        this(DEFAULT_MAX_LENGTH);
    }

    /**
     * Creates a new highlighter, specifying maximum content length.
     * @param maxLength maximum content size to process.
     * @throws IllegalArgumentException if <code>maxLength</code> is negative or <code>Integer.MAX_VALUE</code>
     */
    public XPostingsHighlighter(int maxLength) {
        if (maxLength < 0 || maxLength == Integer.MAX_VALUE) {
            // two reasons: no overflow problems in BreakIterator.preceding(offset+1),
            // our sentinel in the offsets queue uses this value to terminate.
            throw new IllegalArgumentException("maxLength must be < Integer.MAX_VALUE");
        }
        this.maxLength = maxLength;
    }

    /** Returns the {@link java.text.BreakIterator} to use for
     *  dividing text into passages.  This returns
     *  {@link java.text.BreakIterator#getSentenceInstance(java.util.Locale)} by default;
     *  subclasses can override to customize. */
    protected BreakIterator getBreakIterator(String field) {
        return BreakIterator.getSentenceInstance(Locale.ROOT);
    }

    /** Returns the {@link PassageFormatter} to use for
     *  formatting passages into highlighted snippets.  This
     *  returns a new {@code PassageFormatter} by default;
     *  subclasses can override to customize. */
    protected PassageFormatter getFormatter(String field) {
        if (defaultFormatter == null) {
            defaultFormatter = new DefaultPassageFormatter();
        }
        return defaultFormatter;
    }

    /** Returns the {@link PassageScorer} to use for
     *  ranking passages.  This
     *  returns a new {@code PassageScorer} by default;
     *  subclasses can override to customize. */
    protected PassageScorer getScorer(String field) {
        if (defaultScorer == null) {
            defaultScorer = new PassageScorer();
        }
        return defaultScorer;
    }

    /**
     * Highlights the top passages from a single field.
     *
     * @param field field name to highlight.
     *        Must have a stored string value and also be indexed with offsets.
     * @param query query to highlight.
     * @param searcher searcher that was previously used to execute the query.
     * @param topDocs TopDocs containing the summary result documents to highlight.
     * @return Array of formatted snippets corresponding to the documents in <code>topDocs</code>.
     *         If no highlights were found for a document, the
     *         first sentence for the field will be returned.
     * @throws java.io.IOException if an I/O error occurred during processing
     * @throws IllegalArgumentException if <code>field</code> was indexed without
     *         {@link org.apache.lucene.index.FieldInfo.IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
     */
    public String[] highlight(String field, Query query, IndexSearcher searcher, TopDocs topDocs) throws IOException {
        return highlight(field, query, searcher, topDocs, 1);
    }

    /**
     * Highlights the top-N passages from a single field.
     *
     * @param field field name to highlight.
     *        Must have a stored string value and also be indexed with offsets.
     * @param query query to highlight.
     * @param searcher searcher that was previously used to execute the query.
     * @param topDocs TopDocs containing the summary result documents to highlight.
     * @param maxPassages The maximum number of top-N ranked passages used to
     *        form the highlighted snippets.
     * @return Array of formatted snippets corresponding to the documents in <code>topDocs</code>.
     *         If no highlights were found for a document, the
     *         first {@code maxPassages} sentences from the
     *         field will be returned.
     * @throws IOException if an I/O error occurred during processing
     * @throws IllegalArgumentException if <code>field</code> was indexed without
     *         {@link org.apache.lucene.index.FieldInfo.IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
     */
    public String[] highlight(String field, Query query, IndexSearcher searcher, TopDocs topDocs, int maxPassages) throws IOException {
        Map<String,String[]> res = highlightFields(new String[] { field }, query, searcher, topDocs, new int[] { maxPassages });
        return res.get(field);
    }

    /**
     * Highlights the top passages from multiple fields.
     * <p>
     * Conceptually, this behaves as a more efficient form of:
     * <pre class="prettyprint">
     * Map m = new HashMap();
     * for (String field : fields) {
     *   m.put(field, highlight(field, query, searcher, topDocs));
     * }
     * return m;
     * </pre>
     *
     * @param fields field names to highlight.
     *        Must have a stored string value and also be indexed with offsets.
     * @param query query to highlight.
     * @param searcher searcher that was previously used to execute the query.
     * @param topDocs TopDocs containing the summary result documents to highlight.
     * @return Map keyed on field name, containing the array of formatted snippets
     *         corresponding to the documents in <code>topDocs</code>.
     *         If no highlights were found for a document, the
     *         first sentence from the field will be returned.
     * @throws IOException if an I/O error occurred during processing
     * @throws IllegalArgumentException if <code>field</code> was indexed without
     *         {@link org.apache.lucene.index.FieldInfo.IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
     */
    public Map<String,String[]> highlightFields(String fields[], Query query, IndexSearcher searcher, TopDocs topDocs) throws IOException {
        int maxPassages[] = new int[fields.length];
        Arrays.fill(maxPassages, 1);
        return highlightFields(fields, query, searcher, topDocs, maxPassages);
    }

    /**
     * Highlights the top-N passages from multiple fields.
     * <p>
     * Conceptually, this behaves as a more efficient form of:
     * <pre class="prettyprint">
     * Map m = new HashMap();
     * for (String field : fields) {
     *   m.put(field, highlight(field, query, searcher, topDocs, maxPassages));
     * }
     * return m;
     * </pre>
     *
     * @param fields field names to highlight.
     *        Must have a stored string value and also be indexed with offsets.
     * @param query query to highlight.
     * @param searcher searcher that was previously used to execute the query.
     * @param topDocs TopDocs containing the summary result documents to highlight.
     * @param maxPassages The maximum number of top-N ranked passages per-field used to
     *        form the highlighted snippets.
     * @return Map keyed on field name, containing the array of formatted snippets
     *         corresponding to the documents in <code>topDocs</code>.
     *         If no highlights were found for a document, the
     *         first {@code maxPassages} sentences from the
     *         field will be returned.
     * @throws IOException if an I/O error occurred during processing
     * @throws IllegalArgumentException if <code>field</code> was indexed without
     *         {@link org.apache.lucene.index.FieldInfo.IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
     */
    public Map<String,String[]> highlightFields(String fields[], Query query, IndexSearcher searcher, TopDocs topDocs, int maxPassages[]) throws IOException {
        final ScoreDoc scoreDocs[] = topDocs.scoreDocs;
        int docids[] = new int[scoreDocs.length];
        for (int i = 0; i < docids.length; i++) {
            docids[i] = scoreDocs[i].doc;
        }

        return highlightFields(fields, query, searcher, docids, maxPassages);
    }

    /**
     * Highlights the top-N passages from multiple fields,
     * for the provided int[] docids.
     *
     * @param fieldsIn field names to highlight.
     *        Must have a stored string value and also be indexed with offsets.
     * @param query query to highlight.
     * @param searcher searcher that was previously used to execute the query.
     * @param docidsIn containing the document IDs to highlight.
     * @param maxPassagesIn The maximum number of top-N ranked passages per-field used to
     *        form the highlighted snippets.
     * @return Map keyed on field name, containing the array of formatted snippets
     *         corresponding to the documents in <code>topDocs</code>.
     *         If no highlights were found for a document, the
     *         first {@code maxPassages} from the field will
     *         be returned.
     * @throws IOException if an I/O error occurred during processing
     * @throws IllegalArgumentException if <code>field</code> was indexed without
     *         {@link org.apache.lucene.index.FieldInfo.IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
     */
    public Map<String,String[]> highlightFields(String fieldsIn[], Query query, IndexSearcher searcher, int[] docidsIn, int maxPassagesIn[]) throws IOException {
        Map<String,String[]> snippets = new HashMap<>();
        for(Map.Entry<String,Object[]> ent : highlightFieldsAsObjects(fieldsIn, query, searcher, docidsIn, maxPassagesIn).entrySet()) {
            Object[] snippetObjects = ent.getValue();
            String[] snippetStrings = new String[snippetObjects.length];
            snippets.put(ent.getKey(), snippetStrings);
            for(int i=0;i<snippetObjects.length;i++) {
                Object snippet = snippetObjects[i];
                if (snippet != null) {
                    snippetStrings[i] = snippet.toString();
                }
            }
        }

        return snippets;
    }

    public Map<String,Object[]> highlightFieldsAsObjects(String fieldsIn[], Query query, IndexSearcher searcher, int[] docidsIn, int maxPassagesIn[]) throws IOException {
        if (fieldsIn.length < 1) {
            throw new IllegalArgumentException("fieldsIn must not be empty");
        }
        if (fieldsIn.length != maxPassagesIn.length) {
            throw new IllegalArgumentException("invalid number of maxPassagesIn");
        }
        SortedSet<Term> queryTerms = new TreeSet<>();
        EMPTY_INDEXSEARCHER.createNormalizedWeight(query, false).extractTerms(queryTerms);

        IndexReaderContext readerContext = searcher.getIndexReader().getContext();
        List<LeafReaderContext> leaves = readerContext.leaves();

        // Make our own copies because we sort in-place:
        int[] docids = new int[docidsIn.length];
        System.arraycopy(docidsIn, 0, docids, 0, docidsIn.length);
        final String fields[] = new String[fieldsIn.length];
        System.arraycopy(fieldsIn, 0, fields, 0, fieldsIn.length);
        final int maxPassages[] = new int[maxPassagesIn.length];
        System.arraycopy(maxPassagesIn, 0, maxPassages, 0, maxPassagesIn.length);

        // sort for sequential io
        Arrays.sort(docids);
        new InPlaceMergeSorter() {

            @Override
            protected void swap(int i, int j) {
                String tmp = fields[i];
                fields[i] = fields[j];
                fields[j] = tmp;
                int tmp2 = maxPassages[i];
                maxPassages[i] = maxPassages[j];
                maxPassages[j] = tmp2;
            }

            @Override
            protected int compare(int i, int j) {
                return fields[i].compareTo(fields[j]);
            }

        }.sort(0, fields.length);

        // pull stored data:
        String[][] contents = loadFieldValues(searcher, fields, docids, maxLength);

        Map<String,Object[]> highlights = new HashMap<>();
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
            int numPassages = maxPassages[i];

            Term floor = new Term(field, "");
            Term ceiling = new Term(field, UnicodeUtil.BIG_TERM);
            SortedSet<Term> fieldTerms = queryTerms.subSet(floor, ceiling);
            // TODO: should we have some reasonable defaults for term pruning? (e.g. stopwords)

            // Strip off the redundant field:
            BytesRef terms[] = new BytesRef[fieldTerms.size()];
            int termUpto = 0;
            for(Term term : fieldTerms) {
                terms[termUpto++] = term.bytes();
            }
            Map<Integer,Object> fieldHighlights = highlightField(field, contents[i], getBreakIterator(field), terms, docids, leaves, numPassages);

            Object[] result = new Object[docids.length];
            for (int j = 0; j < docidsIn.length; j++) {
                result[j] = fieldHighlights.get(docidsIn[j]);
            }
            highlights.put(field, result);
        }
        return highlights;
    }

    /** Loads the String values for each field X docID to be
     *  highlighted.  By default this loads from stored
     *  fields, but a subclass can change the source.  This
     *  method should allocate the String[fields.length][docids.length]
     *  and fill all values.  The returned Strings must be
     *  identical to what was indexed. */
    protected String[][] loadFieldValues(IndexSearcher searcher, String[] fields, int[] docids, int maxLength) throws IOException {
        String contents[][] = new String[fields.length][docids.length];
        char valueSeparators[] = new char[fields.length];
        for (int i = 0; i < fields.length; i++) {
            valueSeparators[i] = getMultiValuedSeparator(fields[i]);
        }
        LimitedStoredFieldVisitor visitor = new LimitedStoredFieldVisitor(fields, valueSeparators, maxLength);
        for (int i = 0; i < docids.length; i++) {
            searcher.doc(docids[i], visitor);
            for (int j = 0; j < fields.length; j++) {
                contents[j][i] = visitor.getValue(j);
            }
            visitor.reset();
        }
        return contents;
    }

    /**
     * Returns the logical separator between values for multi-valued fields.
     * The default value is a space character, which means passages can span across values,
     * but a subclass can override, for example with {@code U+2029 PARAGRAPH SEPARATOR (PS)}
     * if each value holds a discrete passage for highlighting.
     */
    protected char getMultiValuedSeparator(String field) {
        return ' ';
    }

    //BEGIN EDIT: made protected so that we can call from our subclass and pass in the terms by ourselves
    protected Map<Integer,Object> highlightField(String field, String contents[], BreakIterator bi, BytesRef terms[], int[] docids, List<LeafReaderContext> leaves, int maxPassages) throws IOException {
    //private Map<Integer,Object> highlightField(String field, String contents[], BreakIterator bi, BytesRef terms[], int[] docids, List<LeafReaderContext > leaves, int maxPassages) throws IOException {
    //END EDIT

        Map<Integer,Object> highlights = new HashMap<>();

        // reuse in the real sense... for docs in same segment we just advance our old enum
        PostingsEnum postings[] = null;
        TermsEnum termsEnum = null;
        int lastLeaf = -1;

        PassageFormatter fieldFormatter = getFormatter(field);
        if (fieldFormatter == null) {
            throw new NullPointerException("PassageFormatter cannot be null");
        }

        for (int i = 0; i < docids.length; i++) {
            String content = contents[i];
            if (content.length() == 0) {
                continue; // nothing to do
            }
            bi.setText(content);
            int doc = docids[i];
            int leaf = ReaderUtil.subIndex(doc, leaves);
            LeafReaderContext subContext = leaves.get(leaf);
            LeafReader r = subContext.reader();
            Terms t = r.terms(field);
            if (t == null) {
                continue; // nothing to do
            }
            if (!t.hasOffsets()) {
                // no offsets available
                throw new IllegalArgumentException("field '" + field + "' was indexed without offsets, cannot highlight");
            }
            if (leaf != lastLeaf) {
                termsEnum = t.iterator();
                postings = new PostingsEnum[terms.length];
            }
            Passage passages[] = highlightDoc(field, terms, content.length(), bi, doc - subContext.docBase, termsEnum, postings, maxPassages);
            if (passages.length == 0) {
                passages = getEmptyHighlight(field, bi, maxPassages);
            }
            if (passages.length > 0) {
                // otherwise a null snippet (eg if field is missing
                // entirely from the doc)
                  highlights.put(doc, fieldFormatter.format(passages, content));
            }
            lastLeaf = leaf;
        }

        return highlights;
    }

    // algorithm: treat sentence snippets as miniature documents
    // we can intersect these with the postings lists via BreakIterator.preceding(offset),s
    // score each sentence as norm(sentenceStartOffset) * sum(weight * tf(freq))
    private Passage[] highlightDoc(String field, BytesRef terms[], int contentLength, BreakIterator bi, int doc,
                                   TermsEnum termsEnum, PostingsEnum[] postings, int n) throws IOException {

        //BEGIN EDIT added call to method that returns the offset for the current value (discrete highlighting)
        int valueOffset = getOffsetForCurrentValue(field, doc);
        //END EDIT

        PassageScorer scorer = getScorer(field);
        if (scorer == null) {
            throw new NullPointerException("PassageScorer cannot be null");
        }


        //BEGIN EDIT discrete highlighting
        // the scoring needs to be based on the length of the whole field (all values rather than only the current one)
        int totalContentLength = getContentLength(field, doc);
        if (totalContentLength == -1) {
            totalContentLength = contentLength;
        }
        //END EDIT


        PriorityQueue<OffsetsEnum> pq = new PriorityQueue<>();
        float weights[] = new float[terms.length];
        // initialize postings
        for (int i = 0; i < terms.length; i++) {
            PostingsEnum de = postings[i];
            int pDoc;
            if (de == EMPTY) {
                continue;
            } else if (de == null) {
                postings[i] = EMPTY; // initially
                if (!termsEnum.seekExact(terms[i])) {
                    continue; // term not found
                }
                de = postings[i] = termsEnum.postings(null, null, PostingsEnum.OFFSETS);
                assert de != null;
                pDoc = de.advance(doc);
            } else {
                pDoc = de.docID();
                if (pDoc < doc) {
                    pDoc = de.advance(doc);
                }
            }

            if (doc == pDoc) {
                //BEGIN EDIT we take into account the length of the whole field (all values) to properly score the snippets
                weights[i] = scorer.weight(totalContentLength, de.freq());
                //weights[i] = scorer.weight(contentLength, de.freq());
                //END EDIT
                de.nextPosition();
                pq.add(new OffsetsEnum(de, i));
            }
        }

        pq.add(new OffsetsEnum(EMPTY, Integer.MAX_VALUE)); // a sentinel for termination

        PriorityQueue<Passage> passageQueue = new PriorityQueue<>(n, new Comparator<Passage>() {
            @Override
            public int compare(Passage left, Passage right) {
                if (left.score < right.score) {
                    return -1;
                } else if (left.score > right.score) {
                    return 1;
                } else {
                    return left.startOffset - right.startOffset;
                }
            }
        });
        Passage current = new Passage();

        OffsetsEnum off;
        while ((off = pq.poll()) != null) {
            final PostingsEnum dp = off.dp;

            int start = dp.startOffset();
            assert start >= 0;
            int end = dp.endOffset();
            // LUCENE-5166: this hit would span the content limit... however more valid
            // hits may exist (they are sorted by start). so we pretend like we never
            // saw this term, it won't cause a passage to be added to passageQueue or anything.
            assert EMPTY.startOffset() == Integer.MAX_VALUE;
            if (start < contentLength && end > contentLength) {
                continue;
            }


            //BEGIN EDIT support for discrete highlighting (added block code)
            //switch to the first match in the current value if there is one
            boolean seenEnough = false;
            while (start < valueOffset) {
                if (off.pos == dp.freq()) {
                    seenEnough = true;
                    break;
                } else {
                    off.pos++;
                    dp.nextPosition();
                    start = dp.startOffset();
                    end = dp.endOffset();
                }
            }

            //continue with next term if we've already seen the current one all the times it appears
            //that means that the current value doesn't hold matches for the current term
            if (seenEnough) {
                continue;
            }

            //we now subtract the offset of the current value to both start and end
            start -= valueOffset;
            end -= valueOffset;
            //END EDIT


            if (start >= current.endOffset) {
                if (current.startOffset >= 0) {
                    // finalize current
                    //BEGIN EDIT we take into account the value offset when scoring the snippet based on its position
                    current.score *= scorer.norm(current.startOffset + valueOffset);
                    //current.score *= scorer.norm(current.startOffset);
                    //END EDIT
                    // new sentence: first add 'current' to queue
                    if (passageQueue.size() == n && current.score < passageQueue.peek().score) {
                        current.reset(); // can't compete, just reset it
                    } else {
                        passageQueue.offer(current);
                        if (passageQueue.size() > n) {
                            current = passageQueue.poll();
                            current.reset();
                        } else {
                            current = new Passage();
                        }
                    }
                }
                // if we exceed limit, we are done
                if (start >= contentLength) {
                    Passage passages[] = new Passage[passageQueue.size()];
                    passageQueue.toArray(passages);
                    for (Passage p : passages) {
                        p.sort();
                    }
                    // sort in ascending order
                    Arrays.sort(passages, new Comparator<Passage>() {
                        @Override
                        public int compare(Passage left, Passage right) {
                            return left.startOffset - right.startOffset;
                        }
                    });
                    return passages;
                }
                // advance breakiterator
                assert BreakIterator.DONE < 0;
                current.startOffset = Math.max(bi.preceding(start+1), 0);
                current.endOffset = Math.min(bi.next(), contentLength);
            }
            int tf = 0;
            while (true) {
                tf++;
                current.addMatch(start, end, terms[off.id]);
                if (off.pos == dp.freq()) {
                    break; // removed from pq
                } else {
                    off.pos++;
                    dp.nextPosition();
                    //BEGIN EDIT support for discrete highlighting
                    start = dp.startOffset() - valueOffset;
                    end = dp.endOffset() - valueOffset;
                    //start = dp.startOffset();
                    //end = dp.endOffset();
                    //END EDIT
                }
                if (start >= current.endOffset || end > contentLength) {
                    pq.offer(off);
                    break;
                }
            }
            current.score += weights[off.id] * scorer.tf(tf, current.endOffset - current.startOffset);
        }

        // Dead code but compiler disagrees:
        assert false;
        return null;
    }

    /** Called to summarize a document when no hits were
     *  found.  By default this just returns the first
     *  {@code maxPassages} sentences; subclasses can override
     *  to customize. */
    protected Passage[] getEmptyHighlight(String fieldName, BreakIterator bi, int maxPassages) {
        // BreakIterator should be un-next'd:
        List<Passage> passages = new ArrayList<>();
        int pos = bi.current();
        assert pos == 0;
        while (passages.size() < maxPassages) {
            int next = bi.next();
            if (next == BreakIterator.DONE) {
                break;
            }
            Passage passage = new Passage();
            passage.score = Float.NaN;
            passage.startOffset = pos;
            passage.endOffset = next;
            passages.add(passage);
            pos = next;
        }

        return passages.toArray(new Passage[passages.size()]);
    }

    private static class OffsetsEnum implements Comparable<OffsetsEnum> {
        PostingsEnum dp;
        int pos;
        int id;

        OffsetsEnum(PostingsEnum dp, int id) throws IOException {
            this.dp = dp;
            this.id = id;
            this.pos = 1;
        }

        @Override
        public int compareTo(OffsetsEnum other) {
            try {
                int off = dp.startOffset();
                int otherOff = other.dp.startOffset();
                if (off == otherOff) {
                    return id - other.id;
                } else {
                    return Long.signum(((long)off) - otherOff);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final PostingsEnum EMPTY = new PostingsEnum() {

        @Override
        public int nextPosition() throws IOException { return 0; }

        @Override
        public int startOffset() throws IOException { return Integer.MAX_VALUE; }

        @Override
        public int endOffset() throws IOException { return Integer.MAX_VALUE; }

        @Override
        public BytesRef getPayload() throws IOException { return null; }

        @Override
        public int freq() throws IOException { return 0; }

        @Override
        public int docID() { return NO_MORE_DOCS; }

        @Override
        public int nextDoc() throws IOException { return NO_MORE_DOCS; }

        @Override
        public int advance(int target) throws IOException { return NO_MORE_DOCS; }

        @Override
        public long cost() { return 0; }
    };

    private static class LimitedStoredFieldVisitor extends StoredFieldVisitor {
        private final String fields[];
        private final char valueSeparators[];
        private final int maxLength;
        private final StringBuilder builders[];
        private int currentField = -1;

        public LimitedStoredFieldVisitor(String fields[], char valueSeparators[], int maxLength) {
            assert fields.length == valueSeparators.length;
            this.fields = fields;
            this.valueSeparators = valueSeparators;
            this.maxLength = maxLength;
            builders = new StringBuilder[fields.length];
            for (int i = 0; i < builders.length; i++) {
                builders[i] = new StringBuilder();
            }
        }

        @Override
        public void stringField(FieldInfo fieldInfo, byte[] bytes) throws IOException {
            String value = new String(bytes, StandardCharsets.UTF_8);
            assert currentField >= 0;
            StringBuilder builder = builders[currentField];
            if (builder.length() > 0 && builder.length() < maxLength) {
                builder.append(valueSeparators[currentField]);
            }
            if (builder.length() + value.length() > maxLength) {
                builder.append(value, 0, maxLength - builder.length());
            } else {
                builder.append(value);
            }
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) throws IOException {
            currentField = Arrays.binarySearch(fields, fieldInfo.name);
            if (currentField < 0) {
                return Status.NO;
            } else if (builders[currentField].length() > maxLength) {
                return fields.length == 1 ? Status.STOP : Status.NO;
            }
            return Status.YES;
        }

        String getValue(int i) {
            return builders[i].toString();
        }

        void reset() {
            currentField = -1;
            for (int i = 0; i < fields.length; i++) {
                builders[i].setLength(0);
            }
        }
    }
}
