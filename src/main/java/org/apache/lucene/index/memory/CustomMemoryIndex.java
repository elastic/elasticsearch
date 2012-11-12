package org.apache.lucene.index.memory;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Norm;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.OrdTermState;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.memory.MemoryIndexNormDocValues.SingleValueSource;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.RAMDirectory; // for javadocs
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants; // for javadocs
import org.elasticsearch.common.io.FastStringReader;

/**
 * High-performance single-document main memory Apache Lucene fulltext search index. 
 *
 * <h4>Overview</h4>
 *
 * This class is a replacement/substitute for a large subset of
 * {@link RAMDirectory} functionality. It is designed to
 * enable maximum efficiency for on-the-fly matchmaking combining structured and 
 * fuzzy fulltext search in realtime streaming applications such as Nux XQuery based XML 
 * message queues, publish-subscribe systems for Blogs/newsfeeds, text chat, data acquisition and 
 * distribution systems, application level routers, firewalls, classifiers, etc. 
 * Rather than targeting fulltext search of infrequent queries over huge persistent 
 * data archives (historic search), this class targets fulltext search of huge 
 * numbers of queries over comparatively small transient realtime data (prospective 
 * search). 
 * For example as in 
 * <pre class="prettyprint">
 * float score = search(String text, Query query)
 * </pre>
 * <p>
 * Each instance can hold at most one Lucene "document", with a document containing
 * zero or more "fields", each field having a name and a fulltext value. The
 * fulltext value is tokenized (split and transformed) into zero or more index terms 
 * (aka words) on <code>addField()</code>, according to the policy implemented by an
 * Analyzer. For example, Lucene analyzers can split on whitespace, normalize to lower case
 * for case insensitivity, ignore common terms with little discriminatory value such as "he", "in", "and" (stop
 * words), reduce the terms to their natural linguistic root form such as "fishing"
 * being reduced to "fish" (stemming), resolve synonyms/inflexions/thesauri 
 * (upon indexing and/or querying), etc. For details, see
 * <a target="_blank" href="http://today.java.net/pub/a/today/2003/07/30/LuceneIntro.html">Lucene Analyzer Intro</a>.
 * <p>
 * Arbitrary Lucene queries can be run against this class - see <a target="_blank" 
 * href="{@docRoot}/../queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package_description">
 * Lucene Query Syntax</a>
 * as well as <a target="_blank" 
 * href="http://today.java.net/pub/a/today/2003/11/07/QueryParserRules.html">Query Parser Rules</a>.
 * Note that a Lucene query selects on the field names and associated (indexed) 
 * tokenized terms, not on the original fulltext(s) - the latter are not stored 
 * but rather thrown away immediately after tokenization.
 * <p>
 * For some interesting background information on search technology, see Bob Wyman's
 * <a target="_blank" 
 * href="http://bobwyman.pubsub.com/main/2005/05/mary_hodder_poi.html">Prospective Search</a>, 
 * Jim Gray's
 * <a target="_blank" href="http://www.acmqueue.org/modules.php?name=Content&pa=showpage&pid=293&page=4">
 * A Call to Arms - Custom subscriptions</a>, and Tim Bray's
 * <a target="_blank" 
 * href="http://www.tbray.org/ongoing/When/200x/2003/07/30/OnSearchTOC">On Search, the Series</a>.
 *
 *
 * <h4>Example Usage</h4> 
 *
 * <pre class="prettyprint">
 * Analyzer analyzer = new SimpleAnalyzer(version);
 * MemoryIndex index = new MemoryIndex();
 * index.addField("content", "Readings about Salmons and other select Alaska fishing Manuals", analyzer);
 * index.addField("author", "Tales of James", analyzer);
 * QueryParser parser = new QueryParser(version, "content", analyzer);
 * float score = index.search(parser.parse("+author:james +salmon~ +fish* manual~"));
 * if (score &gt; 0.0f) {
 *     System.out.println("it's a match");
 * } else {
 *     System.out.println("no match found");
 * }
 * System.out.println("indexData=" + index.toString());
 * </pre>
 *
 *
 * <h4>Example XQuery Usage</h4> 
 *
 * <pre class="prettyprint">
 * (: An XQuery that finds all books authored by James that have something to do with "salmon fishing manuals", sorted by relevance :)
 * declare namespace lucene = "java:nux.xom.pool.FullTextUtil";
 * declare variable $query := "+salmon~ +fish* manual~"; (: any arbitrary Lucene query can go here :)
 *
 * for $book in /books/book[author="James" and lucene:match(abstract, $query) > 0.0]
 * let $score := lucene:match($book/abstract, $query)
 * order by $score descending
 * return $book
 * </pre>
 *
 *
 * <h4>No thread safety guarantees</h4>
 *
 * An instance can be queried multiple times with the same or different queries,
 * but an instance is not thread-safe. If desired use idioms such as:
 * <pre class="prettyprint">
 * MemoryIndex index = ...
 * synchronized (index) {
 *    // read and/or write index (i.e. add fields and/or query)
 * }
 * </pre>
 *
 *
 * <h4>Performance Notes</h4>
 *
 * Internally there's a new data structure geared towards efficient indexing 
 * and searching, plus the necessary support code to seamlessly plug into the Lucene 
 * framework.
 * <p>
 * This class performs very well for very small texts (e.g. 10 chars) 
 * as well as for large texts (e.g. 10 MB) and everything in between. 
 * Typically, it is about 10-100 times faster than <code>RAMDirectory</code>.
 * Note that <code>RAMDirectory</code> has particularly 
 * large efficiency overheads for small to medium sized texts, both in time and space.
 * Indexing a field with N tokens takes O(N) in the best case, and O(N logN) in the worst 
 * case. Memory consumption is probably larger than for <code>RAMDirectory</code>.
 * <p>
 * Example throughput of many simple term queries over a single MemoryIndex: 
 * ~500000 queries/sec on a MacBook Pro, jdk 1.5.0_06, server VM. 
 * As always, your mileage may vary.
 * <p>
 * If you're curious about
 * the whereabouts of bottlenecks, run java 1.5 with the non-perturbing '-server
 * -agentlib:hprof=cpu=samples,depth=10' flags, then study the trace log and
 * correlate its hotspot trailer with its call stack headers (see <a
 * target="_blank"
 * href="http://java.sun.com/developer/technicalArticles/Programming/HPROF.html">
 * hprof tracing </a>).
 *
 */
// LUCENE MONITOR - Support adding same field several times
//    -- Added pos to Info
//    -- Use current info of existing field
public class CustomMemoryIndex {

    /** info for each field: Map<String fieldName, Info field> */
    private final HashMap<String,Info> fields = new HashMap<String,Info>();

    /** fields sorted ascending by fieldName; lazily computed on demand */
    private transient Map.Entry<String,Info>[] sortedFields;

    /** pos: positions[3*i], startOffset: positions[3*i +1], endOffset: positions[3*i +2] */
    private final int stride;

    /** Could be made configurable; */
    private static final float docBoost = 1.0f;

    private static final boolean DEBUG = false;

    private HashMap<String,FieldInfo> fieldInfos = new HashMap<String,FieldInfo>();

    /**
     * Sorts term entries into ascending order; also works for
     * Arrays.binarySearch() and Arrays.sort()
     */
    private static final Comparator<Object> termComparator = new Comparator<Object>() {
        @SuppressWarnings({"unchecked","rawtypes"})
        public int compare(Object o1, Object o2) {
            if (o1 instanceof Map.Entry<?,?>) o1 = ((Map.Entry<?,?>) o1).getKey();
            if (o2 instanceof Map.Entry<?,?>) o2 = ((Map.Entry<?,?>) o2).getKey();
            if (o1 == o2) return 0;
            return ((Comparable) o1).compareTo((Comparable) o2);
        }
    };

    /**
     * Constructs an empty instance.
     */
    public CustomMemoryIndex() {
        this(false);
    }

    /**
     * Constructs an empty instance that can optionally store the start and end
     * character offset of each token term in the text. This can be useful for
     * highlighting of hit locations with the Lucene highlighter package.
     * Protected until the highlighter package matures, so that this can actually
     * be meaningfully integrated.
     *
     * @param storeOffsets
     *            whether or not to store the start and end character offset of
     *            each token term in the text
     */
    protected CustomMemoryIndex(boolean storeOffsets) {
        this.stride = storeOffsets ? 3 : 1;
    }

    /**
     * Convenience method; Tokenizes the given field text and adds the resulting
     * terms to the index; Equivalent to adding an indexed non-keyword Lucene
     * {@link org.apache.lucene.document.Field} that is tokenized, not stored,
     * termVectorStored with positions (or termVectorStored with positions and offsets),
     *
     * @param fieldName
     *            a name to be associated with the text
     * @param text
     *            the text to tokenize and index.
     * @param analyzer
     *            the analyzer to use for tokenization
     */
    public void addField(String fieldName, String text, Analyzer analyzer) {
        if (fieldName == null)
            throw new IllegalArgumentException("fieldName must not be null");
        if (text == null)
            throw new IllegalArgumentException("text must not be null");
        if (analyzer == null)
            throw new IllegalArgumentException("analyzer must not be null");

        TokenStream stream;
        try {
            stream = analyzer.tokenStream(fieldName, new FastStringReader(text));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        addField(fieldName, stream);
    }

    /**
     * Convenience method; Creates and returns a token stream that generates a
     * token for each keyword in the given collection, "as is", without any
     * transforming text analysis. The resulting token stream can be fed into
     * {@link #addField(String, TokenStream)}, perhaps wrapped into another
     * {@link org.apache.lucene.analysis.TokenFilter}, as desired.
     *
     * @param keywords
     *            the keywords to generate tokens for
     * @return the corresponding token stream
     */
    public <T> TokenStream keywordTokenStream(final Collection<T> keywords) {
        // TODO: deprecate & move this method into AnalyzerUtil?
        if (keywords == null)
            throw new IllegalArgumentException("keywords must not be null");

        return new TokenStream() {
            private Iterator<T> iter = keywords.iterator();
            private int start = 0;
            private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
            private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

            @Override
            public boolean incrementToken() {
                if (!iter.hasNext()) return false;

                T obj = iter.next();
                if (obj == null)
                    throw new IllegalArgumentException("keyword must not be null");

                String term = obj.toString();
                clearAttributes();
                termAtt.setEmpty().append(term);
                offsetAtt.setOffset(start, start+termAtt.length());
                start += term.length() + 1; // separate words by 1 (blank) character
                return true;
            }
        };
    }

    /**
     * Equivalent to <code>addField(fieldName, stream, 1.0f)</code>.
     *
     * @param fieldName
     *            a name to be associated with the text
     * @param stream
     *            the token stream to retrieve tokens from
     */
    public void addField(String fieldName, TokenStream stream) {
        addField(fieldName, stream, 1.0f);
    }

    /**
     * Iterates over the given token stream and adds the resulting terms to the index;
     * Equivalent to adding a tokenized, indexed, termVectorStored, unstored,
     * Lucene {@link org.apache.lucene.document.Field}.
     * Finally closes the token stream. Note that untokenized keywords can be added with this method via
     * {@link #keywordTokenStream(Collection)}, the Lucene <code>KeywordTokenizer</code> or similar utilities.
     *
     * @param fieldName
     *            a name to be associated with the text
     * @param stream
     *            the token stream to retrieve tokens from.
     * @param boost
     *            the boost factor for hits for this field
     * @see org.apache.lucene.document.Field#setBoost(float)
     */
    public void addField(String fieldName, TokenStream stream, float boost) {
        try {
            if (fieldName == null)
                throw new IllegalArgumentException("fieldName must not be null");
            if (stream == null)
                throw new IllegalArgumentException("token stream must not be null");
            if (boost <= 0.0f)
                throw new IllegalArgumentException("boost factor must be greater than 0.0");

            HashMap<BytesRef,ArrayIntList> terms = new HashMap<BytesRef,ArrayIntList>();
            int numTokens = 0;
            int numOverlapTokens = 0;
            int pos = -1;

            if (!fieldInfos.containsKey(fieldName)) {
                fieldInfos.put(fieldName,
                        new FieldInfo(fieldName, true, fieldInfos.size(), false, false, false, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, null, null, null));
            }


            // CHANGE
            if (fields.get(fieldName) != null) {
                Info info = fields.get(fieldName);
                terms = info.terms;
                numTokens = info.numTokens;
                numOverlapTokens = info.numOverlapTokens;
                pos = info.pos;
            } else {
                terms = new HashMap<BytesRef, ArrayIntList>();
            }
            TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
            PositionIncrementAttribute posIncrAttribute = stream.addAttribute(PositionIncrementAttribute.class);
            OffsetAttribute offsetAtt = stream.addAttribute(OffsetAttribute.class);
            BytesRef ref = termAtt.getBytesRef();
            stream.reset();
            while (stream.incrementToken()) {
                termAtt.fillBytesRef();
                if (ref.length == 0) continue; // nothing to do
//        if (DEBUG) System.err.println("token='" + term + "'");
                numTokens++;
                final int posIncr = posIncrAttribute.getPositionIncrement();
                if (posIncr == 0)
                    numOverlapTokens++;
                pos += posIncr;

                ArrayIntList positions = terms.get(ref);
                if (positions == null) { // term not seen before
                    positions = new ArrayIntList(stride);
                    terms.put(BytesRef.deepCopyOf(ref), positions);
                }
                if (stride == 1) {
                    positions.add(pos);
                } else {
                    positions.add(pos, offsetAtt.startOffset(), offsetAtt.endOffset());
                }
            }
            stream.end();

            // ensure infos.numTokens > 0 invariant; needed for correct operation of terms()
            if (numTokens > 0) {
                boost = boost * docBoost; // see DocumentWriter.addDocument(...)
                fields.put(fieldName, new Info(terms, numTokens, numOverlapTokens, boost, pos));
                sortedFields = null;    // invalidate sorted view, if any
            }
        } catch (IOException e) { // can never happen
            throw new RuntimeException(e);
        } finally {
            try {
                if (stream != null) stream.close();
            } catch (IOException e2) {
                throw new RuntimeException(e2);
            }
        }
    }

    /**
     * Creates and returns a searcher that can be used to execute arbitrary
     * Lucene queries and to collect the resulting query results as hits.
     *
     * @return a searcher
     */
    public IndexSearcher createSearcher() {
        MemoryIndexReader reader = new MemoryIndexReader();
        IndexSearcher searcher = new IndexSearcher(reader); // ensures no auto-close !!
        reader.setSearcher(searcher); // to later get hold of searcher.getSimilarity()
        return searcher;
    }

    /**
     * Convenience method that efficiently returns the relevance score by
     * matching this index against the given Lucene query expression.
     *
     * @param query
     *            an arbitrary Lucene query to run against this index
     * @return the relevance score of the matchmaking; A number in the range
     *         [0.0 .. 1.0], with 0.0 indicating no match. The higher the number
     *         the better the match.
     *
     */
    public float search(Query query) {
        if (query == null)
            throw new IllegalArgumentException("query must not be null");

        IndexSearcher searcher = createSearcher();
        try {
            final float[] scores = new float[1]; // inits to 0.0f (no match)
            searcher.search(query, new Collector() {
                private Scorer scorer;

                @Override
                public void collect(int doc) throws IOException {
                    scores[0] = scorer.score();
                }

                @Override
                public void setScorer(Scorer scorer) {
                    this.scorer = scorer;
                }

                @Override
                public boolean acceptsDocsOutOfOrder() {
                    return true;
                }

                @Override
                public void setNextReader(AtomicReaderContext context) { }
            });
            float score = scores[0];
            return score;
        } catch (IOException e) { // can never happen (RAMDirectory)
            throw new RuntimeException(e);
        } finally {
            // searcher.close();
            /*
            * Note that it is harmless and important for good performance to
            * NOT close the index reader!!! This avoids all sorts of
            * unnecessary baggage and locking in the Lucene IndexReader
            * superclass, all of which is completely unnecessary for this main
            * memory index data structure without thread-safety claims.
            *
            * Wishing IndexReader would be an interface...
            *
            * Actually with the new tight createSearcher() API auto-closing is now
            * made impossible, hence searcher.close() would be harmless and also
            * would not degrade performance...
            */
        }
    }

    /**
     * Returns a reasonable approximation of the main memory [bytes] consumed by
     * this instance. Useful for smart memory sensititive caches/pools. Assumes
     * fieldNames are interned, whereas tokenized terms are memory-overlaid.
     *
     * @return the main memory consumption
     */
    public int getMemorySize() {
        // for example usage in a smart cache see nux.xom.pool.Pool
        int PTR = VM.PTR;
        int INT = VM.INT;
        int size = 0;
        size += VM.sizeOfObject(2 * PTR + INT); // memory index
        if (sortedFields != null) size += VM.sizeOfObjectArray(sortedFields.length);

        size += VM.sizeOfHashMap(fields.size());
        for (Map.Entry<String, Info> entry : fields.entrySet()) { // for each Field Info
            Info info = entry.getValue();
            size += VM.sizeOfObject(2 * INT + 3 * PTR); // Info instance vars
            if (info.sortedTerms != null) size += VM.sizeOfObjectArray(info.sortedTerms.length);

            int len = info.terms.size();
            size += VM.sizeOfHashMap(len);
            Iterator<Map.Entry<BytesRef, ArrayIntList>> iter2 = info.terms.entrySet().iterator();
            while (--len >= 0) { // for each term
                Map.Entry<BytesRef, ArrayIntList> e = iter2.next();
                size += VM.sizeOfObject(PTR + 3 * INT); // assumes substring() memory overlay
//        size += STR + 2 * ((String) e.getKey()).length();
                ArrayIntList positions = e.getValue();
                size += VM.sizeOfArrayIntList(positions.size());
            }
        }
        return size;
    }

    private int numPositions(ArrayIntList positions) {
        return positions.size() / stride;
    }

    /** sorts into ascending order (on demand), reusing memory along the way */
    private void sortFields() {
        if (sortedFields == null) sortedFields = sort(fields);
    }

    /** returns a view of the given map's entries, sorted ascending by key */
    private static <K,V> Map.Entry<K,V>[] sort(HashMap<K,V> map) {
        int size = map.size();
        @SuppressWarnings("unchecked")
        Map.Entry<K,V>[] entries = new Map.Entry[size];

        Iterator<Map.Entry<K,V>> iter = map.entrySet().iterator();
        for (int i=0; i < size; i++) {
            entries[i] = iter.next();
        }

        if (size > 1) ArrayUtil.quickSort(entries, termComparator);
        return entries;
    }

    /**
     * Returns a String representation of the index data for debugging purposes.
     *
     * @return the string representation
     */
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder(256);
        sortFields();
        int sumChars = 0;
        int sumPositions = 0;
        int sumTerms = 0;

        for (int i=0; i < sortedFields.length; i++) {
            Map.Entry<String,Info> entry = sortedFields[i];
            String fieldName = entry.getKey();
            Info info = entry.getValue();
            info.sortTerms();
            result.append(fieldName + ":\n");

            int numChars = 0;
            int numPositions = 0;
            for (int j=0; j < info.sortedTerms.length; j++) {
                Map.Entry<BytesRef,ArrayIntList> e = info.sortedTerms[j];
                BytesRef term = e.getKey();
                ArrayIntList positions = e.getValue();
                result.append("\t'" + term + "':" + numPositions(positions) + ":");
                result.append(positions.toString(stride)); // ignore offsets
                result.append("\n");
                numPositions += numPositions(positions);
                numChars += term.length;
            }

            result.append("\tterms=" + info.sortedTerms.length);
            result.append(", positions=" + numPositions);
            result.append(", Kchars=" + (numChars / 1000.0f));
            result.append("\n");
            sumPositions += numPositions;
            sumChars += numChars;
            sumTerms += info.sortedTerms.length;
        }

        result.append("\nfields=" + sortedFields.length);
        result.append(", terms=" + sumTerms);
        result.append(", positions=" + sumPositions);
        result.append(", Kchars=" + (sumChars / 1000.0f));
        return result.toString();
    }

    /**
     * Index data structure for a field; Contains the tokenized term texts and
     * their positions.
     */
    private static final class Info {

        /**
         * Term strings and their positions for this field: Map <String
         * termText, ArrayIntList positions>
         */
        private final HashMap<BytesRef,ArrayIntList> terms;

        /** Terms sorted ascending by term text; computed on demand */
        private transient Map.Entry<BytesRef,ArrayIntList>[] sortedTerms;

        /** Number of added tokens for this field */
        private final int numTokens;

        /** Number of overlapping tokens for this field */
        private final int numOverlapTokens;

        /** Boost factor for hits for this field */
        private final float boost;

        private final int pos;

        private final long sumTotalTermFreq;

        public Info(HashMap<BytesRef, ArrayIntList> terms, int numTokens, int numOverlapTokens, float boost, int pos) {
            this.terms = terms;
            this.numTokens = numTokens;
            this.numOverlapTokens = numOverlapTokens;
            this.boost = boost;
            this.pos = pos;
            long sum = 0;
            for(Map.Entry<BytesRef,ArrayIntList> ent : terms.entrySet()) {
                sum += ent.getValue().size();
            }
            sumTotalTermFreq = sum;
        }

        public long getSumTotalTermFreq() {
            return sumTotalTermFreq;
        }

        /**
         * Sorts hashed terms into ascending order, reusing memory along the
         * way. Note that sorting is lazily delayed until required (often it's
         * not required at all). If a sorted view is required then hashing +
         * sort + binary search is still faster and smaller than TreeMap usage
         * (which would be an alternative and somewhat more elegant approach,
         * apart from more sophisticated Tries / prefix trees).
         */
        public void sortTerms() {
            if (sortedTerms == null) sortedTerms = sort(terms);
        }

        public float getBoost() {
            return boost;
        }

    }


    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////
    /**
     * Efficient resizable auto-expanding list holding <code>int</code> elements;
     * implemented with arrays.
     */
    private static final class ArrayIntList {

        private int[] elements;
        private int size = 0;

        private static final long serialVersionUID = 2282195016849084649L;

        public ArrayIntList() {
            this(10);
        }

        public ArrayIntList(int initialCapacity) {
            elements = new int[initialCapacity];
        }

        public void add(int elem) {
            if (size == elements.length) ensureCapacity(size + 1);
            elements[size++] = elem;
        }

        public void add(int pos, int start, int end) {
            if (size + 3 > elements.length) ensureCapacity(size + 3);
            elements[size] = pos;
            elements[size+1] = start;
            elements[size+2] = end;
            size += 3;
        }

        public int get(int index) {
            if (index >= size) throwIndex(index);
            return elements[index];
        }

        public int size() {
            return size;
        }

        private void ensureCapacity(int minCapacity) {
            int newCapacity = Math.max(minCapacity, (elements.length * 3) / 2 + 1);
            int[] newElements = new int[newCapacity];
            System.arraycopy(elements, 0, newElements, 0, size);
            elements = newElements;
        }

        private void throwIndex(int index) {
            throw new IndexOutOfBoundsException("index: " + index
                    + ", size: " + size);
        }

        /** returns the first few positions (without offsets); debug only */
        public String toString(int stride) {
            int s = size() / stride;
            int len = Math.min(10, s); // avoid printing huge lists
            StringBuilder buf = new StringBuilder(4*len);
            buf.append("[");
            for (int i = 0; i < len; i++) {
                buf.append(get(i*stride));
                if (i < len-1) buf.append(", ");
            }
            if (len != s) buf.append(", ..."); // and some more...
            buf.append("]");
            return buf.toString();
        }
    }


    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////

    /**
     * Search support for Lucene framework integration; implements all methods
     * required by the Lucene IndexReader contracts.
     */
    final class MemoryIndexReader extends AtomicReader {

        private IndexSearcher searcher; // needed to find searcher.getSimilarity()

        private MemoryIndexReader() {
            super(); // avoid as much superclass baggage as possible
        }

        private Info getInfo(String fieldName) {
            return fields.get(fieldName);
        }

        private Info getInfo(int pos) {
            return sortedFields[pos].getValue();
        }

        @Override
        public Bits getLiveDocs() {
            return null;
        }

        @Override
        public FieldInfos getFieldInfos() {
            return new FieldInfos(fieldInfos.values().toArray(new FieldInfo[fieldInfos.size()]));
        }

        private class MemoryFields extends Fields {
            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {
                    int upto = -1;

                    @Override
                    public String next() {
                        upto++;
                        if (upto >= sortedFields.length) {
                            throw new NoSuchElementException();
                        }
                        return sortedFields[upto].getKey();
                    }

                    @Override
                    public boolean hasNext() {
                        return upto+1 < sortedFields.length;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public Terms terms(final String field) {
                int i = Arrays.binarySearch(sortedFields, field, termComparator);
                if (i < 0) {
                    return null;
                } else {
                    final Info info = getInfo(i);
                    info.sortTerms();

                    return new Terms() {
                        @Override
                        public TermsEnum iterator(TermsEnum reuse) {
                            return new MemoryTermsEnum(info);
                        }

                        @Override
                        public Comparator<BytesRef> getComparator() {
                            return BytesRef.getUTF8SortedAsUnicodeComparator();
                        }

                        @Override
                        public long size() {
                            return info.sortedTerms.length;
                        }

                        @Override
                        public long getSumTotalTermFreq() {
                            return info.getSumTotalTermFreq();
                        }

                        @Override
                        public long getSumDocFreq() {
                            // each term has df=1
                            return info.sortedTerms.length;
                        }

                        @Override
                        public int getDocCount() {
                            return info.sortedTerms.length > 0 ? 1 : 0;
                        }

                        @Override
                        public boolean hasOffsets() {
                            return stride == 3;
                        }

                        @Override
                        public boolean hasPositions() {
                            return true;
                        }

                        @Override
                        public boolean hasPayloads() {
                            return false;
                        }
                    };
                }
            }

            @Override
            public int size() {
                return sortedFields.length;
            }
        }

        @Override
        public Fields fields() {
            sortFields();
            return new MemoryFields();
        }

        private class MemoryTermsEnum extends TermsEnum {
            private final Info info;
            private final BytesRef br = new BytesRef();
            int termUpto = -1;

            public MemoryTermsEnum(Info info) {
                this.info = info;
                info.sortTerms();
            }

            @Override
            public boolean seekExact(BytesRef text, boolean useCache) {
                termUpto = Arrays.binarySearch(info.sortedTerms, text, termComparator);
                if (termUpto >= 0) {
                    br.copyBytes(info.sortedTerms[termUpto].getKey());
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            public SeekStatus seekCeil(BytesRef text, boolean useCache) {
                termUpto = Arrays.binarySearch(info.sortedTerms, text, termComparator);
                if (termUpto < 0) { // not found; choose successor
                    termUpto = -termUpto -1;
                    if (termUpto >= info.sortedTerms.length) {
                        return SeekStatus.END;
                    } else {
                        br.copyBytes(info.sortedTerms[termUpto].getKey());
                        return SeekStatus.NOT_FOUND;
                    }
                } else {
                    br.copyBytes(info.sortedTerms[termUpto].getKey());
                    return SeekStatus.FOUND;
                }
            }

            @Override
            public void seekExact(long ord) {
                assert ord < info.sortedTerms.length;
                termUpto = (int) ord;
            }

            @Override
            public BytesRef next() {
                termUpto++;
                if (termUpto >= info.sortedTerms.length) {
                    return null;
                } else {
                    br.copyBytes(info.sortedTerms[termUpto].getKey());
                    return br;
                }
            }

            @Override
            public BytesRef term() {
                return br;
            }

            @Override
            public long ord() {
                return termUpto;
            }

            @Override
            public int docFreq() {
                return 1;
            }

            @Override
            public long totalTermFreq() {
                return info.sortedTerms[termUpto].getValue().size();
            }

            @Override
            public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) {
                if (reuse == null || !(reuse instanceof MemoryDocsEnum)) {
                    reuse = new MemoryDocsEnum();
                }
                return ((MemoryDocsEnum) reuse).reset(liveDocs, info.sortedTerms[termUpto].getValue());
            }

            @Override
            public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) {
                if (reuse == null || !(reuse instanceof MemoryDocsAndPositionsEnum)) {
                    reuse = new MemoryDocsAndPositionsEnum();
                }
                return ((MemoryDocsAndPositionsEnum) reuse).reset(liveDocs, info.sortedTerms[termUpto].getValue());
            }

            @Override
            public Comparator<BytesRef> getComparator() {
                return BytesRef.getUTF8SortedAsUnicodeComparator();
            }

            @Override
            public void seekExact(BytesRef term, TermState state) throws IOException {
                assert state != null;
                this.seekExact(((OrdTermState)state).ord);
            }

            @Override
            public TermState termState() throws IOException {
                OrdTermState ts = new OrdTermState();
                ts.ord = termUpto;
                return ts;
            }
        }

        private class MemoryDocsEnum extends DocsEnum {
            private ArrayIntList positions;
            private boolean hasNext;
            private Bits liveDocs;
            private int doc = -1;

            public DocsEnum reset(Bits liveDocs, ArrayIntList positions) {
                this.liveDocs = liveDocs;
                this.positions = positions;
                hasNext = true;
                doc = -1;
                return this;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                if (hasNext && (liveDocs == null || liveDocs.get(0))) {
                    hasNext = false;
                    return doc = 0;
                } else {
                    return doc = NO_MORE_DOCS;
                }
            }

            @Override
            public int advance(int target) {
                return nextDoc();
            }

            @Override
            public int freq() throws IOException {
                return positions.size();
            }
        }

        private class MemoryDocsAndPositionsEnum extends DocsAndPositionsEnum {
            private ArrayIntList positions;
            private int posUpto;
            private boolean hasNext;
            private Bits liveDocs;
            private int doc = -1;

            public DocsAndPositionsEnum reset(Bits liveDocs, ArrayIntList positions) {
                this.liveDocs = liveDocs;
                this.positions = positions;
                posUpto = 0;
                hasNext = true;
                doc = -1;
                return this;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                if (hasNext && (liveDocs == null || liveDocs.get(0))) {
                    hasNext = false;
                    return doc = 0;
                } else {
                    return doc = NO_MORE_DOCS;
                }
            }

            @Override
            public int advance(int target) {
                return nextDoc();
            }

            @Override
            public int freq() throws IOException {
                return positions.size() / stride;
            }

            @Override
            public int nextPosition() {
                return positions.get(posUpto++ * stride);
            }

            @Override
            public int startOffset() {
                return stride == 1 ? -1 : positions.get((posUpto - 1) * stride + 1);
            }

            @Override
            public int endOffset() {
                return stride == 1 ? -1 : positions.get((posUpto - 1) * stride + 2);
            }

            @Override
            public BytesRef getPayload() {
                return null;
            }
        }

        @Override
        public Fields getTermVectors(int docID) {
            if (docID == 0) {
                return fields();
            } else {
                return null;
            }
        }

        private Similarity getSimilarity() {
            if (searcher != null) return searcher.getSimilarity();
            return IndexSearcher.getDefaultSimilarity();
        }

        private void setSearcher(IndexSearcher searcher) {
            this.searcher = searcher;
        }

        @Override
        public int numDocs() {
            if (DEBUG) System.err.println("MemoryIndexReader.numDocs");
            return fields.size() > 0 ? 1 : 0;
        }

        @Override
        public int maxDoc() {
            if (DEBUG) System.err.println("MemoryIndexReader.maxDoc");
            return 1;
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) {
            if (DEBUG) System.err.println("MemoryIndexReader.document");
            // no-op: there are no stored fields
        }

        @Override
        public boolean hasDeletions() {
            if (DEBUG) System.err.println("MemoryIndexReader.hasDeletions");
            return false;
        }

        @Override
        protected void doClose() {
            if (DEBUG) System.err.println("MemoryIndexReader.doClose");
        }

        @Override
        public DocValues docValues(String field) {
            return null;
        }

        /** performance hack: cache norms to avoid repeated expensive calculations */
        private DocValues cachedNormValues;
        private String cachedFieldName;
        private Similarity cachedSimilarity;

        @Override
        public DocValues normValues(String field) {
            DocValues norms = cachedNormValues;
            Similarity sim = getSimilarity();
            if (!field.equals(cachedFieldName) || sim != cachedSimilarity) { // not cached?
                Info info = getInfo(field);
                int numTokens = info != null ? info.numTokens : 0;
                int numOverlapTokens = info != null ? info.numOverlapTokens : 0;
                float boost = info != null ? info.getBoost() : 1.0f;
                FieldInvertState invertState = new FieldInvertState(field, 0, numTokens, numOverlapTokens, 0, boost);
                Norm norm = new Norm();
                sim.computeNorm(invertState, norm);
                SingleValueSource singleByteSource = new SingleValueSource(norm);
                norms = new MemoryIndexNormDocValues(singleByteSource);
                // cache it for future reuse
                cachedNormValues = norms;
                cachedFieldName = field;
                cachedSimilarity = sim;
                if (DEBUG) System.err.println("MemoryIndexReader.norms: " + field + ":" + norm + ":" + numTokens);
            }
            return norms;
        }
    }

    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////
    private static final class VM {

        public static final int PTR = Constants.JRE_IS_64BIT ? 8 : 4;

        // bytes occupied by primitive data types
        public static final int BOOLEAN = 1;
        public static final int BYTE = 1;
        public static final int CHAR = 2;
        public static final int SHORT = 2;
        public static final int INT = 4;
        public static final int LONG = 8;
        public static final int FLOAT = 4;
        public static final int DOUBLE = 8;

        private static final int LOG_PTR = (int) Math.round(log2(PTR));

        /**
         * Object header of any heap allocated Java object.
         * ptr to class, info for monitor, gc, hash, etc.
         */
        private static final int OBJECT_HEADER = 2 * PTR;

        private VM() {
        } // not instantiable

        //  assumes n > 0
        //  64 bit VM:
        //    0     --> 0*PTR
        //    1..8  --> 1*PTR
        //    9..16 --> 2*PTR
        private static int sizeOf(int n) {
            return (((n - 1) >> LOG_PTR) + 1) << LOG_PTR;
        }

        public static int sizeOfObject(int n) {
            return sizeOf(OBJECT_HEADER + n);
        }

        public static int sizeOfObjectArray(int len) {
            return sizeOfObject(INT + PTR * len);
        }

        public static int sizeOfCharArray(int len) {
            return sizeOfObject(INT + CHAR * len);
        }

        public static int sizeOfIntArray(int len) {
            return sizeOfObject(INT + INT * len);
        }

        public static int sizeOfString(int len) {
            return sizeOfObject(3 * INT + PTR) + sizeOfCharArray(len);
        }

        public static int sizeOfHashMap(int len) {
            return sizeOfObject(4 * PTR + 4 * INT) + sizeOfObjectArray(len)
                    + len * sizeOfObject(3 * PTR + INT); // entries
        }

        // note: does not include referenced objects
        public static int sizeOfArrayList(int len) {
            return sizeOfObject(PTR + 2 * INT) + sizeOfObjectArray(len);
        }

        public static int sizeOfArrayIntList(int len) {
            return sizeOfObject(PTR + INT) + sizeOfIntArray(len);
        }

        /**
         * logarithm to the base 2. Example: log2(4) == 2, log2(8) == 3
         */
        private static double log2(double value) {
            return Math.log(value) / Math.log(2);
        }

    }

}
