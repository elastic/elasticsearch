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
package org.apache.lucene.index.memory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.*;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;
import org.apache.lucene.util.IntBlockPool.SliceReader;
import org.apache.lucene.util.IntBlockPool.SliceWriter;

// FORK from Lucene's MemoryIndex from master branch
// Changes:
// 1) Renamed to ForkedMemoryIndex (to avoid JAR HELL)
// 2) Removed any reference to point values, because doesn't exist in Lucene 5.x
// 3) Integer.BYTES -> RamUsageEstimator.NUM_BYTES_INT
// 4) MemoryIndexReader#fields() to use pre Java 8 of filtering out fields that are only doc values
// 5) Made info variable in MemoryIndexReader#getSortedSetValues() final
// 6) Changed license header
// 7) Made ForkedMemoryIndex(boolean, boolean, long) constructor public
// 8) Pass BytesRef.getUTF8SortedAsUnicodeComparator() to BytesRefHash#sort(...)
// 9) Use explicit generics in MemoryIndex#addField(...) attributes empty map

/**
 * High-performance single-document main memory Apache Lucene fulltext search index.
 * <p>
 * <b>Overview</b>
 * <p>
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
 * <p>
 * <b>Example Usage</b>
 * <br>
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
 * <p>
 * <b>Example XQuery Usage</b>
 *
 * <pre class="prettyprint">
 * (: An XQuery that finds all books authored by James that have something to do with "salmon fishing manuals", sorted by relevance :)
 * declare namespace lucene = "java:nux.xom.pool.FullTextUtil";
 * declare variable $query := "+salmon~ +fish* manual~"; (: any arbitrary Lucene query can go here :)
 *
 * for $book in /books/book[author="James" and lucene:match(abstract, $query) &gt; 0.0]
 * let $score := lucene:match($book/abstract, $query)
 * order by $score descending
 * return $book
 * </pre>
 *
 * <p>
 * <b>Thread safety guarantees</b>
 * <p>
 * MemoryIndex is not normally thread-safe for adds or queries.  However, queries
 * are thread-safe after {@code freeze()} has been called.
 *
 * <p>
 * <b>Performance Notes</b>
 * <p>
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
public class ForkedMemoryIndex {

    private static final boolean DEBUG = false;

    /** info for each field: Map&lt;String fieldName, Info field&gt; */
    private final SortedMap<String,Info> fields = new TreeMap<>();

    private final boolean storeOffsets;
    private final boolean storePayloads;

    private final ByteBlockPool byteBlockPool;
    private final IntBlockPool intBlockPool;
    //  private final IntBlockPool.SliceReader postingsReader;
    private final IntBlockPool.SliceWriter postingsWriter;
    private final BytesRefArray payloadsBytesRefs;//non null only when storePayloads

    private Counter bytesUsed;

    private boolean frozen = false;

    private Similarity normSimilarity = IndexSearcher.getDefaultSimilarity();

    /**
     * Constructs an empty instance that will not store offsets or payloads.
     */
    public ForkedMemoryIndex() {
        this(false);
    }

    /**
     * Constructs an empty instance that can optionally store the start and end
     * character offset of each token term in the text. This can be useful for
     * highlighting of hit locations with the Lucene highlighter package.  But
     * it will not store payloads; use another constructor for that.
     *
     * @param storeOffsets
     *            whether or not to store the start and end character offset of
     *            each token term in the text
     */
    public ForkedMemoryIndex(boolean storeOffsets) {
        this(storeOffsets, false);
    }

    /**
     * Constructs an empty instance with the option of storing offsets and payloads.
     *
     * @param storeOffsets store term offsets at each position
     * @param storePayloads store term payloads at each position
     */
    public ForkedMemoryIndex(boolean storeOffsets, boolean storePayloads) {
        this(storeOffsets, storePayloads, 0);
    }

    /**
     * Expert: This constructor accepts an upper limit for the number of bytes that should be reused if this instance is {@link #reset()}.
     * The payload storage, if used, is unaffected by maxReusuedBytes, however.
     * @param storeOffsets <code>true</code> if offsets should be stored
     * @param storePayloads <code>true</code> if payloads should be stored
     * @param maxReusedBytes the number of bytes that should remain in the internal memory pools after {@link #reset()} is called
     */
    public ForkedMemoryIndex(boolean storeOffsets, boolean storePayloads, long maxReusedBytes) {
        this.storeOffsets = storeOffsets;
        this.storePayloads = storePayloads;
        this.bytesUsed = Counter.newCounter();
        final int maxBufferedByteBlocks = (int)((maxReusedBytes/2) / ByteBlockPool.BYTE_BLOCK_SIZE );
        final int maxBufferedIntBlocks = (int) ((maxReusedBytes - (maxBufferedByteBlocks*ByteBlockPool.BYTE_BLOCK_SIZE))/(IntBlockPool.INT_BLOCK_SIZE * RamUsageEstimator.NUM_BYTES_INT));
        assert (maxBufferedByteBlocks * ByteBlockPool.BYTE_BLOCK_SIZE) + (maxBufferedIntBlocks * IntBlockPool.INT_BLOCK_SIZE * RamUsageEstimator.NUM_BYTES_INT) <= maxReusedBytes;
        byteBlockPool = new ByteBlockPool(new RecyclingByteBlockAllocator(ByteBlockPool.BYTE_BLOCK_SIZE, maxBufferedByteBlocks, bytesUsed));
        intBlockPool = new IntBlockPool(new RecyclingIntBlockAllocator(IntBlockPool.INT_BLOCK_SIZE, maxBufferedIntBlocks, bytesUsed));
        postingsWriter = new SliceWriter(intBlockPool);
        //TODO refactor BytesRefArray to allow us to apply maxReusedBytes option
        payloadsBytesRefs = storePayloads ? new BytesRefArray(bytesUsed) : null;
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

        TokenStream stream = analyzer.tokenStream(fieldName, text);
        addField(fieldName, stream, 1.0f, analyzer.getPositionIncrementGap(fieldName), analyzer.getOffsetGap(fieldName), DocValuesType.NONE, null);
    }

    /**
     * Builds a MemoryIndex from a lucene {@link Document} using an analyzer
     *
     * @param document the document to index
     * @param analyzer the analyzer to use
     * @return a MemoryIndex
     */
    public static MemoryIndex fromDocument(Iterable<? extends IndexableField> document, Analyzer analyzer) {
        return fromDocument(document, analyzer, false, false, 0);
    }

    /**
     * Builds a MemoryIndex from a lucene {@link Document} using an analyzer
     * @param document the document to index
     * @param analyzer the analyzer to use
     * @param storeOffsets <code>true</code> if offsets should be stored
     * @param storePayloads <code>true</code> if payloads should be stored
     * @return a MemoryIndex
     */
    public static MemoryIndex fromDocument(Iterable<? extends IndexableField> document, Analyzer analyzer, boolean storeOffsets, boolean storePayloads) {
        return fromDocument(document, analyzer, storeOffsets, storePayloads, 0);
    }

    /**
     * Builds a MemoryIndex from a lucene {@link Document} using an analyzer
     * @param document the document to index
     * @param analyzer the analyzer to use
     * @param storeOffsets <code>true</code> if offsets should be stored
     * @param storePayloads <code>true</code> if payloads should be stored
     * @param maxReusedBytes the number of bytes that should remain in the internal memory pools after {@link #reset()} is called
     * @return a MemoryIndex
     */
    public static MemoryIndex fromDocument(Iterable<? extends IndexableField> document, Analyzer analyzer, boolean storeOffsets, boolean storePayloads, long maxReusedBytes) {
        MemoryIndex mi = new MemoryIndex(storeOffsets, storePayloads, maxReusedBytes);
        for (IndexableField field : document) {
            mi.addField(field, analyzer);
        }
        return mi;
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
     * Adds a lucene {@link IndexableField} to the MemoryIndex using the provided analyzer.
     * Also stores doc values based on {@link IndexableFieldType#docValuesType()} if set.
     *
     * @param field the field to add
     * @param analyzer the analyzer to use for term analysis
     * @throws IllegalArgumentException if the field is a DocValues or Point field, as these
     *                                  structures are not supported by MemoryIndex
     */
    public void addField(IndexableField field, Analyzer analyzer) {
        addField(field, analyzer, 1.0f);
    }

    /**
     * Adds a lucene {@link IndexableField} to the MemoryIndex using the provided analyzer.
     * Also stores doc values based on {@link IndexableFieldType#docValuesType()} if set.
     *
     * @param field the field to add
     * @param analyzer the analyzer to use for term analysis
     * @param boost a field boost
     * @throws IllegalArgumentException if the field is a DocValues or Point field, as these
     *                                  structures are not supported by MemoryIndex
     */
    public void addField(IndexableField field, Analyzer analyzer, float boost) {
        int offsetGap;
        TokenStream tokenStream;
        int positionIncrementGap;
        if (analyzer != null) {
            offsetGap = analyzer.getOffsetGap(field.name());
            tokenStream = field.tokenStream(analyzer, null);
            positionIncrementGap = analyzer.getPositionIncrementGap(field.name());
        } else {
            offsetGap = 1;
            tokenStream = field.tokenStream(null, null);
            positionIncrementGap = 0;
        }

        DocValuesType docValuesType = field.fieldType().docValuesType();
        Object docValuesValue;
        switch (docValuesType) {
            case NONE:
                docValuesValue = null;
                break;
            case BINARY:
            case SORTED:
            case SORTED_SET:
                docValuesValue = field.binaryValue();
                break;
            case NUMERIC:
            case SORTED_NUMERIC:
                docValuesValue = field.numericValue();
                break;
            default:
                throw new UnsupportedOperationException("unknown doc values type [" + docValuesType + "]");
        }
        addField(field.name(), tokenStream, boost, positionIncrementGap, offsetGap, docValuesType, docValuesValue);
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
     *
     * @see org.apache.lucene.document.Field#setBoost(float)
     */
    public void addField(String fieldName, TokenStream stream, float boost) {
        addField(fieldName, stream, boost, 0);
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
     *
     * @param positionIncrementGap
     *            the position increment gap if fields with the same name are added more than once
     *
     *
     * @see org.apache.lucene.document.Field#setBoost(float)
     */
    public void addField(String fieldName, TokenStream stream, float boost, int positionIncrementGap) {
        addField(fieldName, stream, boost, positionIncrementGap, 1);
    }

    /**
     * Iterates over the given token stream and adds the resulting terms to the index;
     * Equivalent to adding a tokenized, indexed, termVectorStored, unstored,
     * Lucene {@link org.apache.lucene.document.Field}.
     * Finally closes the token stream. Note that untokenized keywords can be added with this method via
     * {@link #keywordTokenStream(Collection)}, the Lucene <code>KeywordTokenizer</code> or similar utilities.
     *
     *
     * @param fieldName
     *            a name to be associated with the text
     * @param tokenStream
     *            the token stream to retrieve tokens from. It's guaranteed to be closed no matter what.
     * @param boost
     *            the boost factor for hits for this field
     * @param positionIncrementGap
     *            the position increment gap if fields with the same name are added more than once
     * @param offsetGap
     *            the offset gap if fields with the same name are added more than once
     * @see org.apache.lucene.document.Field#setBoost(float)
     */
    public void addField(String fieldName, TokenStream tokenStream, float boost, int positionIncrementGap, int offsetGap) {
        addField(fieldName, tokenStream, boost, positionIncrementGap, offsetGap, DocValuesType.NONE, null);
    }

    private void addField(String fieldName, TokenStream tokenStream, float boost, int positionIncrementGap, int offsetGap,
                          DocValuesType docValuesType, Object docValuesValue) {

        if (frozen) {
            throw new IllegalArgumentException("Cannot call addField() when MemoryIndex is frozen");
        }
        if (fieldName == null) {
            throw new IllegalArgumentException("fieldName must not be null");
        }
        if (boost <= 0.0f) {
            throw new IllegalArgumentException("boost factor must be greater than 0.0");
        }

        Info info = fields.get(fieldName);
        if (info == null) {
            IndexOptions indexOptions = storeOffsets ? IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS : IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
            FieldInfo fieldInfo = new FieldInfo(fieldName, fields.size(), true, false, storePayloads, indexOptions, docValuesType, -1, Collections.<String, String>emptyMap());
            fields.put(fieldName, info = new Info(fieldInfo, byteBlockPool));
        }

        if (docValuesType != DocValuesType.NONE) {
            storeDocValues(info, docValuesType, docValuesValue);
        }
        if (tokenStream != null) {
            storeTerms(info, tokenStream, boost, positionIncrementGap, offsetGap);
        }
    }

    private void storeDocValues(Info info, DocValuesType docValuesType, Object docValuesValue) {
        String fieldName = info.fieldInfo.name;
        DocValuesType existingDocValuesType = info.fieldInfo.getDocValuesType();
        if (existingDocValuesType == DocValuesType.NONE) {
            // first time we add doc values for this field:
            info.fieldInfo = new FieldInfo(
                    info.fieldInfo.name, info.fieldInfo.number, info.fieldInfo.hasVectors(), info.fieldInfo.hasPayloads(),
                    info.fieldInfo.hasPayloads(), info.fieldInfo.getIndexOptions(), docValuesType, -1, info.fieldInfo.attributes()
            );
        } else if (existingDocValuesType != docValuesType) {
            throw new IllegalArgumentException("Can't add [" + docValuesType + "] doc values field [" + fieldName + "], because [" + existingDocValuesType + "] doc values field already exists");
        }
        switch (docValuesType) {
            case NUMERIC:
                if (info.numericProducer.dvLongValues != null) {
                    throw new IllegalArgumentException("Only one value per field allowed for [" + docValuesType + "] doc values field [" + fieldName + "]");
                }
                info.numericProducer.dvLongValues = new long[]{(long) docValuesValue};
                info.numericProducer.count++;
                break;
            case SORTED_NUMERIC:
                if (info.numericProducer.dvLongValues == null) {
                    info.numericProducer.dvLongValues = new long[4];
                }
                info.numericProducer.dvLongValues = ArrayUtil.grow(info.numericProducer.dvLongValues, info.numericProducer.count + 1);
                info.numericProducer.dvLongValues[info.numericProducer.count++] = (long) docValuesValue;
                break;
            case BINARY:
                if (info.binaryProducer.dvBytesValuesSet != null) {
                    throw new IllegalArgumentException("Only one value per field allowed for [" + docValuesType + "] doc values field [" + fieldName + "]");
                }
                info.binaryProducer.dvBytesValuesSet = new BytesRefHash(byteBlockPool);
                info.binaryProducer.dvBytesValuesSet.add((BytesRef) docValuesValue);
                break;
            case SORTED:
                if (info.binaryProducer.dvBytesValuesSet != null) {
                    throw new IllegalArgumentException("Only one value per field allowed for [" + docValuesType + "] doc values field [" + fieldName + "]");
                }
                info.binaryProducer.dvBytesValuesSet = new BytesRefHash(byteBlockPool);
                info.binaryProducer.dvBytesValuesSet.add((BytesRef) docValuesValue);
                break;
            case SORTED_SET:
                if (info.binaryProducer.dvBytesValuesSet == null) {
                    info.binaryProducer.dvBytesValuesSet = new BytesRefHash(byteBlockPool);
                }
                info.binaryProducer.dvBytesValuesSet.add((BytesRef) docValuesValue);
                break;
            default:
                throw new UnsupportedOperationException("unknown doc values type [" + docValuesType + "]");
        }
    }

    private void storeTerms(Info info, TokenStream tokenStream, float boost, int positionIncrementGap, int offsetGap) {
        int pos = -1;
        int offset = 0;
        if (info.numTokens == 0) {
            info.boost = boost;
        } else if (info.numTokens > 0) {
            pos = info.lastPosition + positionIncrementGap;
            offset = info.lastOffset + offsetGap;
            info.boost *= boost;
        }

        try (TokenStream stream = tokenStream) {
            TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
            PositionIncrementAttribute posIncrAttribute = stream.addAttribute(PositionIncrementAttribute.class);
            OffsetAttribute offsetAtt = stream.addAttribute(OffsetAttribute.class);
            PayloadAttribute payloadAtt = storePayloads ? stream.addAttribute(PayloadAttribute.class) : null;
            stream.reset();

            while (stream.incrementToken()) {
//        if (DEBUG) System.err.println("token='" + term + "'");
                info.numTokens++;
                final int posIncr = posIncrAttribute.getPositionIncrement();
                if (posIncr == 0) {
                    info.numOverlapTokens++;
                }
                pos += posIncr;
                int ord = info.terms.add(termAtt.getBytesRef());
                if (ord < 0) {
                    ord = (-ord) - 1;
                    postingsWriter.reset(info.sliceArray.end[ord]);
                } else {
                    info.sliceArray.start[ord] = postingsWriter.startNewSlice();
                }
                info.sliceArray.freq[ord]++;
                info.sumTotalTermFreq++;
                postingsWriter.writeInt(pos);
                if (storeOffsets) {
                    postingsWriter.writeInt(offsetAtt.startOffset() + offset);
                    postingsWriter.writeInt(offsetAtt.endOffset() + offset);
                }
                if (storePayloads) {
                    final BytesRef payload = payloadAtt.getPayload();
                    final int pIndex;
                    if (payload == null || payload.length == 0) {
                        pIndex = -1;
                    } else {
                        pIndex = payloadsBytesRefs.append(payload);
                    }
                    postingsWriter.writeInt(pIndex);
                }
                info.sliceArray.end[ord] = postingsWriter.getCurrentOffset();
            }
            stream.end();
            if (info.numTokens > 0) {
                info.lastPosition = pos;
                info.lastOffset = offsetAtt.endOffset() + offset;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Set the Similarity to be used for calculating field norms
     */
    public void setSimilarity(Similarity similarity) {
        if (frozen)
            throw new IllegalArgumentException("Cannot set Similarity when MemoryIndex is frozen");
        if (this.normSimilarity == similarity)
            return;
        this.normSimilarity = similarity;
        //invalidate any cached norms that may exist
        for (Info info : fields.values()) {
            info.norms = null;
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
        searcher.setSimilarity(normSimilarity);
        return searcher;
    }

    /**
     * Prepares the MemoryIndex for querying in a non-lazy way.
     * <p>
     * After calling this you can query the MemoryIndex from multiple threads, but you
     * cannot subsequently add new data.
     */
    public void freeze() {
        this.frozen = true;
        for (Info info : fields.values()) {
            info.freeze();
        }
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
            searcher.search(query, new SimpleCollector() {
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
                public boolean needsScores() {
                    return true;
                }
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
       * memory index data structure.
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
     * Returns a String representation of the index data for debugging purposes.
     *
     * @return the string representation
     */
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder(256);
        int sumPositions = 0;
        int sumTerms = 0;
        final BytesRef spare = new BytesRef();
        for (Map.Entry<String, Info> entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            Info info = entry.getValue();
            info.sortTerms();
            result.append(fieldName + ":\n");
            SliceByteStartArray sliceArray = info.sliceArray;
            int numPositions = 0;
            SliceReader postingsReader = new SliceReader(intBlockPool);
            for (int j = 0; j < info.terms.size(); j++) {
                int ord = info.sortedTerms[j];
                info.terms.get(ord, spare);
                int freq = sliceArray.freq[ord];
                result.append("\t'" + spare + "':" + freq + ":");
                postingsReader.reset(sliceArray.start[ord], sliceArray.end[ord]);
                result.append(" [");
                final int iters = storeOffsets ? 3 : 1;
                while (!postingsReader.endOfSlice()) {
                    result.append("(");

                    for (int k = 0; k < iters; k++) {
                        result.append(postingsReader.readInt());
                        if (k < iters - 1) {
                            result.append(", ");
                        }
                    }
                    result.append(")");
                    if (!postingsReader.endOfSlice()) {
                        result.append(",");
                    }

                }
                result.append("]");
                result.append("\n");
                numPositions += freq;
            }

            result.append("\tterms=" + info.terms.size());
            result.append(", positions=" + numPositions);
            result.append("\n");
            sumPositions += numPositions;
            sumTerms += info.terms.size();
        }

        result.append("\nfields=" + fields.size());
        result.append(", terms=" + sumTerms);
        result.append(", positions=" + sumPositions);
        return result.toString();
    }

    /**
     * Index data structure for a field; contains the tokenized term texts and
     * their positions.
     */
    private final class Info {

        private FieldInfo fieldInfo;

        /** The norms for this field; computed on demand. */
        private transient NumericDocValues norms;

        /**
         * Term strings and their positions for this field: Map &lt;String
         * termText, ArrayIntList positions&gt;
         */
        private BytesRefHash terms; // note unfortunate variable name class with Terms type

        private SliceByteStartArray sliceArray;

        /** Terms sorted ascending by term text; computed on demand */
        private transient int[] sortedTerms;

        /** Number of added tokens for this field */
        private int numTokens;

        /** Number of overlapping tokens for this field */
        private int numOverlapTokens;

        /** Boost factor for hits for this field */
        private float boost;

        private long sumTotalTermFreq;

        /** the last position encountered in this field for multi field support*/
        private int lastPosition;

        /** the last offset encountered in this field for multi field support*/
        private int lastOffset;

        private BinaryDocValuesProducer binaryProducer;

        private NumericDocValuesProducer numericProducer;

        private boolean preparedDocValues;

        private Info(FieldInfo fieldInfo, ByteBlockPool byteBlockPool) {
            this.fieldInfo = fieldInfo;
            this.sliceArray = new SliceByteStartArray(BytesRefHash.DEFAULT_CAPACITY);
            this.terms = new BytesRefHash(byteBlockPool, BytesRefHash.DEFAULT_CAPACITY, sliceArray);;
            this.binaryProducer = new BinaryDocValuesProducer();
            this.numericProducer = new NumericDocValuesProducer();
        }

        void freeze() {
            sortTerms();
            prepareDocValues();
            getNormDocValues();
        }

        /**
         * Sorts hashed terms into ascending order, reusing memory along the
         * way. Note that sorting is lazily delayed until required (often it's
         * not required at all). If a sorted view is required then hashing +
         * sort + binary search is still faster and smaller than TreeMap usage
         * (which would be an alternative and somewhat more elegant approach,
         * apart from more sophisticated Tries / prefix trees).
         */
        void sortTerms() {
            if (sortedTerms == null) {
                sortedTerms = terms.sort(BytesRef.getUTF8SortedAsUnicodeComparator());
            }
        }

        void prepareDocValues() {
            if (preparedDocValues == false) {
                DocValuesType dvType = fieldInfo.getDocValuesType();
                if (dvType == DocValuesType.NUMERIC || dvType == DocValuesType.SORTED_NUMERIC) {
                    numericProducer.prepareForUsage();
                }
                if (dvType == DocValuesType.BINARY || dvType == DocValuesType.SORTED || dvType == DocValuesType.SORTED_SET) {
                    binaryProducer.prepareForUsage();
                }
                preparedDocValues = true;
            }
        }

        NumericDocValues getNormDocValues() {
            if (norms == null) {
                FieldInvertState invertState = new FieldInvertState(fieldInfo.name, fieldInfo.number,
                        numTokens, numOverlapTokens, 0, boost);
                final long value = normSimilarity.computeNorm(invertState);
                if (DEBUG) System.err.println("MemoryIndexReader.norms: " + fieldInfo.name + ":" + value + ":" + numTokens);
                norms = new NumericDocValues() {

                    @Override
                    public long get(int docID) {
                        if (docID != 0)
                            throw new IndexOutOfBoundsException();
                        else
                            return value;
                    }

                };
            }
            return norms;
        }
    }

    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////

    private static final class BinaryDocValuesProducer {

        BytesRefHash dvBytesValuesSet;
        final SortedDocValues sortedDocValues;
        final BytesRef spare = new BytesRef();

        int[] bytesIds;

        private BinaryDocValuesProducer() {
            sortedDocValues = new SortedDocValues() {
                @Override
                public int getOrd(int docID) {
                    return 0;
                }

                @Override
                public BytesRef lookupOrd(int ord) {
                    return getValue(ord);
                }

                @Override
                public int getValueCount() {
                    return 1;
                }
            };
        }

        private void prepareForUsage() {
            bytesIds = dvBytesValuesSet.sort(BytesRef.getUTF8SortedAsUnicodeComparator());
        }

        private BytesRef getValue(int index) {
            return dvBytesValuesSet.get(bytesIds[index], spare);
        }

    }

    private static final class NumericDocValuesProducer {

        long[] dvLongValues;
        int count;

        final NumericDocValues numericDocValues;
        final SortedNumericDocValues sortedNumericDocValues;

        private NumericDocValuesProducer() {
            this.numericDocValues = new NumericDocValues() {
                @Override
                public long get(int docID) {
                    return dvLongValues[0];
                }
            };
            this.sortedNumericDocValues = new SortedNumericDocValues() {
                @Override
                public void setDocument(int doc) {
                }

                @Override
                public long valueAt(int index) {
                    return dvLongValues[index];
                }

                @Override
                public int count() {
                    return count;
                }
            };
        }

        private void prepareForUsage() {
            Arrays.sort(dvLongValues, 0, count);
        }
    }

    /**
     * Search support for Lucene framework integration; implements all methods
     * required by the Lucene IndexReader contracts.
     */
    private final class MemoryIndexReader extends LeafReader {

        private MemoryIndexReader() {
            super(); // avoid as much superclass baggage as possible
            for (Info info : fields.values()) {
                info.prepareDocValues();
            }
        }

        @Override
        public void addCoreClosedListener(CoreClosedListener listener) {
            addCoreClosedListenerAsReaderClosedListener(this, listener);
        }

        @Override
        public void removeCoreClosedListener(CoreClosedListener listener) {
            removeCoreClosedListenerAsReaderClosedListener(this, listener);
        }

        private Info getInfoForExpectedDocValuesType(String fieldName, DocValuesType expectedType) {
            if (expectedType == DocValuesType.NONE) {
                return null;
            }
            Info info = fields.get(fieldName);
            if (info == null) {
                return null;
            }
            if (info.fieldInfo.getDocValuesType() != expectedType) {
                return null;
            }
            return info;
        }

        @Override
        public Bits getLiveDocs() {
            return null;
        }

        @Override
        public FieldInfos getFieldInfos() {
            FieldInfo[] fieldInfos = new FieldInfo[fields.size()];
            int i = 0;
            for (Info info : fields.values()) {
                fieldInfos[i++] = info.fieldInfo;
            }
            return new FieldInfos(fieldInfos);
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) {
            Info info = getInfoForExpectedDocValuesType(field, DocValuesType.NUMERIC);
            if (info != null) {
                return info.numericProducer.numericDocValues;
            } else {
                return null;
            }
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) {
            return getSortedDocValues(field, DocValuesType.BINARY);
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) {
            return getSortedDocValues(field, DocValuesType.SORTED);
        }

        private SortedDocValues getSortedDocValues(String field, DocValuesType docValuesType) {
            Info info = getInfoForExpectedDocValuesType(field, docValuesType);
            if (info != null) {
                return info.binaryProducer.sortedDocValues;
            } else {
                return null;
            }
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) {
            Info info = getInfoForExpectedDocValuesType(field, DocValuesType.SORTED_NUMERIC);
            if (info != null) {
                return info.numericProducer.sortedNumericDocValues;
            } else {
                return null;
            }
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) {
            final Info info = getInfoForExpectedDocValuesType(field, DocValuesType.SORTED_SET);
            if (info != null) {
                return new SortedSetDocValues() {

                    int index = 0;

                    @Override
                    public long nextOrd() {
                        if (index >= info.binaryProducer.dvBytesValuesSet.size()) {
                            return NO_MORE_ORDS;
                        }
                        return index++;
                    }

                    @Override
                    public void setDocument(int docID) {
                        index = 0;
                    }

                    @Override
                    public BytesRef lookupOrd(long ord) {
                        return info.binaryProducer.getValue((int) ord);
                    }

                    @Override
                    public long getValueCount() {
                        return info.binaryProducer.dvBytesValuesSet.size();
                    }
                };
            } else {
                return null;
            }
        }

        @Override
        public Bits getDocsWithField(String field) throws IOException {
            Info info = fields.get(field);
            if (info != null && info.fieldInfo.getDocValuesType() != DocValuesType.NONE) {
                return new Bits.MatchAllBits(1);
            } else {
                return null;
            }
        }

        @Override
        public void checkIntegrity() throws IOException {
            // no-op
        }

        @Override
        public Fields fields() {
            Map<String, Info> filteredFields = new TreeMap<String, Info>();
            for (Map.Entry<String, Info> entry : fields.entrySet()) {
                if (entry.getValue().numTokens > 0) {
                    filteredFields.put(entry.getKey(), entry.getValue());
                }
            }
            return new MemoryFields(filteredFields );
        }

        private class MemoryFields extends Fields {

            private final Map<String, Info> fields;

            public MemoryFields(Map<String, Info> fields) {
                this.fields = fields;
            }

            @Override
            public Iterator<String> iterator() {
                return fields.keySet().iterator();
            }

            @Override
            public Terms terms(final String field) {
                final Info info = fields.get(field);
                if (info == null) {
                    return null;
                }

                return new Terms() {
                    @Override
                    public TermsEnum iterator() {
                        return new MemoryTermsEnum(info);
                    }

                    @Override
                    public long size() {
                        return info.terms.size();
                    }

                    @Override
                    public long getSumTotalTermFreq() {
                        return info.sumTotalTermFreq;
                    }

                    @Override
                    public long getSumDocFreq() {
                        // each term has df=1
                        return info.terms.size();
                    }

                    @Override
                    public int getDocCount() {
                        return size() > 0 ? 1 : 0;
                    }

                    @Override
                    public boolean hasFreqs() {
                        return true;
                    }

                    @Override
                    public boolean hasOffsets() {
                        return storeOffsets;
                    }

                    @Override
                    public boolean hasPositions() {
                        return true;
                    }

                    @Override
                    public boolean hasPayloads() {
                        return storePayloads;
                    }
                };
            }

            @Override
            public int size() {
                return fields.size();
            }
        }

        private class MemoryTermsEnum extends TermsEnum {
            private final Info info;
            private final BytesRef br = new BytesRef();
            int termUpto = -1;

            public MemoryTermsEnum(Info info) {
                this.info = info;
                info.sortTerms();
            }

            private final int binarySearch(BytesRef b, BytesRef bytesRef, int low,
                                           int high, BytesRefHash hash, int[] ords) {
                int mid = 0;
                while (low <= high) {
                    mid = (low + high) >>> 1;
                    hash.get(ords[mid], bytesRef);
                    final int cmp = bytesRef.compareTo(b);
                    if (cmp < 0) {
                        low = mid + 1;
                    } else if (cmp > 0) {
                        high = mid - 1;
                    } else {
                        return mid;
                    }
                }
                assert bytesRef.compareTo(b) != 0;
                return -(low + 1);
            }


            @Override
            public boolean seekExact(BytesRef text) {
                termUpto = binarySearch(text, br, 0, info.terms.size()-1, info.terms, info.sortedTerms);
                return termUpto >= 0;
            }

            @Override
            public SeekStatus seekCeil(BytesRef text) {
                termUpto = binarySearch(text, br, 0, info.terms.size()-1, info.terms, info.sortedTerms);
                if (termUpto < 0) { // not found; choose successor
                    termUpto = -termUpto-1;
                    if (termUpto >= info.terms.size()) {
                        return SeekStatus.END;
                    } else {
                        info.terms.get(info.sortedTerms[termUpto], br);
                        return SeekStatus.NOT_FOUND;
                    }
                } else {
                    return SeekStatus.FOUND;
                }
            }

            @Override
            public void seekExact(long ord) {
                assert ord < info.terms.size();
                termUpto = (int) ord;
                info.terms.get(info.sortedTerms[termUpto], br);
            }

            @Override
            public BytesRef next() {
                termUpto++;
                if (termUpto >= info.terms.size()) {
                    return null;
                } else {
                    info.terms.get(info.sortedTerms[termUpto], br);
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
                return info.sliceArray.freq[info.sortedTerms[termUpto]];
            }

            @Override
            public PostingsEnum postings(PostingsEnum reuse, int flags) {
                if (reuse == null || !(reuse instanceof MemoryPostingsEnum)) {
                    reuse = new MemoryPostingsEnum();
                }
                final int ord = info.sortedTerms[termUpto];
                return ((MemoryPostingsEnum) reuse).reset(info.sliceArray.start[ord], info.sliceArray.end[ord], info.sliceArray.freq[ord]);
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

        private class MemoryPostingsEnum extends PostingsEnum {

            private final SliceReader sliceReader;
            private int posUpto; // for assert
            private boolean hasNext;
            private int doc = -1;
            private int freq;
            private int pos;
            private int startOffset;
            private int endOffset;
            private int payloadIndex;
            private final BytesRefBuilder payloadBuilder;//only non-null when storePayloads

            public MemoryPostingsEnum() {
                this.sliceReader = new SliceReader(intBlockPool);
                this.payloadBuilder = storePayloads ? new BytesRefBuilder() : null;
            }

            public PostingsEnum reset(int start, int end, int freq) {
                this.sliceReader.reset(start, end);
                posUpto = 0; // for assert
                hasNext = true;
                doc = -1;
                this.freq = freq;
                return this;
            }


            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                pos = -1;
                if (hasNext) {
                    hasNext = false;
                    return doc = 0;
                } else {
                    return doc = NO_MORE_DOCS;
                }
            }

            @Override
            public int advance(int target) throws IOException {
                return slowAdvance(target);
            }

            @Override
            public int freq() throws IOException {
                return freq;
            }

            @Override
            public int nextPosition() {
                posUpto++;
                assert posUpto <= freq;
                assert !sliceReader.endOfSlice() : " stores offsets : " + startOffset;
                int pos = sliceReader.readInt();
                if (storeOffsets) {
                    //pos = sliceReader.readInt();
                    startOffset = sliceReader.readInt();
                    endOffset = sliceReader.readInt();
                }
                if (storePayloads) {
                    payloadIndex = sliceReader.readInt();
                }
                return pos;
            }

            @Override
            public int startOffset() {
                return startOffset;
            }

            @Override
            public int endOffset() {
                return endOffset;
            }

            @Override
            public BytesRef getPayload() {
                if (payloadBuilder == null || payloadIndex == -1) {
                    return null;
                }
                return payloadsBytesRefs.get(payloadBuilder, payloadIndex);
            }

            @Override
            public long cost() {
                return 1;
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

        @Override
        public int numDocs() {
            if (DEBUG) System.err.println("MemoryIndexReader.numDocs");
            return 1;
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
        protected void doClose() {
            if (DEBUG) System.err.println("MemoryIndexReader.doClose");
        }

        @Override
        public NumericDocValues getNormValues(String field) {
            Info info = fields.get(field);
            if (info == null) {
                return null;
            }
            return info.getNormDocValues();
        }

    }

    /**
     * Resets the {@link MemoryIndex} to its initial state and recycles all internal buffers.
     */
    public void reset() {
        fields.clear();
        this.normSimilarity = IndexSearcher.getDefaultSimilarity();
        byteBlockPool.reset(false, false); // no need to 0-fill the buffers
        intBlockPool.reset(true, false); // here must must 0-fill since we use slices
        if (payloadsBytesRefs != null) {
            payloadsBytesRefs.clear();
        }
        this.frozen = false;
    }

    private static final class SliceByteStartArray extends DirectBytesStartArray {
        int[] start; // the start offset in the IntBlockPool per term
        int[] end; // the end pointer in the IntBlockPool for the postings slice per term
        int[] freq; // the term frequency

        public SliceByteStartArray(int initSize) {
            super(initSize);
        }

        @Override
        public int[] init() {
            final int[] ord = super.init();
            start = new int[ArrayUtil.oversize(ord.length, RamUsageEstimator.NUM_BYTES_INT)];
            end = new int[ArrayUtil.oversize(ord.length, RamUsageEstimator.NUM_BYTES_INT)];
            freq = new int[ArrayUtil.oversize(ord.length, RamUsageEstimator.NUM_BYTES_INT)];
            assert start.length >= ord.length;
            assert end.length >= ord.length;
            assert freq.length >= ord.length;
            return ord;
        }

        @Override
        public int[] grow() {
            final int[] ord = super.grow();
            if (start.length < ord.length) {
                start = ArrayUtil.grow(start, ord.length);
                end = ArrayUtil.grow(end, ord.length);
                freq = ArrayUtil.grow(freq, ord.length);
            }
            assert start.length >= ord.length;
            assert end.length >= ord.length;
            assert freq.length >= ord.length;
            return ord;
        }

        @Override
        public int[] clear() {
            start = end = null;
            return super.clear();
        }

    }
}
