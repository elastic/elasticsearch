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

/**
 * Code to maintain and access indices.
 * <!-- TODO: add IndexWriter, IndexWriterConfig, DocValues, etc etc -->
 * <h2>Table Of Contents</h2>
 *     <ol>
 *         <li><a href="#postings">Postings APIs</a>
 *             <ul>
 *                 <li><a href="#fields">Fields</a></li>
 *                 <li><a href="#terms">Terms</a></li>
 *                 <li><a href="#documents">Documents</a></li>
 *                 <li><a href="#positions">Positions</a></li>
 *             </ul>
 *         </li>
 *         <li><a href="#stats">Index Statistics</a>
 *             <ul>
 *                 <li><a href="#termstats">Term-level</a></li>
 *                 <li><a href="#fieldstats">Field-level</a></li>
 *                 <li><a href="#segmentstats">Segment-level</a></li>
 *                 <li><a href="#documentstats">Document-level</a></li>
 *             </ul>
 *         </li>
 *     </ol>
 * <a name="postings"></a>
 * <h2>Postings APIs</h2>
 * <a name="fields"></a>
 * <h3>
 *     Fields
 * </h3>
 * <p>
 * {@link org.apache.lucene5_shaded.index.Fields} is the initial entry point into the
 * postings APIs, this can be obtained in several ways:
 * <pre class="prettyprint">
 * // access indexed fields for an index segment
 * Fields fields = reader.fields();
 * // access term vector fields for a specified document
 * Fields fields = reader.getTermVectors(docid);
 * </pre>
 * Fields implements Java's Iterable interface, so it's easy to enumerate the
 * list of fields:
 * <pre class="prettyprint">
 * // enumerate list of fields
 * for (String field : fields) {
 *   // access the terms for this field
 *   Terms terms = fields.terms(field);
 * }
 * </pre>
 * <a name="terms"></a>
 * <h3>
 *     Terms
 * </h3>
 * <p>
 * {@link org.apache.lucene5_shaded.index.Terms} represents the collection of terms
 * within a field, exposes some metadata and <a href="#fieldstats">statistics</a>,
 * and an API for enumeration.
 * <pre class="prettyprint">
 * // metadata about the field
 * System.out.println("positions? " + terms.hasPositions());
 * System.out.println("offsets? " + terms.hasOffsets());
 * System.out.println("payloads? " + terms.hasPayloads());
 * // iterate through terms
 * TermsEnum termsEnum = terms.iterator(null);
 * BytesRef term = null;
 * while ((term = termsEnum.next()) != null) {
 *   doSomethingWith(termsEnum.term());
 * }
 * </pre>
 * {@link org.apache.lucene5_shaded.index.TermsEnum} provides an iterator over the list
 * of terms within a field, some <a href="#termstats">statistics</a> about the term,
 * and methods to access the term's <a href="#documents">documents</a> and
 * <a href="#positions">positions</a>.
 * <pre class="prettyprint">
 * // seek to a specific term
 * boolean found = termsEnum.seekExact(new BytesRef("foobar"));
 * if (found) {
 *   // get the document frequency
 *   System.out.println(termsEnum.docFreq());
 *   // enumerate through documents
 *   PostingsEnum docs = termsEnum.postings(null, null);
 *   // enumerate through documents and positions
 *   PostingsEnum docsAndPositions = termsEnum.postings(null, null, PostingsEnum.FLAG_POSITIONS);
 * }
 * </pre>
 * <a name="documents"></a>
 * <h3>
 *   Documents
 * </h3>
 * <p>
 *   {@link org.apache.lucene5_shaded.index.PostingsEnum} is an extension of
 *   {@link org.apache.lucene5_shaded.search.DocIdSetIterator}that iterates over the list of
 *   documents for a term, along with the term frequency within that document.
 * <pre class="prettyprint">
 * int docid;
 * while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
 *   System.out.println(docid);
 *   System.out.println(docsEnum.freq());
 *  }
 * </pre>
 * <a name="positions"></a>
 * <h3>
 *   Positions
 * </h3>
 * <p>
 *   PostingsEnum also allows iteration
 *   of the positions a term occurred within the document, and any additional
 *   per-position information (offsets and payload).  The information available
 *   is controlled by flags passed to TermsEnum#postings
 * <pre class="prettyprint">
 * int docid;
 * PostingsEnum postings = termsEnum.postings(null, null, PostingsEnum.FLAG_PAYLOADS | PostingsEnum.FLAG_OFFSETS);
 * while ((docid = postings.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
 *   System.out.println(docid);
 *   int freq = postings.freq();
 *   for (int i = 0; i &lt; freq; i++) {
 *      System.out.println(postings.nextPosition());
 *      System.out.println(postings.startOffset());
 *      System.out.println(postings.endOffset());
 *      System.out.println(postings.getPayload());
 *   }
 * }
 * </pre>
 * <a name="stats"></a>
 * <h2>Index Statistics</h2>
 * <a name="termstats"></a>
 * <h3>
 *     Term statistics
 * </h3>
 *     <ul>
 *        <li>{@link org.apache.lucene5_shaded.index.TermsEnum#docFreq}: Returns the number of
 *            documents that contain at least one occurrence of the term. This statistic 
 *            is always available for an indexed term. Note that it will also count 
 *            deleted documents, when segments are merged the statistic is updated as 
 *            those deleted documents are merged away.
 *        <li>{@link org.apache.lucene5_shaded.index.TermsEnum#totalTermFreq}: Returns the number
 *            of occurrences of this term across all documents. Note that this statistic 
 *            is unavailable (returns <code>-1</code>) if term frequencies were omitted 
 *            from the index 
 *            ({@link org.apache.lucene5_shaded.index.IndexOptions#DOCS DOCS})
 *            for the field. Like docFreq(), it will also count occurrences that appear in 
 *            deleted documents.
 *     </ul>
 * <a name="fieldstats"></a>
 * <h3>
 *     Field statistics
 * </h3>
 *     <ul>
 *        <li>{@link org.apache.lucene5_shaded.index.Terms#size}: Returns the number of
 *            unique terms in the field. This statistic may be unavailable 
 *            (returns <code>-1</code>) for some Terms implementations such as
 *            {@link org.apache.lucene5_shaded.index.MultiTerms}, where it cannot be efficiently
 *            computed.  Note that this count also includes terms that appear only
 *            in deleted documents: when segments are merged such terms are also merged
 *            away and the statistic is then updated.
 *        <li>{@link org.apache.lucene5_shaded.index.Terms#getDocCount}: Returns the number of
 *            documents that contain at least one occurrence of any term for this field.
 *            This can be thought of as a Field-level docFreq(). Like docFreq() it will
 *            also count deleted documents.
 *        <li>{@link org.apache.lucene5_shaded.index.Terms#getSumDocFreq}: Returns the number of
 *            postings (term-document mappings in the inverted index) for the field. This
 *            can be thought of as the sum of {@link org.apache.lucene5_shaded.index.TermsEnum#docFreq}
 *            across all terms in the field, and like docFreq() it will also count postings
 *            that appear in deleted documents.
 *        <li>{@link org.apache.lucene5_shaded.index.Terms#getSumTotalTermFreq}: Returns the number
 *            of tokens for the field. This can be thought of as the sum of 
 *            {@link org.apache.lucene5_shaded.index.TermsEnum#totalTermFreq} across all terms in the
 *            field, and like totalTermFreq() it will also count occurrences that appear in
 *            deleted documents, and will be unavailable (returns <code>-1</code>) if term 
 *            frequencies were omitted from the index 
 *            ({@link org.apache.lucene5_shaded.index.IndexOptions#DOCS DOCS})
 *            for the field.
 *     </ul>
 * <a name="segmentstats"></a>
 * <h3>
 *     Segment statistics
 * </h3>
 *     <ul>
 *        <li>{@link org.apache.lucene5_shaded.index.IndexReader#maxDoc}: Returns the number of
 *            documents (including deleted documents) in the index. 
 *        <li>{@link org.apache.lucene5_shaded.index.IndexReader#numDocs}: Returns the number
 *            of live documents (excluding deleted documents) in the index.
 *        <li>{@link org.apache.lucene5_shaded.index.IndexReader#numDeletedDocs}: Returns the
 *            number of deleted documents in the index.
 *        <li>{@link org.apache.lucene5_shaded.index.Fields#size}: Returns the number of indexed
 *            fields.
 *     </ul>
 * <a name="documentstats"></a>
 * <h3>
 *     Document statistics
 * </h3>
 * <p>
 * Document statistics are available during the indexing process for an indexed field: typically
 * a {@link org.apache.lucene5_shaded.search.similarities.Similarity} implementation will store some
 * of these values (possibly in a lossy way), into the normalization value for the document in
 * its {@link org.apache.lucene5_shaded.search.similarities.Similarity#computeNorm} method.
 *     <ul>
 *        <li>{@link org.apache.lucene5_shaded.index.FieldInvertState#getLength}: Returns the number of
 *            tokens for this field in the document. Note that this is just the number
 *            of times that {@link org.apache.lucene5_shaded.analysis.TokenStream#incrementToken} returned
 *            true, and is unrelated to the values in 
 *            {@link org.apache.lucene5_shaded.analysis.tokenattributes.PositionIncrementAttribute}.
 *        <li>{@link org.apache.lucene5_shaded.index.FieldInvertState#getNumOverlap}: Returns the number
 *            of tokens for this field in the document that had a position increment of zero. This
 *            can be used to compute a document length that discounts artificial tokens
 *            such as synonyms.
 *        <li>{@link org.apache.lucene5_shaded.index.FieldInvertState#getPosition}: Returns the accumulated
 *            position value for this field in the document: computed from the values of
 *            {@link org.apache.lucene5_shaded.analysis.tokenattributes.PositionIncrementAttribute} and including
 *            {@link org.apache.lucene5_shaded.analysis.Analyzer#getPositionIncrementGap}s across multivalued
 *            fields.
 *        <li>{@link org.apache.lucene5_shaded.index.FieldInvertState#getOffset}: Returns the total
 *            character offset value for this field in the document: computed from the values of
 *            {@link org.apache.lucene5_shaded.analysis.tokenattributes.OffsetAttribute} returned by
 *            {@link org.apache.lucene5_shaded.analysis.TokenStream#end}, and including
 *            {@link org.apache.lucene5_shaded.analysis.Analyzer#getOffsetGap}s across multivalued
 *            fields.
 *        <li>{@link org.apache.lucene5_shaded.index.FieldInvertState#getUniqueTermCount}: Returns the number
 *            of unique terms encountered for this field in the document.
 *        <li>{@link org.apache.lucene5_shaded.index.FieldInvertState#getMaxTermFrequency}: Returns the maximum
 *            frequency across all unique terms encountered for this field in the document. 
 *     </ul>
 * <p>
 * Additional user-supplied statistics can be added to the document as DocValues fields and
 * accessed via {@link org.apache.lucene5_shaded.index.LeafReader#getNumericDocValues}.
 */
package org.apache.lucene5_shaded.index;
