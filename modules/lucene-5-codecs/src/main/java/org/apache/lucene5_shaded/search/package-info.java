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
 * Code to search indices.
 * 
 * <h2>Table Of Contents</h2>
 *     <ol>
 *         <li><a href="#search">Search Basics</a></li>
 *         <li><a href="#query">The Query Classes</a></li>
 *         <li><a href="#scoring">Scoring: Introduction</a></li>
 *         <li><a href="#scoringBasics">Scoring: Basics</a></li>
 *         <li><a href="#changingScoring">Changing the Scoring</a></li>
 *         <li><a href="#algorithm">Appendix: Search Algorithm</a></li>
 *     </ol>
 * 
 * 
 * <a name="search"></a>
 * <h2>Search Basics</h2>
 * <p>
 * Lucene offers a wide variety of {@link org.apache.lucene5_shaded.search.Query} implementations, most of which are in
 * this package, its subpackage ({@link org.apache.lucene5_shaded.search.spans spans},
 * or the <a href="{@docRoot}/../queries/overview-summary.html">queries module</a>. These implementations can be combined in a wide 
 * variety of ways to provide complex querying capabilities along with information about where matches took place in the document 
 * collection. The <a href="#query">Query Classes</a> section below highlights some of the more important Query classes. For details 
 * on implementing your own Query class, see <a href="#customQueriesExpert">Custom Queries -- Expert Level</a> below.
 * <p>
 * To perform a search, applications usually call {@link
 * org.apache.lucene5_shaded.search.IndexSearcher#search(Query,int)}.
 * <p>
 * Once a Query has been created and submitted to the {@link org.apache.lucene5_shaded.search.IndexSearcher IndexSearcher}, the scoring
 * process begins. After some infrastructure setup, control finally passes to the {@link org.apache.lucene5_shaded.search.Weight Weight}
 * implementation and its {@link org.apache.lucene5_shaded.search.Scorer Scorer} or {@link org.apache.lucene5_shaded.search.BulkScorer BulkScore}
 * instances. See the <a href="#algorithm">Algorithm</a> section for more notes on the process.
 *     <!-- FILL IN MORE HERE -->   
 *     <!-- TODO: this page over-links the same things too many times -->
 * 
 * 
 * <a name="query"></a>
 * <h2>Query Classes</h2>
 * <h3>
 *     {@link org.apache.lucene5_shaded.search.TermQuery TermQuery}
 * </h3>
 * 
 * <p>Of the various implementations of
 *     {@link org.apache.lucene5_shaded.search.Query Query}, the
 *     {@link org.apache.lucene5_shaded.search.TermQuery TermQuery}
 *     is the easiest to understand and the most often used in applications. A
 *     {@link org.apache.lucene5_shaded.search.TermQuery TermQuery} matches all the documents that contain the
 *     specified
 *     {@link org.apache.lucene5_shaded.index.Term Term},
 *     which is a word that occurs in a certain
 *     {@link org.apache.lucene5_shaded.document.Field Field}.
 *     Thus, a {@link org.apache.lucene5_shaded.search.TermQuery TermQuery} identifies and scores all
 *     {@link org.apache.lucene5_shaded.document.Document Document}s that have a
 *         {@link org.apache.lucene5_shaded.document.Field Field} with the specified string in it.
 *     Constructing a {@link org.apache.lucene5_shaded.search.TermQuery TermQuery}
 *     is as simple as:
 *     <pre class="prettyprint">
 *         TermQuery tq = new TermQuery(new Term("fieldName", "term"));
 *     </pre>In this example, the {@link org.apache.lucene5_shaded.search.Query Query} identifies all
 *         {@link org.apache.lucene5_shaded.document.Document Document}s that have the
 *         {@link org.apache.lucene5_shaded.document.Field Field} named <tt>"fieldName"</tt>
 *     containing the word <tt>"term"</tt>.
 * <h3>
 *     {@link org.apache.lucene5_shaded.search.BooleanQuery BooleanQuery}
 * </h3>
 * 
 * <p>Things start to get interesting when one combines multiple
 *     {@link org.apache.lucene5_shaded.search.TermQuery TermQuery} instances into a
 *         {@link org.apache.lucene5_shaded.search.BooleanQuery BooleanQuery}.
 *     A {@link org.apache.lucene5_shaded.search.BooleanQuery BooleanQuery} contains multiple
 *     {@link org.apache.lucene5_shaded.search.BooleanClause BooleanClause}s,
 *     where each clause contains a sub-query ({@link org.apache.lucene5_shaded.search.Query Query}
 *     instance) and an operator (from 
 *         {@link org.apache.lucene5_shaded.search.BooleanClause.Occur BooleanClause.Occur})
 *     describing how that sub-query is combined with the other clauses:
 *     <ol>
 * 
 *         <li><p>{@link org.apache.lucene5_shaded.search.BooleanClause.Occur#SHOULD SHOULD} &mdash; Use this operator when a clause can occur in the result set, but is not required.
 *             If a query is made up of all SHOULD clauses, then every document in the result
 *             set matches at least one of these clauses.</p></li>
 * 
 *         <li><p>{@link org.apache.lucene5_shaded.search.BooleanClause.Occur#MUST MUST} &mdash; Use this operator when a clause is required to occur in the result set. Every
 *             document in the result set will match
 *             all such clauses.</p></li>
 * 
 *         <li><p>{@link org.apache.lucene5_shaded.search.BooleanClause.Occur#MUST_NOT MUST NOT} &mdash; Use this operator when a
 *             clause must not occur in the result set. No
 *             document in the result set will match
 *             any such clauses.</p></li>
 *     </ol>
 *     Boolean queries are constructed by adding two or more
 *     {@link org.apache.lucene5_shaded.search.BooleanClause BooleanClause}
 *     instances. If too many clauses are added, a {@link org.apache.lucene5_shaded.search.BooleanQuery.TooManyClauses TooManyClauses}
 *     exception will be thrown during searching. This most often occurs
 *     when a {@link org.apache.lucene5_shaded.search.Query Query}
 *     is rewritten into a {@link org.apache.lucene5_shaded.search.BooleanQuery BooleanQuery} with many
 *     {@link org.apache.lucene5_shaded.search.TermQuery TermQuery} clauses,
 *     for example by {@link org.apache.lucene5_shaded.search.WildcardQuery WildcardQuery}.
 *     The default setting for the maximum number
 *     of clauses 1024, but this can be changed via the
 *     static method {@link org.apache.lucene5_shaded.search.BooleanQuery#setMaxClauseCount(int)}.
 * 
 * <h3>Phrases</h3>
 * 
 * <p>Another common search is to find documents containing certain phrases. This
 *     is handled three different ways:
 *     <ol>
 *         <li>
 *             <p>{@link org.apache.lucene5_shaded.search.PhraseQuery PhraseQuery}
 *                 &mdash; Matches a sequence of
 *                 {@link org.apache.lucene5_shaded.index.Term Term}s.
 *                 {@link org.apache.lucene5_shaded.search.PhraseQuery PhraseQuery} uses a slop factor to determine
 *                 how many positions may occur between any two terms in the phrase and still be considered a match.
 *           The slop is 0 by default, meaning the phrase must match exactly.</p>
 *         </li>
 *         <li>
 *             <p>{@link org.apache.lucene5_shaded.search.MultiPhraseQuery MultiPhraseQuery}
 *                 &mdash; A more general form of PhraseQuery that accepts multiple Terms
 *                 for a position in the phrase. For example, this can be used to perform phrase queries that also
 *                 incorporate synonyms.
 *         </li>
 *         <li>
 *             <p>{@link org.apache.lucene5_shaded.search.spans.SpanNearQuery SpanNearQuery}
 *                 &mdash; Matches a sequence of other
 *                 {@link org.apache.lucene5_shaded.search.spans.SpanQuery SpanQuery}
 *                 instances. {@link org.apache.lucene5_shaded.search.spans.SpanNearQuery SpanNearQuery} allows for
 *                 much more
 *                 complicated phrase queries since it is constructed from other 
 *                     {@link org.apache.lucene5_shaded.search.spans.SpanQuery SpanQuery}
 *                 instances, instead of only {@link org.apache.lucene5_shaded.search.TermQuery TermQuery}
 *                 instances.</p>
 *         </li>
 *     </ol>
 * 
 * <h3>
 *     {@link org.apache.lucene5_shaded.search.TermRangeQuery TermRangeQuery}
 * </h3>
 * 
 * <p>The
 *     {@link org.apache.lucene5_shaded.search.TermRangeQuery TermRangeQuery}
 *     matches all documents that occur in the
 *     exclusive range of a lower
 *     {@link org.apache.lucene5_shaded.index.Term Term}
 *     and an upper
 *     {@link org.apache.lucene5_shaded.index.Term Term}
 *     according to {@link org.apache.lucene5_shaded.util.BytesRef#compareTo BytesRef.compareTo()}. It is not intended
 *     for numerical ranges; use {@link org.apache.lucene5_shaded.search.NumericRangeQuery NumericRangeQuery} instead.
 * 
 *     For example, one could find all documents
 *     that have terms beginning with the letters <tt>a</tt> through <tt>c</tt>.
 * 
 * <h3>
 *     {@link org.apache.lucene5_shaded.search.NumericRangeQuery NumericRangeQuery}
 * </h3>
 * 
 * <p>The
 *     {@link org.apache.lucene5_shaded.search.NumericRangeQuery NumericRangeQuery}
 *     matches all documents that occur in a numeric range.
 *     For NumericRangeQuery to work, you must index the values
 *     using a one of the numeric fields ({@link org.apache.lucene5_shaded.document.IntField IntField},
 *     {@link org.apache.lucene5_shaded.document.LongField LongField}, {@link org.apache.lucene5_shaded.document.FloatField FloatField},
 *     or {@link org.apache.lucene5_shaded.document.DoubleField DoubleField}).
 * 
 * <h3>
 *     {@link org.apache.lucene5_shaded.search.PrefixQuery PrefixQuery},
 *     {@link org.apache.lucene5_shaded.search.WildcardQuery WildcardQuery},
 *     {@link org.apache.lucene5_shaded.search.RegexpQuery RegexpQuery}
 * </h3>
 * 
 * <p>While the
 *     {@link org.apache.lucene5_shaded.search.PrefixQuery PrefixQuery}
 *     has a different implementation, it is essentially a special case of the
 *     {@link org.apache.lucene5_shaded.search.WildcardQuery WildcardQuery}.
 *     The {@link org.apache.lucene5_shaded.search.PrefixQuery PrefixQuery} allows an application
 *     to identify all documents with terms that begin with a certain string. The 
 *         {@link org.apache.lucene5_shaded.search.WildcardQuery WildcardQuery} generalizes this by allowing
 *     for the use of <tt>*</tt> (matches 0 or more characters) and <tt>?</tt> (matches exactly one character) wildcards.
 *     Note that the {@link org.apache.lucene5_shaded.search.WildcardQuery WildcardQuery} can be quite slow. Also
 *     note that
 *     {@link org.apache.lucene5_shaded.search.WildcardQuery WildcardQuery} should
 *     not start with <tt>*</tt> and <tt>?</tt>, as these are extremely slow. 
 *     Some QueryParsers may not allow this by default, but provide a <code>setAllowLeadingWildcard</code> method
 *     to remove that protection.
 *     The {@link org.apache.lucene5_shaded.search.RegexpQuery RegexpQuery} is even more general than WildcardQuery,
 *     allowing an application to identify all documents with terms that match a regular expression pattern.
 * <h3>
 *     {@link org.apache.lucene5_shaded.search.FuzzyQuery FuzzyQuery}
 * </h3>
 * 
 * <p>A
 *     {@link org.apache.lucene5_shaded.search.FuzzyQuery FuzzyQuery}
 *     matches documents that contain terms similar to the specified term. Similarity is
 *     determined using
 *     <a href="http://en.wikipedia.org/wiki/Levenshtein_distance">Levenshtein distance</a>.
 *     This type of query can be useful when accounting for spelling variations in the collection.
 * 
 * 
 * <a name="scoring"></a>
 * <h2>Scoring &mdash; Introduction</h2>
 * <p>Lucene scoring is the heart of why we all love Lucene. It is blazingly fast and it hides 
 *    almost all of the complexity from the user. In a nutshell, it works.  At least, that is, 
 *    until it doesn't work, or doesn't work as one would expect it to work.  Then we are left 
 *    digging into Lucene internals or asking for help on 
 *    <a href="mailto:java-user@lucene.apache.org">java-user@lucene5_shaded.apache.org</a> to figure out
 *    why a document with five of our query terms scores lower than a different document with 
 *    only one of the query terms. 
 * <p>While this document won't answer your specific scoring issues, it will, hopefully, point you 
 *   to the places that can help you figure out the <i>what</i> and <i>why</i> of Lucene scoring.
 * <p>Lucene scoring supports a number of pluggable information retrieval 
 *    <a href="http://en.wikipedia.org/wiki/Information_retrieval#Model_types">models</a>, including:
 *    <ul>
 *      <li><a href="http://en.wikipedia.org/wiki/Vector_Space_Model">Vector Space Model (VSM)</a></li>
 *      <li><a href="http://en.wikipedia.org/wiki/Probabilistic_relevance_model">Probablistic Models</a> such as 
 *          <a href="http://en.wikipedia.org/wiki/Probabilistic_relevance_model_(BM25)">Okapi BM25</a> and
 *          <a href="http://en.wikipedia.org/wiki/Divergence-from-randomness_model">DFR</a></li>
 *      <li><a href="http://en.wikipedia.org/wiki/Language_model">Language models</a></li>
 *    </ul>
 *    These models can be plugged in via the {@link org.apache.lucene5_shaded.search.similarities Similarity API},
 *    and offer extension hooks and parameters for tuning. In general, Lucene first finds the documents
 *    that need to be scored based on boolean logic in the Query specification, and then ranks this subset of
 *    matching documents via the retrieval model. For some valuable references on VSM and IR in general refer to
 *    <a href="http://wiki.apache.org/lucene-java/InformationRetrieval">Lucene Wiki IR references</a>.
 * <p>The rest of this document will cover <a href="#scoringBasics">Scoring basics</a> and explain how to 
 *    change your {@link org.apache.lucene5_shaded.search.similarities.Similarity Similarity}. Next, it will cover
 *    ways you can customize the lucene5_shaded internals in
 *    <a href="#customQueriesExpert">Custom Queries -- Expert Level</a>, which gives details on 
 *    implementing your own {@link org.apache.lucene5_shaded.search.Query Query} class and related functionality.
 *    Finally, we will finish up with some reference material in the <a href="#algorithm">Appendix</a>.
 * 
 * 
 * <a name="scoringBasics"></a>
 * <h2>Scoring &mdash; Basics</h2>
 * <p>Scoring is very much dependent on the way documents are indexed, so it is important to understand 
 *    indexing. (see <a href="{@docRoot}/overview-summary.html#overview_description">Lucene overview</a> 
 *    before continuing on with this section) Be sure to use the useful
 *    {@link org.apache.lucene5_shaded.search.IndexSearcher#explain(Query, int) IndexSearcher.explain(Query, doc)}
 *    to understand how the score for a certain matching document was
 *    computed.
 * 
 * <p>Generally, the Query determines which documents match (a binary
 *   decision), while the Similarity determines how to assign scores to
 *   the matching documents.
 * 
 * </p>
 * <h3>Fields and Documents</h3>
 * <p>In Lucene, the objects we are scoring are {@link org.apache.lucene5_shaded.document.Document Document}s.
 *    A Document is a collection of {@link org.apache.lucene5_shaded.document.Field Field}s.  Each Field has
 *    {@link org.apache.lucene5_shaded.document.FieldType semantics} about how it is created and stored
 *    ({@link org.apache.lucene5_shaded.document.FieldType#tokenized() tokenized},
 *    {@link org.apache.lucene5_shaded.document.FieldType#stored() stored}, etc). It is important to note that
 *    Lucene scoring works on Fields and then combines the results to return Documents. This is 
 *    important because two Documents with the exact same content, but one having the content in two
 *    Fields and the other in one Field may return different scores for the same query due to length
 *    normalization.
 * <h3>Score Boosting</h3>
 * <p>Lucene allows influencing search results by "boosting" at different times:
 *    <ul>                   
 *       <li><b>Index-time boost</b> by calling
 *        {@link org.apache.lucene5_shaded.document.Field#setBoost(float) Field.setBoost()} before a document is
 *        added to the index.</li>
 *       <li><b>Query-time boost</b> by applying a boost to a query by wrapping with
 *       {@link org.apache.lucene5_shaded.search.BoostQuery}.</li>
 *    </ul>    
 * <p>Indexing time boosts are pre-processed for storage efficiency and written to
 *    storage for a field as follows:
 *    <ul>
 *        <li>All boosts of that field (i.e. all boosts under the same field name in that doc) are 
 *            multiplied.</li>
 *        <li>The boost is then encoded into a normalization value by the Similarity
 *            object at index-time: {@link org.apache.lucene5_shaded.search.similarities.Similarity#computeNorm computeNorm()}.
 *            The actual encoding depends upon the Similarity implementation, but note that most
 *            use a lossy encoding (such as multiplying the boost with document length or similar, packed
 *            into a single byte!).</li>
 *        <li>Decoding of any index-time normalization values and integration into the document's score is also performed 
 *            at search time by the Similarity.</li>
 *     </ul>
 * 
 * <a name="changingScoring"></a>
 * <h2>Changing Scoring &mdash; Similarity</h2>
 * <p>
 * Changing {@link org.apache.lucene5_shaded.search.similarities.Similarity Similarity} is an easy way to
 * influence scoring, this is done at index-time with 
 * {@link org.apache.lucene5_shaded.index.IndexWriterConfig#setSimilarity(org.apache.lucene5_shaded.search.similarities.Similarity)
 *  IndexWriterConfig.setSimilarity(Similarity)} and at query-time with
 * {@link org.apache.lucene5_shaded.search.IndexSearcher#setSimilarity(org.apache.lucene5_shaded.search.similarities.Similarity)
 *  IndexSearcher.setSimilarity(Similarity)}.  Be sure to use the same
 * Similarity at query-time as at index-time (so that norms are
 * encoded/decoded correctly); Lucene makes no effort to verify this.
 * <p>
 * You can influence scoring by configuring a different built-in Similarity implementation, or by tweaking its
 * parameters, subclassing it to override behavior. Some implementations also offer a modular API which you can
 * extend by plugging in a different component (e.g. term frequency normalizer).
 * <p>
 * Finally, you can extend the low level {@link org.apache.lucene5_shaded.search.similarities.Similarity Similarity} directly
 * to implement a new retrieval model, or to use external scoring factors particular to your application. For example,
 * a custom Similarity can access per-document values via {@link org.apache.lucene5_shaded.index.NumericDocValues} and
 * integrate them into the score.
 * <p>
 * See the {@link org.apache.lucene5_shaded.search.similarities} package documentation for information
 * on the built-in available scoring models and extending or changing Similarity.
 * 
 * 
 * <a name="customQueriesExpert"></a>
 * <h2>Custom Queries &mdash; Expert Level</h2>
 * 
 * <p>Custom queries are an expert level task, so tread carefully and be prepared to share your code if
 *     you want help.
 * 
 * <p>With the warning out of the way, it is possible to change a lot more than just the Similarity
 *     when it comes to matching and scoring in Lucene. Lucene's search is a complex mechanism that is grounded by
 *     <span>three main classes</span>:
 *     <ol>
 *         <li>
 *             {@link org.apache.lucene5_shaded.search.Query Query} &mdash; The abstract object representation of the
 *             user's information need.</li>
 *         <li>
 *             {@link org.apache.lucene5_shaded.search.Weight Weight} &mdash; The internal interface representation of
 *             the user's Query, so that Query objects may be reused.
 *             This is global (across all segments of the index) and
 *             generally will require global statistics (such as docFreq
 *             for a given term across all segments).</li>
 *         <li>
 *             {@link org.apache.lucene5_shaded.search.Scorer Scorer} &mdash; An abstract class containing common
 *             functionality for scoring. Provides both scoring and
 *             explanation capabilities.  This is created per-segment.</li>
 *         <li>
 *             {@link org.apache.lucene5_shaded.search.BulkScorer BulkScorer} &mdash; An abstract class that scores
 *       a range of documents.  A default implementation simply iterates through the hits from
 *       {@link org.apache.lucene5_shaded.search.Scorer Scorer}, but some queries such as
 *       {@link org.apache.lucene5_shaded.search.BooleanQuery BooleanQuery} have more efficient
 *       implementations.</li>
 *     </ol>
 *     Details on each of these classes, and their children, can be found in the subsections below.
 * <h3>The Query Class</h3>
 *     <p>In some sense, the
 *         {@link org.apache.lucene5_shaded.search.Query Query}
 *         class is where it all begins. Without a Query, there would be
 *         nothing to score. Furthermore, the Query class is the catalyst for the other scoring classes as it
 *         is often responsible
 *         for creating them or coordinating the functionality between them. The
 *         {@link org.apache.lucene5_shaded.search.Query Query} class has several methods that are important for
 *         derived classes:
 *         <ol>
 *             <li>{@link org.apache.lucene5_shaded.search.Query#createWeight(IndexSearcher,boolean) createWeight(IndexSearcher searcher,boolean)} &mdash; A
 *                 {@link org.apache.lucene5_shaded.search.Weight Weight} is the internal representation of the
 *                 Query, so each Query implementation must
 *                 provide an implementation of Weight. See the subsection on <a
 *                     href="#weightClass">The Weight Interface</a> below for details on implementing the Weight
 *                 interface.</li>
 *             <li>{@link org.apache.lucene5_shaded.search.Query#rewrite(org.apache.lucene5_shaded.index.IndexReader) rewrite(IndexReader reader)} &mdash; Rewrites queries into primitive queries. Primitive queries are:
 *                 {@link org.apache.lucene5_shaded.search.TermQuery TermQuery},
 *                 {@link org.apache.lucene5_shaded.search.BooleanQuery BooleanQuery}, <span
 *                     >and other queries that implement {@link org.apache.lucene5_shaded.search.Query#createWeight(IndexSearcher,boolean) createWeight(IndexSearcher searcher,boolean,float)}</span></li>
 *         </ol>
 * <a name="weightClass"></a>
 * <h3>The Weight Interface</h3>
 *     <p>The
 *         {@link org.apache.lucene5_shaded.search.Weight Weight}
 *         interface provides an internal representation of the Query so that it can be reused. Any
 *         {@link org.apache.lucene5_shaded.search.IndexSearcher IndexSearcher}
 *         dependent state should be stored in the Weight implementation,
 *         not in the Query class. The interface defines five methods that must be implemented:
 *         <ol>
 *             <li>
 *                 {@link org.apache.lucene5_shaded.search.Weight#getQuery getQuery()} &mdash; Pointer to the
 *                 Query that this Weight represents.</li>
 *             <li>
 *                 {@link org.apache.lucene5_shaded.search.Weight#getValueForNormalization() getValueForNormalization()} &mdash;
 *                 A weight can return a floating point value to indicate its magnitude for query normalization. Typically
 *                 a weight such as TermWeight that scores via a {@link org.apache.lucene5_shaded.search.similarities.Similarity Similarity}
 *                 will just defer to the Similarity's implementation: 
 *                 {@link org.apache.lucene5_shaded.search.similarities.Similarity.SimWeight#getValueForNormalization SimWeight#getValueForNormalization()}.
 *                 For example, with {@link org.apache.lucene5_shaded.search.similarities.TFIDFSimilarity Lucene's classic vector-space formula}, this
 *                 is implemented as the sum of squared weights: <code>(idf * boost)<sup>2</sup></code></li>
 *             <li>
 *                 {@link org.apache.lucene5_shaded.search.Weight#normalize(float,float) normalize(float norm, float boost)} &mdash;
 *                 Performs query normalization: 
 *                 <ul>
 *                 <li><code>boost</code>: A query-boost factor from any wrapping queries that should be multiplied into every
 *                 document's score. For example, a TermQuery that is wrapped within a BooleanQuery with a boost of <code>5</code> would
 *                 receive this value at this time. This allows the TermQuery (the leaf node in this case) to compute this up-front
 *                 a single time (e.g. by multiplying into the IDF), rather than for every document.</li> 
 *                 <li><code>norm</code>: Passes in a a normalization factor which may
 *                 allow for comparing scores between queries.</li>
 *                 </ul>
 *                 Typically a weight such as TermWeight
 *                 that scores via a {@link org.apache.lucene5_shaded.search.similarities.Similarity Similarity} will just defer to the Similarity's implementation:
 *                 {@link org.apache.lucene5_shaded.search.similarities.Similarity.SimWeight#normalize SimWeight#normalize(float,float)}.</li>
 *             <li>
 *                 {@link org.apache.lucene5_shaded.search.Weight#scorer scorer()} &mdash;
 *                 Construct a new {@link org.apache.lucene5_shaded.search.Scorer Scorer} for this Weight. See <a href="#scorerClass">The Scorer Class</a>
 *                 below for help defining a Scorer. As the name implies, the Scorer is responsible for doing the actual scoring of documents 
 *                 given the Query.
 *             </li>
 *             <li>
 *                 {@link org.apache.lucene5_shaded.search.Weight#bulkScorer bulkScorer()} &mdash;
 *                 Construct a new {@link org.apache.lucene5_shaded.search.BulkScorer BulkScorer} for this Weight. See <a href="#bulkScorerClass">The BulkScorer Class</a>
 *                 below for help defining a BulkScorer. This is an optional method, and most queries do not implement it.
 *             </li>
 *             <li>
 *                 {@link org.apache.lucene5_shaded.search.Weight#explain(org.apache.lucene5_shaded.index.LeafReaderContext, int)
 *                   explain(LeafReaderContext context, int doc)} &mdash; Provide a means for explaining why a given document was
 *                 scored the way it was.
 *                 Typically a weight such as TermWeight
 *                 that scores via a {@link org.apache.lucene5_shaded.search.similarities.Similarity Similarity} will make use of the Similarity's implementation:
 *                 {@link org.apache.lucene5_shaded.search.similarities.Similarity.SimScorer#explain(int, Explanation) SimScorer#explain(int doc, Explanation freq)}.
 *             </li>
 *         </ol>
 * <a name="scorerClass"></a>
 * <h3>The Scorer Class</h3>
 *     <p>The
 *         {@link org.apache.lucene5_shaded.search.Scorer Scorer}
 *         abstract class provides common scoring functionality for all Scorer implementations and
 *         is the heart of the Lucene scoring process. The Scorer defines the following methods which
 *         must be implemented:
 *         <ol>
 *             <li>
 *                 {@link org.apache.lucene5_shaded.search.Scorer#iterator iterator()} &mdash; Return a
 *                 {@link org.apache.lucene5_shaded.search.DocIdSetIterator DocIdSetIterator} that can iterate over all
 *                 document that matches this Query.
 *             </li>
 *             <li>
 *                 {@link org.apache.lucene5_shaded.search.Scorer#docID docID()} &mdash; Returns the id of the
 *                 {@link org.apache.lucene5_shaded.document.Document Document} that contains the match.
 *             </li>
 *             <li>
 *                 {@link org.apache.lucene5_shaded.search.Scorer#score score()} &mdash; Return the score of the
 *                 current document. This value can be determined in any appropriate way for an application. For instance, the
 *                 {@link org.apache.lucene5_shaded.search.TermScorer TermScorer} simply defers to the configured Similarity:
 *                 {@link org.apache.lucene5_shaded.search.similarities.Similarity.SimScorer#score(int, float) SimScorer.score(int doc, float freq)}.
 *             </li>
 *             <li>
 *                 {@link org.apache.lucene5_shaded.search.Scorer#freq freq()} &mdash; Returns the number of matches
 *                 for the current document. This value can be determined in any appropriate way for an application. For instance, the
 *                 {@link org.apache.lucene5_shaded.search.TermScorer TermScorer} simply defers to the term frequency from the inverted index:
 *                 {@link org.apache.lucene5_shaded.index.PostingsEnum#freq PostingsEnum.freq()}.
 *             </li>
 *             <li>
 *                 {@link org.apache.lucene5_shaded.search.Scorer#getChildren getChildren()} &mdash; Returns any child subscorers
 *                 underneath this scorer. This allows for users to navigate the scorer hierarchy and receive more fine-grained
 *                 details on the scoring process.
 *             </li>
 *         </ol>
 * <a name="bulkScorerClass"></a>
 * <h3>The BulkScorer Class</h3>
 *     <p>The
 *         {@link org.apache.lucene5_shaded.search.BulkScorer BulkScorer} scores a range of documents.  There is only one
 *         abstract method:
 *         <ol>
 *             <li>
 *                 {@link org.apache.lucene5_shaded.search.BulkScorer#score(LeafCollector,org.apache.lucene5_shaded.util.Bits,int,int) score(LeafCollector,Bits,int,int)} &mdash;
 *     Score all documents up to but not including the specified max document.
 *       </li>
 *         </ol>
 * <h3>Why would I want to add my own Query?</h3>
 * 
 *     <p>In a nutshell, you want to add your own custom Query implementation when you think that Lucene's
 *         aren't appropriate for the
 *         task that you want to do. You might be doing some cutting edge research or you need more information
 *         back
 *         out of Lucene (similar to Doug adding SpanQuery functionality).
 * 
 * <!-- TODO: integrate this better, it's better served as an intro than an appendix -->
 * 
 * 
 * <a name="algorithm"></a>
 * <h2>Appendix: Search Algorithm</h2>
 * <p>This section is mostly notes on stepping through the Scoring process and serves as
 *    fertilizer for the earlier sections.
 * <p>In the typical search application, a {@link org.apache.lucene5_shaded.search.Query Query}
 *    is passed to the {@link org.apache.lucene5_shaded.search.IndexSearcher IndexSearcher},
 *    beginning the scoring process.
 * <p>Once inside the IndexSearcher, a {@link org.apache.lucene5_shaded.search.Collector Collector}
 *    is used for the scoring and sorting of the search results.
 *    These important objects are involved in a search:
 *    <ol>                
 *       <li>The {@link org.apache.lucene5_shaded.search.Weight Weight} object of the Query. The
 *           Weight object is an internal representation of the Query that allows the Query 
 *           to be reused by the IndexSearcher.</li>
 *       <li>The IndexSearcher that initiated the call.</li>     
 *       <li>A {@link org.apache.lucene5_shaded.search.Sort Sort} object for specifying how to sort
 *           the results if the standard score-based sort method is not desired.</li>                   
 *   </ol>       
 * <p>Assuming we are not sorting (since sorting doesn't affect the raw Lucene score),
 *    we call one of the search methods of the IndexSearcher, passing in the
 *    {@link org.apache.lucene5_shaded.search.Weight Weight} object created by
 *    {@link org.apache.lucene5_shaded.search.IndexSearcher#createNormalizedWeight(Query,boolean)
 *     IndexSearcher.createNormalizedWeight(Query,boolean)} and the number of results we want.
 *    This method returns a {@link org.apache.lucene5_shaded.search.TopDocs TopDocs} object,
 *    which is an internal collection of search results. The IndexSearcher creates
 *    a {@link org.apache.lucene5_shaded.search.TopScoreDocCollector TopScoreDocCollector} and
 *    passes it along with the Weight, Filter to another expert search method (for
 *    more on the {@link org.apache.lucene5_shaded.search.Collector Collector} mechanism,
 *    see {@link org.apache.lucene5_shaded.search.IndexSearcher IndexSearcher}). The TopScoreDocCollector
 *    uses a {@link org.apache.lucene5_shaded.util.PriorityQueue PriorityQueue} to collect the
 *    top results for the search.
 * <p>If a Filter is being used, some initial setup is done to determine which docs to include. 
 *    Otherwise, we ask the Weight for a {@link org.apache.lucene5_shaded.search.Scorer Scorer} for each
 *    {@link org.apache.lucene5_shaded.index.IndexReader IndexReader} segment and proceed by calling
 *    {@link org.apache.lucene5_shaded.search.BulkScorer#score(LeafCollector,org.apache.lucene5_shaded.util.Bits) BulkScorer.score(LeafCollector,Bits)}.
 * <p>At last, we are actually going to score some documents. The score method takes in the Collector
 *    (most likely the TopScoreDocCollector or TopFieldCollector) and does its business.Of course, here 
 *    is where things get involved. The {@link org.apache.lucene5_shaded.search.Scorer Scorer} that is returned
 *    by the {@link org.apache.lucene5_shaded.search.Weight Weight} object depends on what type of Query was
 *    submitted. In most real world applications with multiple query terms, the 
 *    {@link org.apache.lucene5_shaded.search.Scorer Scorer} is going to be a <code>BooleanScorer2</code> created
 *    from {@link org.apache.lucene5_shaded.search.BooleanWeight BooleanWeight} (see the section on
 *    <a href="#customQueriesExpert">custom queries</a> for info on changing this).
 * <p>Assuming a BooleanScorer2, we first initialize the Coordinator, which is used to apply the coord() 
 *   factor. We then get a internal Scorer based on the required, optional and prohibited parts of the query.
 *   Using this internal Scorer, the BooleanScorer2 then proceeds into a while loop based on the 
 *   {@link org.apache.lucene5_shaded.search.DocIdSetIterator#nextDoc DocIdSetIterator.nextDoc()} method. The nextDoc() method advances
 *   to the next document matching the query. This is an abstract method in the Scorer class and is thus 
 *   overridden by all derived  implementations. If you have a simple OR query your internal Scorer is most 
 *   likely a DisjunctionSumScorer, which essentially combines the scorers from the sub scorers of the OR'd terms.
 */
package org.apache.lucene5_shaded.search;
