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
 * This package contains the various ranking models that can be used in Lucene. The
 * abstract class {@link org.apache.lucene5_shaded.search.similarities.Similarity} serves
 * as the base for ranking functions. For searching, users can employ the models
 * already implemented or create their own by extending one of the classes in this
 * package.
 * 
 * <h2>Table Of Contents</h2>
 *     <ol>
 *         <li><a href="#sims">Summary of the Ranking Methods</a></li>
 *         <li><a href="#changingSimilarity">Changing the Similarity</a></li>
 *     </ol>
 * 
 * 
 * <a name="sims"></a>
 * <h2>Summary of the Ranking Methods</h2>
 * 
 * <p>{@link org.apache.lucene5_shaded.search.similarities.DefaultSimilarity} is the original Lucene
 * scoring function. It is based on a highly optimized 
 * <a href="http://en.wikipedia.org/wiki/Vector_Space_Model">Vector Space Model</a>. For more
 * information, see {@link org.apache.lucene5_shaded.search.similarities.TFIDFSimilarity}.
 * 
 * <p>{@link org.apache.lucene5_shaded.search.similarities.BM25Similarity} is an optimized
 * implementation of the successful Okapi BM25 model.
 * 
 * <p>{@link org.apache.lucene5_shaded.search.similarities.SimilarityBase} provides a basic
 * implementation of the Similarity contract and exposes a highly simplified
 * interface, which makes it an ideal starting point for new ranking functions.
 * Lucene ships the following methods built on
 * {@link org.apache.lucene5_shaded.search.similarities.SimilarityBase}:
 * 
 * <a name="framework"></a>
 * <ul>
 *   <li>Amati and Rijsbergen's {@linkplain org.apache.lucene5_shaded.search.similarities.DFRSimilarity DFR} framework;</li>
 *   <li>Clinchant and Gaussier's {@linkplain org.apache.lucene5_shaded.search.similarities.IBSimilarity Information-based models}
 *     for IR;</li>
 *   <li>The implementation of two {@linkplain org.apache.lucene5_shaded.search.similarities.LMSimilarity language models} from
 *   Zhai and Lafferty's paper.</li>
 *   <li>{@linkplain org.apache.lucene5_shaded.search.similarities.DFISimilarity Divergence from independence} models as described
 *   in "IRRA at TREC 2012" (Dinçer).
 *   <li>
 * </ul>
 * 
 * Since {@link org.apache.lucene5_shaded.search.similarities.SimilarityBase} is not
 * optimized to the same extent as
 * {@link org.apache.lucene5_shaded.search.similarities.DefaultSimilarity} and
 * {@link org.apache.lucene5_shaded.search.similarities.BM25Similarity}, a difference in
 * performance is to be expected when using the methods listed above. However,
 * optimizations can always be implemented in subclasses; see
 * <a href="#changingSimilarity">below</a>.
 * 
 * <a name="changingSimilarity"></a>
 * <h2>Changing Similarity</h2>
 * 
 * <p>Chances are the available Similarities are sufficient for all
 *     your searching needs.
 *     However, in some applications it may be necessary to customize your <a
 *         href="Similarity.html">Similarity</a> implementation. For instance, some
 *     applications do not need to
 *     distinguish between shorter and longer documents (see <a
 *         href="http://www.gossamer-threads.com/lists/lucene/java-user/38967#38967">a "fair" similarity</a>).
 * 
 * <p>To change {@link org.apache.lucene5_shaded.search.similarities.Similarity}, one must do so for both indexing and
 *     searching, and the changes must happen before
 *     either of these actions take place. Although in theory there is nothing stopping you from changing mid-stream, it
 *     just isn't well-defined what is going to happen.
 * 
 * <p>To make this change, implement your own {@link org.apache.lucene5_shaded.search.similarities.Similarity} (likely
 *     you'll want to simply subclass an existing method, be it
 *     {@link org.apache.lucene5_shaded.search.similarities.DefaultSimilarity} or a descendant of
 *     {@link org.apache.lucene5_shaded.search.similarities.SimilarityBase}), and
 *     then register the new class by calling
 *     {@link org.apache.lucene5_shaded.index.IndexWriterConfig#setSimilarity(Similarity)}
 *     before indexing and
 *     {@link org.apache.lucene5_shaded.search.IndexSearcher#setSimilarity(Similarity)}
 *     before searching.
 * 
 * <h3>Extending {@linkplain org.apache.lucene5_shaded.search.similarities.SimilarityBase}</h3>
 * <p>
 * The easiest way to quickly implement a new ranking method is to extend
 * {@link org.apache.lucene5_shaded.search.similarities.SimilarityBase}, which provides
 * basic implementations for the low level . Subclasses are only required to
 * implement the {@link org.apache.lucene5_shaded.search.similarities.SimilarityBase#score(BasicStats, float, float)}
 * and {@link org.apache.lucene5_shaded.search.similarities.SimilarityBase#toString()}
 * methods.
 * 
 * <p>Another option is to extend one of the <a href="#framework">frameworks</a>
 * based on {@link org.apache.lucene5_shaded.search.similarities.SimilarityBase}. These
 * Similarities are implemented modularly, e.g.
 * {@link org.apache.lucene5_shaded.search.similarities.DFRSimilarity} delegates
 * computation of the three parts of its formula to the classes
 * {@link org.apache.lucene5_shaded.search.similarities.BasicModel},
 * {@link org.apache.lucene5_shaded.search.similarities.AfterEffect} and
 * {@link org.apache.lucene5_shaded.search.similarities.Normalization}. Instead of
 * subclassing the Similarity, one can simply introduce a new basic model and tell
 * {@link org.apache.lucene5_shaded.search.similarities.DFRSimilarity} to use it.
 * 
 * <h3>Changing {@linkplain org.apache.lucene5_shaded.search.similarities.DefaultSimilarity}</h3>
 * <p>
 *     If you are interested in use cases for changing your similarity, see the Lucene users's mailing list at <a
 *         href="http://www.gossamer-threads.com/lists/lucene/java-user/39125">Overriding Similarity</a>.
 *     In summary, here are a few use cases:
 *     <ol>
 *         <li><p>The <code>SweetSpotSimilarity</code> in
 *             <code>org.apache.lucene5_shaded.misc</code> gives small
 *             increases as the frequency increases a small amount
 *             and then greater increases when you hit the "sweet spot", i.e. where
 *             you think the frequency of terms is more significant.</li>
 *         <li><p>Overriding tf &mdash; In some applications, it doesn't matter what the score of a document is as long as a
 *             matching term occurs. In these
 *             cases people have overridden Similarity to return 1 from the tf() method.</li>
 *         <li><p>Changing Length Normalization &mdash; By overriding
 *             {@link org.apache.lucene5_shaded.search.similarities.Similarity#computeNorm(org.apache.lucene5_shaded.index.FieldInvertState state)},
 *             it is possible to discount how the length of a field contributes
 *             to a score. In {@link org.apache.lucene5_shaded.search.similarities.DefaultSimilarity},
 *             lengthNorm = 1 / (numTerms in field)^0.5, but if one changes this to be
 *             1 / (numTerms in field), all fields will be treated
 *             <a href="http://www.gossamer-threads.com/lists/lucene/java-user/38967#38967">"fairly"</a>.</li>
 *     </ol>
 *     In general, Chris Hostetter sums it up best in saying (from <a
 *         href="http://www.gossamer-threads.com/lists/lucene/java-user/39125#39125">the Lucene users's mailing list</a>):
 *     <blockquote>[One would override the Similarity in] ... any situation where you know more about your data then just
 *         that
 *         it's "text" is a situation where it *might* make sense to to override your
 *         Similarity method.</blockquote>
 */
package org.apache.lucene5_shaded.search.similarities;
