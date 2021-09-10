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
package org.apache.lucene5_shaded.search.similarities;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene5_shaded.index.FieldInvertState;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.index.NumericDocValues;
import org.apache.lucene5_shaded.search.CollectionStatistics;
import org.apache.lucene5_shaded.search.Explanation;
import org.apache.lucene5_shaded.search.IndexSearcher;
import org.apache.lucene5_shaded.search.PhraseQuery;
import org.apache.lucene5_shaded.search.TermStatistics;
import org.apache.lucene5_shaded.util.BytesRef;


/**
 * Implementation of {@link Similarity} with the Vector Space Model.
 * <p>
 * Expert: Scoring API.
 * <p>TFIDFSimilarity defines the components of Lucene scoring.
 * Overriding computation of these components is a convenient
 * way to alter Lucene scoring.
 *
 * <p>Suggested reading:
 * <a href="http://nlp.stanford.edu/IR-book/html/htmledition/queries-as-vectors-1.html">
 * Introduction To Information Retrieval, Chapter 6</a>.
 *
 * <p>The following describes how Lucene scoring evolves from
 * underlying information retrieval models to (efficient) implementation.
 * We first brief on <i>VSM Score</i>, 
 * then derive from it <i>Lucene's Conceptual Scoring Formula</i>,
 * from which, finally, evolves <i>Lucene's Practical Scoring Function</i> 
 * (the latter is connected directly with Lucene classes and methods).    
 *
 * <p>Lucene combines
 * <a href="http://en.wikipedia.org/wiki/Standard_Boolean_model">
 * Boolean model (BM) of Information Retrieval</a>
 * with
 * <a href="http://en.wikipedia.org/wiki/Vector_Space_Model">
 * Vector Space Model (VSM) of Information Retrieval</a> -
 * documents "approved" by BM are scored by VSM.
 *
 * <p>In VSM, documents and queries are represented as
 * weighted vectors in a multi-dimensional space,
 * where each distinct index term is a dimension,
 * and weights are
 * <a href="http://en.wikipedia.org/wiki/Tfidf">Tf-idf</a> values.
 *
 * <p>VSM does not require weights to be <i>Tf-idf</i> values,
 * but <i>Tf-idf</i> values are believed to produce search results of high quality,
 * and so Lucene is using <i>Tf-idf</i>.
 * <i>Tf</i> and <i>Idf</i> are described in more detail below,
 * but for now, for completion, let's just say that
 * for given term <i>t</i> and document (or query) <i>x</i>,
 * <i>Tf(t,x)</i> varies with the number of occurrences of term <i>t</i> in <i>x</i>
 * (when one increases so does the other) and
 * <i>idf(t)</i> similarly varies with the inverse of the
 * number of index documents containing term <i>t</i>.
 *
 * <p><i>VSM score</i> of document <i>d</i> for query <i>q</i> is the
 * <a href="http://en.wikipedia.org/wiki/Cosine_similarity">
 * Cosine Similarity</a>
 * of the weighted query vectors <i>V(q)</i> and <i>V(d)</i>:
 *
 *  <br>&nbsp;<br>
 *  <table cellpadding="2" cellspacing="2" border="0" style="width:auto; margin-left:auto; margin-right:auto" summary="formatting only">
 *    <tr><td>
 *    <table cellpadding="1" cellspacing="0" border="1" style="margin-left:auto; margin-right:auto" summary="formatting only">
 *      <tr><td>
 *      <table cellpadding="2" cellspacing="2" border="0" style="margin-left:auto; margin-right:auto" summary="cosine similarity formula">
 *        <tr>
 *          <td valign="middle" align="right" rowspan="1">
 *            cosine-similarity(q,d) &nbsp; = &nbsp;
 *          </td>
 *          <td valign="middle" align="center">
 *            <table summary="cosine similarity formula">
 *               <tr><td align="center" style="text-align: center"><small>V(q)&nbsp;&middot;&nbsp;V(d)</small></td></tr>
 *               <tr><td align="center" style="text-align: center">&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;</td></tr>
 *               <tr><td align="center" style="text-align: center"><small>|V(q)|&nbsp;|V(d)|</small></td></tr>
 *            </table>
 *          </td>
 *        </tr>
 *      </table>
 *      </td></tr>
 *    </table>
 *    </td></tr>
 *    <tr><td>
 *    <center><u>VSM Score</u></center>
 *    </td></tr>
 *  </table>
 *  <br>&nbsp;<br>
 *   
 *
 * Where <i>V(q)</i> &middot; <i>V(d)</i> is the
 * <a href="http://en.wikipedia.org/wiki/Dot_product">dot product</a>
 * of the weighted vectors,
 * and <i>|V(q)|</i> and <i>|V(d)|</i> are their
 * <a href="http://en.wikipedia.org/wiki/Euclidean_norm#Euclidean_norm">Euclidean norms</a>.
 *
 * <p>Note: the above equation can be viewed as the dot product of
 * the normalized weighted vectors, in the sense that dividing
 * <i>V(q)</i> by its euclidean norm is normalizing it to a unit vector.
 *
 * <p>Lucene refines <i>VSM score</i> for both search quality and usability:
 * <ul>
 *  <li>Normalizing <i>V(d)</i> to the unit vector is known to be problematic in that 
 *  it removes all document length information. 
 *  For some documents removing this info is probably ok, 
 *  e.g. a document made by duplicating a certain paragraph <i>10</i> times,
 *  especially if that paragraph is made of distinct terms. 
 *  But for a document which contains no duplicated paragraphs, 
 *  this might be wrong. 
 *  To avoid this problem, a different document length normalization 
 *  factor is used, which normalizes to a vector equal to or larger 
 *  than the unit vector: <i>doc-len-norm(d)</i>.
 *  </li>
 *
 *  <li>At indexing, users can specify that certain documents are more
 *  important than others, by assigning a document boost.
 *  For this, the score of each document is also multiplied by its boost value
 *  <i>doc-boost(d)</i>.
 *  </li>
 *
 *  <li>Lucene is field based, hence each query term applies to a single
 *  field, document length normalization is by the length of the certain field,
 *  and in addition to document boost there are also document fields boosts.
 *  </li>
 *
 *  <li>The same field can be added to a document during indexing several times,
 *  and so the boost of that field is the multiplication of the boosts of
 *  the separate additions (or parts) of that field within the document.
 *  </li>
 *
 *  <li>At search time users can specify boosts to each query, sub-query, and
 *  each query term, hence the contribution of a query term to the score of
 *  a document is multiplied by the boost of that query term <i>query-boost(q)</i>.
 *  </li>
 *
 *  <li>A document may match a multi term query without containing all
 *  the terms of that query (this is correct for some of the queries),
 *  and users can further reward documents matching more query terms
 *  through a coordination factor, which is usually larger when
 *  more terms are matched: <i>coord-factor(q,d)</i>.
 *  </li>
 * </ul>
 *
 * <p>Under the simplifying assumption of a single field in the index,
 * we get <i>Lucene's Conceptual scoring formula</i>:
 *
 *  <br>&nbsp;<br>
 *  <table cellpadding="2" cellspacing="2" border="0" style="width:auto; margin-left:auto; margin-right:auto" summary="formatting only">
 *    <tr><td>
 *    <table cellpadding="1" cellspacing="0" border="1" style="margin-left:auto; margin-right:auto" summary="formatting only">
 *      <tr><td>
 *      <table cellpadding="2" cellspacing="2" border="0" style="margin-left:auto; margin-right:auto" summary="formatting only">
 *        <tr>
 *          <td valign="middle" align="right" rowspan="1">
 *            score(q,d) &nbsp; = &nbsp;
 *            <span style="color: #FF9933">coord-factor(q,d)</span> &middot; &nbsp;
 *            <span style="color: #CCCC00">query-boost(q)</span> &middot; &nbsp;
 *          </td>
 *          <td valign="middle" align="center">
 *            <table summary="Lucene conceptual scoring formula">
 *               <tr><td align="center" style="text-align: center"><small><span style="color: #993399">V(q)&nbsp;&middot;&nbsp;V(d)</span></small></td></tr>
 *               <tr><td align="center" style="text-align: center">&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;</td></tr>
 *               <tr><td align="center" style="text-align: center"><small><span style="color: #FF33CC">|V(q)|</span></small></td></tr>
 *            </table>
 *          </td>
 *          <td valign="middle" align="right" rowspan="1">
 *            &nbsp; &middot; &nbsp; <span style="color: #3399FF">doc-len-norm(d)</span>
 *            &nbsp; &middot; &nbsp; <span style="color: #3399FF">doc-boost(d)</span>
 *          </td>
 *        </tr>
 *      </table>
 *      </td></tr>
 *    </table>
 *    </td></tr>
 *    <tr><td>
 *    <center><u>Lucene Conceptual Scoring Formula</u></center>
 *    </td></tr>
 *  </table>
 *  <br>&nbsp;<br>
 *
 * <p>The conceptual formula is a simplification in the sense that (1) terms and documents
 * are fielded and (2) boosts are usually per query term rather than per query.
 *
 * <p>We now describe how Lucene implements this conceptual scoring formula, and
 * derive from it <i>Lucene's Practical Scoring Function</i>.
 *  
 * <p>For efficient score computation some scoring components
 * are computed and aggregated in advance:
 *
 * <ul>
 *  <li><i>Query-boost</i> for the query (actually for each query term)
 *  is known when search starts.
 *  </li>
 *
 *  <li>Query Euclidean norm <i>|V(q)|</i> can be computed when search starts,
 *  as it is independent of the document being scored.
 *  From search optimization perspective, it is a valid question
 *  why bother to normalize the query at all, because all
 *  scored documents will be multiplied by the same <i>|V(q)|</i>,
 *  and hence documents ranks (their order by score) will not
 *  be affected by this normalization.
 *  There are two good reasons to keep this normalization:
 *  <ul>
 *   <li>Recall that
 *   <a href="http://en.wikipedia.org/wiki/Cosine_similarity">
 *   Cosine Similarity</a> can be used find how similar
 *   two documents are. One can use Lucene for e.g.
 *   clustering, and use a document as a query to compute
 *   its similarity to other documents.
 *   In this use case it is important that the score of document <i>d3</i>
 *   for query <i>d1</i> is comparable to the score of document <i>d3</i>
 *   for query <i>d2</i>. In other words, scores of a document for two
 *   distinct queries should be comparable.
 *   There are other applications that may require this.
 *   And this is exactly what normalizing the query vector <i>V(q)</i>
 *   provides: comparability (to a certain extent) of two or more queries.
 *   </li>
 *
 *   <li>Applying query normalization on the scores helps to keep the
 *   scores around the unit vector, hence preventing loss of score data
 *   because of floating point precision limitations.
 *   </li>
 *  </ul>
 *  </li>
 *
 *  <li>Document length norm <i>doc-len-norm(d)</i> and document
 *  boost <i>doc-boost(d)</i> are known at indexing time.
 *  They are computed in advance and their multiplication
 *  is saved as a single value in the index: <i>norm(d)</i>.
 *  (In the equations below, <i>norm(t in d)</i> means <i>norm(field(t) in doc d)</i>
 *  where <i>field(t)</i> is the field associated with term <i>t</i>.)
 *  </li>
 * </ul>
 *
 * <p><i>Lucene's Practical Scoring Function</i> is derived from the above.
 * The color codes demonstrate how it relates
 * to those of the <i>conceptual</i> formula:
 *
 * <table cellpadding="2" cellspacing="2" border="0" style="width:auto; margin-left:auto; margin-right:auto" summary="formatting only">
 *  <tr><td>
 *  <table cellpadding="" cellspacing="2" border="2" style="margin-left:auto; margin-right:auto" summary="formatting only">
 *  <tr><td>
 *   <table cellpadding="2" cellspacing="2" border="0" style="margin-left:auto; margin-right:auto" summary="Lucene conceptual scoring formula">
 *   <tr>
 *     <td valign="middle" align="right" rowspan="1">
 *       score(q,d) &nbsp; = &nbsp;
 *       <A HREF="#formula_coord"><span style="color: #FF9933">coord(q,d)</span></A> &nbsp;&middot;&nbsp;
 *       <A HREF="#formula_queryNorm"><span style="color: #FF33CC">queryNorm(q)</span></A> &nbsp;&middot;&nbsp;
 *     </td>
 *     <td valign="bottom" align="center" rowspan="1" style="text-align: center">
 *       <big><big><big>&sum;</big></big></big>
 *     </td>
 *     <td valign="middle" align="right" rowspan="1">
 *       <big><big>(</big></big>
 *       <A HREF="#formula_tf"><span style="color: #993399">tf(t in d)</span></A> &nbsp;&middot;&nbsp;
 *       <A HREF="#formula_idf"><span style="color: #993399">idf(t)</span></A><sup>2</sup> &nbsp;&middot;&nbsp;
 *       <A HREF="#formula_termBoost"><span style="color: #CCCC00">t.getBoost()</span></A>&nbsp;&middot;&nbsp;
 *       <A HREF="#formula_norm"><span style="color: #3399FF">norm(t,d)</span></A>
 *       <big><big>)</big></big>
 *     </td>
 *   </tr>
 *   <tr valign="top">
 *    <td></td>
 *    <td align="center" style="text-align: center"><small>t in q</small></td>
 *    <td></td>
 *   </tr>
 *   </table>
 *  </td></tr>
 *  </table>
 * </td></tr>
 * <tr><td>
 *  <center><u>Lucene Practical Scoring Function</u></center>
 * </td></tr>
 * </table>
 *
 * <p> where
 * <ol>
 *    <li>
 *      <A NAME="formula_tf"></A>
 *      <b><i>tf(t in d)</i></b>
 *      correlates to the term's <i>frequency</i>,
 *      defined as the number of times term <i>t</i> appears in the currently scored document <i>d</i>.
 *      Documents that have more occurrences of a given term receive a higher score.
 *      Note that <i>tf(t in q)</i> is assumed to be <i>1</i> and therefore it does not appear in this equation,
 *      However if a query contains twice the same term, there will be
 *      two term-queries with that same term and hence the computation would still be correct (although
 *      not very efficient).
 *      The default computation for <i>tf(t in d)</i> in
 *      {@link DefaultSimilarity#tf(float) DefaultSimilarity} is:
 *
 *      <br>&nbsp;<br>
 *      <table cellpadding="2" cellspacing="2" border="0" style="width:auto; margin-left:auto; margin-right:auto" summary="term frequency computation">
 *        <tr>
 *          <td valign="middle" align="right" rowspan="1">
 *            {@link DefaultSimilarity#tf(float) tf(t in d)} &nbsp; = &nbsp;
 *          </td>
 *          <td valign="top" align="center" rowspan="1">
 *               frequency<sup><big>&frac12;</big></sup>
 *          </td>
 *        </tr>
 *      </table>
 *      <br>&nbsp;<br>
 *    </li>
 *
 *    <li>
 *      <A NAME="formula_idf"></A>
 *      <b><i>idf(t)</i></b> stands for Inverse Document Frequency. This value
 *      correlates to the inverse of <i>docFreq</i>
 *      (the number of documents in which the term <i>t</i> appears).
 *      This means rarer terms give higher contribution to the total score.
 *      <i>idf(t)</i> appears for <i>t</i> in both the query and the document,
 *      hence it is squared in the equation.
 *      The default computation for <i>idf(t)</i> in
 *      {@link DefaultSimilarity#idf(long, long) DefaultSimilarity} is:
 *
 *      <br>&nbsp;<br>
 *      <table cellpadding="2" cellspacing="2" border="0" style="width:auto; margin-left:auto; margin-right:auto" summary="inverse document frequency computation">
 *        <tr>
 *          <td valign="middle" align="right">
 *            {@link DefaultSimilarity#idf(long, long) idf(t)}&nbsp; = &nbsp;
 *          </td>
 *          <td valign="middle" align="center">
 *            1 + log <big>(</big>
 *          </td>
 *          <td valign="middle" align="center">
 *            <table summary="inverse document frequency computation">
 *               <tr><td align="center" style="text-align: center"><small>numDocs</small></td></tr>
 *               <tr><td align="center" style="text-align: center">&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;</td></tr>
 *               <tr><td align="center" style="text-align: center"><small>docFreq+1</small></td></tr>
 *            </table>
 *          </td>
 *          <td valign="middle" align="center">
 *            <big>)</big>
 *          </td>
 *        </tr>
 *      </table>
 *      <br>&nbsp;<br>
 *    </li>
 *
 *    <li>
 *      <A NAME="formula_coord"></A>
 *      <b><i>coord(q,d)</i></b>
 *      is a score factor based on how many of the query terms are found in the specified document.
 *      Typically, a document that contains more of the query's terms will receive a higher score
 *      than another document with fewer query terms.
 *      This is a search time factor computed in
 *      {@link #coord(int, int) coord(q,d)}
 *      by the Similarity in effect at search time.
 *      <br>&nbsp;<br>
 *    </li>
 *
 *    <li><b>
 *      <A NAME="formula_queryNorm"></A>
 *      <i>queryNorm(q)</i>
 *      </b>
 *      is a normalizing factor used to make scores between queries comparable.
 *      This factor does not affect document ranking (since all ranked documents are multiplied by the same factor),
 *      but rather just attempts to make scores from different queries (or even different indexes) comparable.
 *      This is a search time factor computed by the Similarity in effect at search time.
 *
 *      The default computation in
 *      {@link DefaultSimilarity#queryNorm(float) DefaultSimilarity}
 *      produces a <a href="http://en.wikipedia.org/wiki/Euclidean_norm#Euclidean_norm">Euclidean norm</a>:
 *      <br>&nbsp;<br>
 *      <table cellpadding="1" cellspacing="0" border="0" style="width:auto; margin-left:auto; margin-right:auto" summary="query normalization computation">
 *        <tr>
 *          <td valign="middle" align="right" rowspan="1">
 *            queryNorm(q)  &nbsp; = &nbsp;
 *            {@link DefaultSimilarity#queryNorm(float) queryNorm(sumOfSquaredWeights)}
 *            &nbsp; = &nbsp;
 *          </td>
 *          <td valign="middle" align="center" rowspan="1">
 *            <table summary="query normalization computation">
 *               <tr><td align="center" style="text-align: center"><big>1</big></td></tr>
 *               <tr><td align="center" style="text-align: center"><big>
 *                  &ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;
 *               </big></td></tr>
 *               <tr><td align="center" style="text-align: center">sumOfSquaredWeights<sup><big>&frac12;</big></sup></td></tr>
 *            </table>
 *          </td>
 *        </tr>
 *      </table>
 *      <br>&nbsp;<br>
 *
 *      The sum of squared weights (of the query terms) is
 *      computed by the query {@link org.apache.lucene5_shaded.search.Weight} object.
 *      For example, a {@link org.apache.lucene5_shaded.search.BooleanQuery}
 *      computes this value as:
 *
 *      <br>&nbsp;<br>
 *      <table cellpadding="1" cellspacing="0" border="0" style="width:auto; margin-left:auto; margin-right:auto" summary="sum of squared weights computation">
 *        <tr>
 *          <td valign="middle" align="right" rowspan="1">
 *            {@link org.apache.lucene5_shaded.search.Weight#getValueForNormalization() sumOfSquaredWeights} &nbsp; = &nbsp;
 *            {@link org.apache.lucene5_shaded.search.BoostQuery#getBoost() q.getBoost()} <sup><big>2</big></sup>
 *            &nbsp;&middot;&nbsp;
 *          </td>
 *          <td valign="bottom" align="center" rowspan="1" style="text-align: center">
 *            <big><big><big>&sum;</big></big></big>
 *          </td>
 *          <td valign="middle" align="right" rowspan="1">
 *            <big><big>(</big></big>
 *            <A HREF="#formula_idf">idf(t)</A> &nbsp;&middot;&nbsp;
 *            <A HREF="#formula_termBoost">t.getBoost()</A>
 *            <big><big>) <sup>2</sup> </big></big>
 *          </td>
 *        </tr>
 *        <tr valign="top">
 *          <td></td>
 *          <td align="center" style="text-align: center"><small>t in q</small></td>
 *          <td></td>
 *        </tr>
 *      </table>
 *      <br>&nbsp;<br>
 *
 *    </li>
 *
 *    <li>
 *      <A NAME="formula_termBoost"></A>
 *      <b><i>t.getBoost()</i></b>
 *      is a search time boost of term <i>t</i> in the query <i>q</i> as
 *      specified in the query text
 *      (see <A HREF="{@docRoot}/../queryparser/org/apache/lucene5_shaded/queryparser/classic/package-summary.html#Boosting_a_Term">query syntax</A>),
 *      or as set by wrapping with
 *      {@link org.apache.lucene5_shaded.search.BoostQuery#BoostQuery(org.apache.lucene5_shaded.search.Query, float) BoostQuery}.
 *      Notice that there is really no direct API for accessing a boost of one term in a multi term query,
 *      but rather multi terms are represented in a query as multi
 *      {@link org.apache.lucene5_shaded.search.TermQuery TermQuery} objects,
 *      and so the boost of a term in the query is accessible by calling the sub-query
 *      {@link org.apache.lucene5_shaded.search.BoostQuery#getBoost() getBoost()}.
 *      <br>&nbsp;<br>
 *    </li>
 *
 *    <li>
 *      <A NAME="formula_norm"></A>
 *      <b><i>norm(t,d)</i></b> encapsulates a few (indexing time) boost and length factors:
 *
 *      <ul>
 *        <li><b>Field boost</b> - set by calling
 *        {@link org.apache.lucene5_shaded.document.Field#setBoost(float) field.setBoost()}
 *        before adding the field to a document.
 *        </li>
 *        <li><b>lengthNorm</b> - computed
 *        when the document is added to the index in accordance with the number of tokens
 *        of this field in the document, so that shorter fields contribute more to the score.
 *        LengthNorm is computed by the Similarity class in effect at indexing.
 *        </li>
 *      </ul>
 *      The {@link #computeNorm} method is responsible for
 *      combining all of these factors into a single float.
 *
 *      <p>
 *      When a document is added to the index, all the above factors are multiplied.
 *      If the document has multiple fields with the same name, all their boosts are multiplied together:
 *
 *      <br>&nbsp;<br>
 *      <table cellpadding="1" cellspacing="0" border="0" style="width:auto; margin-left:auto; margin-right:auto" summary="index-time normalization">
 *        <tr>
 *          <td valign="middle" align="right" rowspan="1">
 *            norm(t,d) &nbsp; = &nbsp;
 *            lengthNorm
 *            &nbsp;&middot;&nbsp;
 *          </td>
 *          <td valign="bottom" align="center" rowspan="1" style="text-align: center">
 *            <big><big><big>&prod;</big></big></big>
 *          </td>
 *          <td valign="middle" align="right" rowspan="1">
 *            {@link org.apache.lucene5_shaded.index.IndexableField#boost() f.boost}()
 *          </td>
 *        </tr>
 *        <tr valign="top">
 *          <td></td>
 *          <td align="center" style="text-align: center"><small>field <i><b>f</b></i> in <i>d</i> named as <i><b>t</b></i></small></td>
 *          <td></td>
 *        </tr>
 *      </table>
 *      Note that search time is too late to modify this <i>norm</i> part of scoring, 
 *      e.g. by using a different {@link Similarity} for search.
 *    </li>
 * </ol>
 *
 * @see org.apache.lucene5_shaded.index.IndexWriterConfig#setSimilarity(Similarity)
 * @see IndexSearcher#setSimilarity(Similarity)
 */
public abstract class TFIDFSimilarity extends Similarity {
  
  /**
   * Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.)
   */
  public TFIDFSimilarity() {}
  
  /** Computes a score factor based on the fraction of all query terms that a
   * document contains.  This value is multiplied into scores.
   *
   * <p>The presence of a large portion of the query terms indicates a better
   * match with the query, so implementations of this method usually return
   * larger values when the ratio between these parameters is large and smaller
   * values when the ratio between them is small.
   *
   * @param overlap the number of query terms matched in the document
   * @param maxOverlap the total number of terms in the query
   * @return a score factor based on term overlap with the query
   */
  @Override
  public abstract float coord(int overlap, int maxOverlap);
  
  /** Computes the normalization value for a query given the sum of the squared
   * weights of each of the query terms.  This value is multiplied into the
   * weight of each query term. While the classic query normalization factor is
   * computed as 1/sqrt(sumOfSquaredWeights), other implementations might
   * completely ignore sumOfSquaredWeights (ie return 1).
   *
   * <p>This does not affect ranking, but the default implementation does make scores
   * from different queries more comparable than they would be by eliminating the
   * magnitude of the Query vector as a factor in the score.
   *
   * @param sumOfSquaredWeights the sum of the squares of query term weights
   * @return a normalization factor for query weights
   */
  @Override
  public abstract float queryNorm(float sumOfSquaredWeights);
  
  /** Computes a score factor based on a term or phrase's frequency in a
   * document.  This value is multiplied by the {@link #idf(long, long)}
   * factor for each term in the query and these products are then summed to
   * form the initial score for a document.
   *
   * <p>Terms and phrases repeated in a document indicate the topic of the
   * document, so implementations of this method usually return larger values
   * when <code>freq</code> is large, and smaller values when <code>freq</code>
   * is small.
   *
   * @param freq the frequency of a term within a document
   * @return a score factor based on a term's within-document frequency
   */
  public abstract float tf(float freq);

  /**
   * Computes a score factor for a simple term and returns an explanation
   * for that score factor.
   * 
   * <p>
   * The default implementation uses:
   * 
   * <pre class="prettyprint">
   * idf(docFreq, searcher.maxDoc());
   * </pre>
   * 
   * Note that {@link CollectionStatistics#maxDoc()} is used instead of
   * {@link org.apache.lucene5_shaded.index.IndexReader#numDocs() IndexReader#numDocs()} because also
   * {@link TermStatistics#docFreq()} is used, and when the latter 
   * is inaccurate, so is {@link CollectionStatistics#maxDoc()}, and in the same direction.
   * In addition, {@link CollectionStatistics#maxDoc()} is more efficient to compute
   *   
   * @param collectionStats collection-level statistics
   * @param termStats term-level statistics for the term
   * @return an Explain object that includes both an idf score factor 
             and an explanation for the term.
   */
  public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics termStats) {
    final long df = termStats.docFreq();
    final long max = collectionStats.maxDoc();
    final float idf = idf(df, max);
    return Explanation.match(idf, "idf(docFreq=" + df + ", maxDocs=" + max + ")");
  }

  /**
   * Computes a score factor for a phrase.
   * 
   * <p>
   * The default implementation sums the idf factor for
   * each term in the phrase.
   * 
   * @param collectionStats collection-level statistics
   * @param termStats term-level statistics for the terms in the phrase
   * @return an Explain object that includes both an idf 
   *         score factor for the phrase and an explanation 
   *         for each term.
   */
  public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics termStats[]) {
    final long max = collectionStats.maxDoc();
    float idf = 0.0f;
    List<Explanation> subs = new ArrayList<>();
    for (final TermStatistics stat : termStats ) {
      final long df = stat.docFreq();
      final float termIdf = idf(df, max);
      subs.add(Explanation.match(termIdf, "idf(docFreq=" + df + ", maxDocs=" + max + ")"));
      idf += termIdf;
    }
    return Explanation.match(idf, "idf(), sum of:", subs);
  }

  /** Computes a score factor based on a term's document frequency (the number
   * of documents which contain the term).  This value is multiplied by the
   * {@link #tf(float)} factor for each term in the query and these products are
   * then summed to form the initial score for a document.
   *
   * <p>Terms that occur in fewer documents are better indicators of topic, so
   * implementations of this method usually return larger values for rare terms,
   * and smaller values for common terms.
   *
   * @param docFreq the number of documents which contain the term
   * @param numDocs the total number of documents in the collection
   * @return a score factor based on the term's document frequency
   */
  public abstract float idf(long docFreq, long numDocs);

  /**
   * Compute an index-time normalization value for this field instance.
   * <p>
   * This value will be stored in a single byte lossy representation by 
   * {@link #encodeNormValue(float)}.
   * 
   * @param state statistics of the current field (such as length, boost, etc)
   * @return an index-time normalization value
   */
  public abstract float lengthNorm(FieldInvertState state);
  
  @Override
  public final long computeNorm(FieldInvertState state) {
    float normValue = lengthNorm(state);
    return encodeNormValue(normValue);
  }
  
  /**
   * Decodes a normalization factor stored in an index.
   * 
   * @see #encodeNormValue(float)
   */
  public abstract float decodeNormValue(long norm);

  /** Encodes a normalization factor for storage in an index. */
  public abstract long encodeNormValue(float f);
 
  /** Computes the amount of a sloppy phrase match, based on an edit distance.
   * This value is summed for each sloppy phrase match in a document to form
   * the frequency to be used in scoring instead of the exact term count.
   *
   * <p>A phrase match with a small edit distance to a document passage more
   * closely matches the document, so implementations of this method usually
   * return larger values when the edit distance is small and smaller values
   * when it is large.
   *
   * @see PhraseQuery#getSlop()
   * @param distance the edit distance of this sloppy phrase match
   * @return the frequency increment for this match
   */
  public abstract float sloppyFreq(int distance);

  /**
   * Calculate a scoring factor based on the data in the payload.  Implementations
   * are responsible for interpreting what is in the payload.  Lucene makes no assumptions about
   * what is in the byte array.
   *
   * @param doc The docId currently being scored.
   * @param start The start position of the payload
   * @param end The end position of the payload
   * @param payload The payload byte array to be scored
   * @return An implementation dependent float to be used as a scoring factor
   */
  public abstract float scorePayload(int doc, int start, int end, BytesRef payload);

  @Override
  public final SimWeight computeWeight(CollectionStatistics collectionStats, TermStatistics... termStats) {
    final Explanation idf = termStats.length == 1
    ? idfExplain(collectionStats, termStats[0])
    : idfExplain(collectionStats, termStats);
    return new IDFStats(collectionStats.field(), idf);
  }

  @Override
  public final SimScorer simScorer(SimWeight stats, LeafReaderContext context) throws IOException {
    IDFStats idfstats = (IDFStats) stats;
    return new TFIDFSimScorer(idfstats, context.reader().getNormValues(idfstats.field));
  }
  
  private final class TFIDFSimScorer extends SimScorer {
    private final IDFStats stats;
    private final float weightValue;
    private final NumericDocValues norms;
    
    TFIDFSimScorer(IDFStats stats, NumericDocValues norms) throws IOException {
      this.stats = stats;
      this.weightValue = stats.value;
      this.norms = norms;
    }
    
    @Override
    public float score(int doc, float freq) {
      final float raw = tf(freq) * weightValue; // compute tf(f)*weight
      
      return norms == null ? raw : raw * decodeNormValue(norms.get(doc));  // normalize for field
    }
    
    @Override
    public float computeSlopFactor(int distance) {
      return sloppyFreq(distance);
    }

    @Override
    public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
      return scorePayload(doc, start, end, payload);
    }

    @Override
    public Explanation explain(int doc, Explanation freq) {
      return explainScore(doc, freq, stats, norms);
    }
  }
  
  /** Collection statistics for the TF-IDF model. The only statistic of interest
   * to this model is idf. */
  private static class IDFStats extends SimWeight {
    private final String field;
    /** The idf and its explanation */
    private final Explanation idf;
    private float queryNorm;
    private float boost;
    private float queryWeight;
    private float value;
    
    public IDFStats(String field, Explanation idf) {
      // TODO: Validate?
      this.field = field;
      this.idf = idf;
      normalize(1f, 1f);
    }

    @Override
    public float getValueForNormalization() {
      // TODO: (sorta LUCENE-1907) make non-static class and expose this squaring via a nice method to subclasses?
      return queryWeight * queryWeight;  // sum of squared weights
    }

    @Override
    public void normalize(float queryNorm, float boost) {
      this.boost = boost;
      this.queryNorm = queryNorm;
      queryWeight = queryNorm * boost * idf.getValue();
      value = queryWeight * idf.getValue();         // idf for document
    }
  }  

  private Explanation explainQuery(IDFStats stats) {
    List<Explanation> subs = new ArrayList<>();

    Explanation boostExpl = Explanation.match(stats.boost, "boost");
    if (stats.boost != 1.0f)
      subs.add(boostExpl);
    subs.add(stats.idf);

    Explanation queryNormExpl = Explanation.match(stats.queryNorm,"queryNorm");
    subs.add(queryNormExpl);

    return Explanation.match(
        boostExpl.getValue() * stats.idf.getValue() * queryNormExpl.getValue(),
        "queryWeight, product of:", subs);
  }

  private Explanation explainField(int doc, Explanation freq, IDFStats stats, NumericDocValues norms) {
    Explanation tfExplanation = Explanation.match(tf(freq.getValue()), "tf(freq="+freq.getValue()+"), with freq of:", freq);
    Explanation fieldNormExpl = Explanation.match(
        norms != null ? decodeNormValue(norms.get(doc)) : 1.0f,
        "fieldNorm(doc=" + doc + ")");

    return Explanation.match(
        tfExplanation.getValue() * stats.idf.getValue() * fieldNormExpl.getValue(),
        "fieldWeight in " + doc + ", product of:",
        tfExplanation, stats.idf, fieldNormExpl);
  }

  private Explanation explainScore(int doc, Explanation freq, IDFStats stats, NumericDocValues norms) {
    Explanation queryExpl = explainQuery(stats);
    Explanation fieldExpl = explainField(doc, freq, stats, norms);
    if (queryExpl.getValue() == 1f) {
      return fieldExpl;
    }
    return Explanation.match(
        queryExpl.getValue() * fieldExpl.getValue(),
        "score(doc="+doc+",freq="+freq.getValue()+"), product of:",
        queryExpl, fieldExpl);
  }
}
