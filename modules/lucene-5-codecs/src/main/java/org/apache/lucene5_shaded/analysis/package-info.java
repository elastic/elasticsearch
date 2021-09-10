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
 * Text analysis. 
 * <p>API and code to convert text into indexable/searchable tokens.  Covers {@link org.apache.lucene5_shaded.analysis.Analyzer} and related classes.</p>
 * <h2>Parsing? Tokenization? Analysis!</h2>
 * <p>
 * Lucene, an indexing and search library, accepts only plain text input.
 * <h2>Parsing</h2>
 * <p>
 * Applications that build their search capabilities upon Lucene may support documents in various formats &ndash; HTML, XML, PDF, Word &ndash; just to name a few.
 * Lucene does not care about the <i>Parsing</i> of these and other document formats, and it is the responsibility of the 
 * application using Lucene to use an appropriate <i>Parser</i> to convert the original format into plain text before passing that plain text to Lucene.
 * <h2>Tokenization</h2>
 * <p>
 * Plain text passed to Lucene for indexing goes through a process generally called tokenization. Tokenization is the process
 * of breaking input text into small indexing elements &ndash; tokens.
 * The way input text is broken into tokens heavily influences how people will then be able to search for that text. 
 * For instance, sentences beginnings and endings can be identified to provide for more accurate phrase 
 * and proximity searches (though sentence identification is not provided by Lucene).
 * <p>
 *   In some cases simply breaking the input text into tokens is not enough
 *   &ndash; a deeper <i>Analysis</i> may be needed. Lucene includes both
 *   pre- and post-tokenization analysis facilities.
 * </p>
 * <p>
 *   Pre-tokenization analysis can include (but is not limited to) stripping
 *   HTML markup, and transforming or removing text matching arbitrary patterns
 *   or sets of fixed strings.
 * </p>
 * <p>
 *   There are many post-tokenization steps that can be done, including 
 *   (but not limited to):
 * </p>
 * <ul>
 *   <li><a href="http://en.wikipedia.org/wiki/Stemming">Stemming</a> &ndash; 
 *       Replacing words with their stems. 
 *       For instance with English stemming "bikes" is replaced with "bike"; 
 *       now query "bike" can find both documents containing "bike" and those containing "bikes".
 *   </li>
 *   <li><a href="http://en.wikipedia.org/wiki/Stop_words">Stop Words Filtering</a> &ndash; 
 *       Common words like "the", "and" and "a" rarely add any value to a search.
 *       Removing them shrinks the index size and increases performance.
 *       It may also reduce some "noise" and actually improve search quality.
 *   </li>
 *   <li><a href="http://en.wikipedia.org/wiki/Text_normalization">Text Normalization</a> &ndash; 
 *       Stripping accents and other character markings can make for better searching.
 *   </li>
 *   <li><a href="http://en.wikipedia.org/wiki/Synonym">Synonym Expansion</a> &ndash; 
 *       Adding in synonyms at the same token position as the current word can mean better 
 *       matching when users search with words in the synonym set.
 *   </li>
 * </ul>
 * <h2>Core Analysis</h2>
 * <p>
 *   The analysis package provides the mechanism to convert Strings and Readers
 *   into tokens that can be indexed by Lucene.  There are four main classes in 
 *   the package from which all analysis processes are derived.  These are:
 * </p>
 * <ul>
 *   <li>
 *     {@link org.apache.lucene5_shaded.analysis.Analyzer} &ndash; An <code>Analyzer</code> is
 *     responsible for supplying a
 *     {@link org.apache.lucene5_shaded.analysis.TokenStream} which can be consumed
 *     by the indexing and searching processes.  See below for more information
 *     on implementing your own {@link org.apache.lucene5_shaded.analysis.Analyzer}. Most of the time, you can use
 *     an anonymous subclass of {@link org.apache.lucene5_shaded.analysis.Analyzer}.
 *   </li>
 *   <li>
 *     {@link org.apache.lucene5_shaded.analysis.CharFilter} &ndash; <code>CharFilter</code> extends
 *     {@link java.io.Reader} to transform the text before it is
 *     tokenized, while providing
 *     corrected character offsets to account for these modifications.  This
 *     capability allows highlighting to function over the original text when 
 *     indexed tokens are created from <code>CharFilter</code>-modified text with offsets
 *     that are not the same as those in the original text. {@link org.apache.lucene5_shaded.analysis.Tokenizer#setReader(java.io.Reader)}
 *     accept <code>CharFilter</code>s.  <code>CharFilter</code>s may
 *     be chained to perform multiple pre-tokenization modifications.
 *   </li>
 *   <li>
 *     {@link org.apache.lucene5_shaded.analysis.Tokenizer} &ndash; A <code>Tokenizer</code> is a
 *     {@link org.apache.lucene5_shaded.analysis.TokenStream} and is responsible for
 *     breaking up incoming text into tokens. In many cases, an {@link org.apache.lucene5_shaded.analysis.Analyzer} will
 *     use a {@link org.apache.lucene5_shaded.analysis.Tokenizer} as the first step in the analysis process.  However,
 *     to modify text prior to tokenization, use a {@link org.apache.lucene5_shaded.analysis.CharFilter} subclass (see
 *     above).
 *   </li>
 *   <li>
 *     {@link org.apache.lucene5_shaded.analysis.TokenFilter} &ndash; A <code>TokenFilter</code> is
 *     a {@link org.apache.lucene5_shaded.analysis.TokenStream} and is responsible
 *     for modifying tokens that have been created by the <code>Tokenizer</code>. Common 
 *     modifications performed by a <code>TokenFilter</code> are: deletion, stemming, synonym 
 *     injection, and case folding.  Not all <code>Analyzer</code>s require <code>TokenFilter</code>s.
 *   </li>
 * </ul>
 * <h2>Hints, Tips and Traps</h2>
 * <p>
 *   The relationship between {@link org.apache.lucene5_shaded.analysis.Analyzer} and
 *   {@link org.apache.lucene5_shaded.analysis.CharFilter}s,
 *   {@link org.apache.lucene5_shaded.analysis.Tokenizer}s,
 *   and {@link org.apache.lucene5_shaded.analysis.TokenFilter}s is sometimes confusing. To ease
 *   this confusion, here is some clarifications:
 * </p>
 * <ul>
 *   <li>
 *     The {@link org.apache.lucene5_shaded.analysis.Analyzer} is a
 *     <strong>factory</strong> for analysis chains. <code>Analyzer</code>s don't
 *     process text, <code>Analyzer</code>s construct <code>CharFilter</code>s, <code>Tokenizer</code>s, and/or
 *     <code>TokenFilter</code>s that process text. An <code>Analyzer</code> has two tasks: 
 *     to produce {@link org.apache.lucene5_shaded.analysis.TokenStream}s that accept a
 *     reader and produces tokens, and to wrap or otherwise
 *     pre-process {@link java.io.Reader} objects.
 *   </li>
 *   <li>
 *   The {@link org.apache.lucene5_shaded.analysis.CharFilter} is a subclass of
 *  {@link java.io.Reader} that supports offset tracking.
 *   </li>
 *   <li>The{@link org.apache.lucene5_shaded.analysis.Tokenizer}
 *     is only responsible for <u>breaking</u> the input text into tokens.
 *   </li>
 *   <li>The{@link org.apache.lucene5_shaded.analysis.TokenFilter} modifies a
 *   stream of tokens and their contents.
 *   </li>
 *   <li>
 *     {@link org.apache.lucene5_shaded.analysis.Tokenizer} is a {@link org.apache.lucene5_shaded.analysis.TokenStream},
 *     but {@link org.apache.lucene5_shaded.analysis.Analyzer} is not.
 *   </li>
 *   <li>
 *     {@link org.apache.lucene5_shaded.analysis.Analyzer} is "field aware", but
 *     {@link org.apache.lucene5_shaded.analysis.Tokenizer} is not. {@link org.apache.lucene5_shaded.analysis.Analyzer}s may
 *     take a field name into account when constructing the {@link org.apache.lucene5_shaded.analysis.TokenStream}.
 *   </li>
 * </ul>
 * <p>
 *   If you want to use a particular combination of <code>CharFilter</code>s, a
 *   <code>Tokenizer</code>, and some <code>TokenFilter</code>s, the simplest thing is often an
 *   create an anonymous subclass of {@link org.apache.lucene5_shaded.analysis.Analyzer}, provide {@link
 *   org.apache.lucene5_shaded.analysis.Analyzer#createComponents(String)} and perhaps also
 *   {@link org.apache.lucene5_shaded.analysis.Analyzer#initReader(String,
 *   java.io.Reader)}. However, if you need the same set of components
 *   over and over in many places, you can make a subclass of
 *   {@link org.apache.lucene5_shaded.analysis.Analyzer}. In fact, Apache Lucene
 *   supplies a large family of <code>Analyzer</code> classes that deliver useful
 *   analysis chains. The most common of these is the <a href="{@docRoot}/../analyzers-common/org/apache/lucene5_shaded/analysis/standard/StandardAnalyzer.html">StandardAnalyzer</a>.
 *   Many applications will have a long and industrious life with nothing more
 *   than the <code>StandardAnalyzer</code>. The <a href="{@docRoot}/../analyzers-common/overview-summary.html">analyzers-common</a>
 *   library provides many pre-existing analyzers for various languages.
 *   The analysis-common library also allows to configure a custom Analyzer without subclassing using the
 *   <a href="{@docRoot}/../analyzers-common/org/apache/lucene5_shaded/analysis/custom/CustomAnalyzer.html">CustomAnalyzer</a>
 *   class.
 * </p>
 * <p>
 *   Aside from the <code>StandardAnalyzer</code>,
 *   Lucene includes several components containing analysis components,
 *   all under the 'analysis' directory of the distribution. Some of
 *   these support particular languages, others integrate external
 *   components. The 'common' subdirectory has some noteworthy
 *  general-purpose analyzers, including the <a href="{@docRoot}/../analyzers-common/org/apache/lucene5_shaded/analysis/miscellaneous/PerFieldAnalyzerWrapper.html">PerFieldAnalyzerWrapper</a>. Most <code>Analyzer</code>s perform the same operation on all
 *  {@link org.apache.lucene5_shaded.document.Field}s.  The PerFieldAnalyzerWrapper can be used to associate a different <code>Analyzer</code> with different
 *  {@link org.apache.lucene5_shaded.document.Field}s. There is a great deal of
 *  functionality in the analysis area, you should study it carefully to
 *  find the pieces you need.
 * </p>
 * <p>
 *   Analysis is one of the main causes of slow indexing.  Simply put, the more you analyze the slower the indexing (in most cases).
 *   Perhaps your application would be just fine using the simple WhitespaceTokenizer combined with a StopFilter. The benchmark/ library can be useful 
 *   for testing out the speed of the analysis process.
 * </p>
 * <h2>Invoking the Analyzer</h2>
 * <p>
 *   Applications usually do not invoke analysis &ndash; Lucene does it
 *  for them. Applications construct <code>Analyzer</code>s and pass then into Lucene,
 *  as follows:
 * </p>
 * <ul>
 *   <li>
 *     At indexing, as a consequence of 
 *     {@link org.apache.lucene5_shaded.index.IndexWriter#addDocument(Iterable) addDocument(doc)},
 *     the <code>Analyzer</code> in effect for indexing is invoked for each indexed field of the added document.
 *   </li>
 *   <li>
 *     At search, a <code>QueryParser</code> may invoke the Analyzer during parsing.  Note that for some queries, analysis does not
 *     take place, e.g. wildcard queries.
 *   </li>
 * </ul>
 * <p>
 *   However an application might invoke Analysis of any text for testing or for any other purpose, something like:
 * </p>
 * <PRE class="prettyprint" id="analysis-workflow">
 *     Version matchVersion = Version.LUCENE_XY; // Substitute desired Lucene version for XY
 *     Analyzer analyzer = new StandardAnalyzer(matchVersion); // or any other analyzer
 *     TokenStream ts = analyzer.tokenStream("myfield", new StringReader("some text goes here"));
 *     // The Analyzer class will construct the Tokenizer, TokenFilter(s), and CharFilter(s),
 *     //   and pass the resulting Reader to the Tokenizer.
 *     OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
 *     
 *     try {
 *       ts.reset(); // Resets this stream to the beginning. (Required)
 *       while (ts.incrementToken()) {
 *         // Use {@link org.apache.lucene5_shaded.util.AttributeSource#reflectAsString(boolean)}
 *         // for token stream debugging.
 *         System.out.println("token: " + ts.reflectAsString(true));
 * 
 *         System.out.println("token start offset: " + offsetAtt.startOffset());
 *         System.out.println("  token end offset: " + offsetAtt.endOffset());
 *       }
 *       ts.end();   // Perform end-of-stream operations, e.g. set the final offset.
 *     } finally {
 *       ts.close(); // Release resources associated with this stream.
 *     }
 * </PRE>
 * <h2>Indexing Analysis vs. Search Analysis</h2>
 * <p>
 *   Selecting the "correct" analyzer is crucial
 *   for search quality, and can also affect indexing and search performance.
 *   The "correct" analyzer for your application will depend on what your input text
 *   looks like and what problem you are trying to solve.
 *   Lucene java's wiki page 
 *   <a href="http://wiki.apache.org/lucene-java/AnalysisParalysis">AnalysisParalysis</a> 
 *   provides some data on "analyzing your analyzer".
 *   Here are some rules of thumb:
 *   <ol>
 *     <li>Test test test... (did we say test?)</li>
 *     <li>Beware of too much analysis &ndash; it might hurt indexing performance.</li>
 *     <li>Start with the same analyzer for indexing and search, otherwise searches would not find what they are supposed to...</li>
 *     <li>In some cases a different analyzer is required for indexing and search, for instance:
 *         <ul>
 *            <li>Certain searches require more stop words to be filtered. (i.e. more than those that were filtered at indexing.)</li>
 *            <li>Query expansion by synonyms, acronyms, auto spell correction, etc.</li>
 *         </ul>
 *         This might sometimes require a modified analyzer &ndash; see the next section on how to do that.
 *     </li>
 *   </ol>
 * <h2>Implementing your own Analyzer and Analysis Components</h2>
 * <p>
 *   Creating your own Analyzer is straightforward. Your Analyzer should subclass {@link org.apache.lucene5_shaded.analysis.Analyzer}. It can use
 *   existing analysis components &mdash; CharFilter(s) <i>(optional)</i>, a
 *   Tokenizer, and TokenFilter(s) <i>(optional)</i> &mdash; or components you
 *   create, or a combination of existing and newly created components.  Before
 *   pursuing this approach, you may find it worthwhile to explore the
 *   <a href="{@docRoot}/../analyzers-common/overview-summary.html">analyzers-common</a> library and/or ask on the 
 *   <a href="http://lucene.apache.org/core/discussion.html">java-user@lucene5_shaded.apache.org mailing list</a> first to see if what you
 *   need already exists. If you are still committed to creating your own
 *   Analyzer, have a look at the source code of any one of the many samples
 *   located in this package.
 * </p>
 * <p>
 *   The following sections discuss some aspects of implementing your own analyzer.
 * </p>
 * <h3>Field Section Boundaries</h3>
 * <p>
 *   When {@link org.apache.lucene5_shaded.document.Document#add(org.apache.lucene5_shaded.index.IndexableField) document.add(field)}
 *   is called multiple times for the same field name, we could say that each such call creates a new 
 *   section for that field in that document. 
 *   In fact, a separate call to 
 *   {@link org.apache.lucene5_shaded.analysis.Analyzer#tokenStream(String, java.io.Reader) tokenStream(field,reader)}
 *   would take place for each of these so called "sections".
 *   However, the default Analyzer behavior is to treat all these sections as one large section. 
 *   This allows phrase search and proximity search to seamlessly cross 
 *   boundaries between these "sections".
 *   In other words, if a certain field "f" is added like this:
 * </p>
 * <PRE class="prettyprint">
 *     document.add(new Field("f","first ends",...);
 *     document.add(new Field("f","starts two",...);
 *     indexWriter.addDocument(document);
 * </PRE>
 * <p>
 *   Then, a phrase search for "ends starts" would find that document.
 *   Where desired, this behavior can be modified by introducing a "position gap" between consecutive field "sections", 
 *   simply by overriding 
 *   {@link org.apache.lucene5_shaded.analysis.Analyzer#getPositionIncrementGap(String) Analyzer.getPositionIncrementGap(fieldName)}:
 * </p>
 * <PRE class="prettyprint">
 *   Version matchVersion = Version.LUCENE_XY; // Substitute desired Lucene version for XY
 *   Analyzer myAnalyzer = new StandardAnalyzer(matchVersion) {
 *     public int getPositionIncrementGap(String fieldName) {
 *       return 10;
 *     }
 *   };
 * </PRE>
 * <h3>End of Input Cleanup</h3>
 * <p>
 *    At the ends of each field, Lucene will call the {@link org.apache.lucene5_shaded.analysis.TokenStream#end()}.
 *    The components of the token stream (the tokenizer and the token filters) <strong>must</strong>
 *    put accurate values into the token attributes to reflect the situation at the end of the field.
 *    The Offset attribute must contain the final offset (the total number of characters processed)
 *    in both start and end. Attributes like PositionLength must be correct. 
 * </p>
 * <p>
 *    The base method{@link org.apache.lucene5_shaded.analysis.TokenStream#end()} sets PositionIncrement to 0, which is required.
 *    Other components must override this method to fix up the other attributes.
 * </p>
 * <h3>Token Position Increments</h3>
 * <p>
 *    By default, TokenStream arranges for the 
 *    {@link org.apache.lucene5_shaded.analysis.tokenattributes.PositionIncrementAttribute#getPositionIncrement() position increment} of all tokens to be one.
 *    This means that the position stored for that token in the index would be one more than
 *    that of the previous token.
 *    Recall that phrase and proximity searches rely on position info.
 * </p>
 * <p>
 *    If the selected analyzer filters the stop words "is" and "the", then for a document 
 *    containing the string "blue is the sky", only the tokens "blue", "sky" are indexed, 
 *    with position("sky") = 3 + position("blue"). Now, a phrase query "blue is the sky"
 *    would find that document, because the same analyzer filters the same stop words from
 *    that query. But the phrase query "blue sky" would not find that document because the
 *    position increment between "blue" and "sky" is only 1.
 * </p>
 * <p>   
 *    If this behavior does not fit the application needs, the query parser needs to be
 *    configured to not take position increments into account when generating phrase queries.
 * </p>
 * <p>
 *   Note that a filter that filters <strong>out</strong> tokens <strong>must</strong> increment the position increment in order not to generate corrupt
 *   tokenstream graphs. Here is the logic used by StopFilter to increment positions when filtering out tokens:
 * </p>
 * <PRE class="prettyprint">
 *   public TokenStream tokenStream(final String fieldName, Reader reader) {
 *     final TokenStream ts = someAnalyzer.tokenStream(fieldName, reader);
 *     TokenStream res = new TokenStream() {
 *       CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
 *       PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
 * 
 *       public boolean incrementToken() throws IOException {
 *         int extraIncrement = 0;
 *         while (true) {
 *           boolean hasNext = ts.incrementToken();
 *           if (hasNext) {
 *             if (stopWords.contains(termAtt.toString())) {
 *               extraIncrement += posIncrAtt.getPositionIncrement(); // filter this word
 *               continue;
 *             } 
 *             if (extraIncrement &gt; 0) {
 *               posIncrAtt.setPositionIncrement(posIncrAtt.getPositionIncrement()+extraIncrement);
 *             }
 *           }
 *           return hasNext;
 *         }
 *       }
 *     };
 *     return res;
 *   }
 * </PRE>
 * <p>
 *    A few more use cases for modifying position increments are:
 * </p>
 * <ol>
 *   <li>Inhibiting phrase and proximity matches in sentence boundaries &ndash; for this, a tokenizer that 
 *     identifies a new sentence can add 1 to the position increment of the first token of the new sentence.</li>
 *   <li>Injecting synonyms &ndash; here, synonyms of a token should be added after that token, 
 *     and their position increment should be set to 0.
 *     As result, all synonyms of a token would be considered to appear in exactly the 
 *     same position as that token, and so would they be seen by phrase and proximity searches.</li>
 * </ol>
 * 
 * <h3>Token Position Length</h3>
 * <p>
 *    By default, all tokens created by Analyzers and Tokenizers have a
 *    {@link org.apache.lucene5_shaded.analysis.tokenattributes.PositionLengthAttribute#getPositionLength() position length} of one.
 *    This means that the token occupies a single position. This attribute is not indexed
 *    and thus not taken into account for positional queries, but is used by eg. suggesters.
 * </p>
 * <p>
 *    The main use case for positions lengths is multi-word synonyms. With single-word
 *    synonyms, setting the position increment to 0 is enough to denote the fact that two
 *    words are synonyms, for example:
 * </p>
 * <table summary="table showing position increments of 1 and 0 for red and magenta, respectively">
 * <tr><td>Term</td><td>red</td><td>magenta</td></tr>
 * <tr><td>Position increment</td><td>1</td><td>0</td></tr>
 * </table>
 * <p>
 *    Given that position(magenta) = 0 + position(red), they are at the same position, so anything
 *    working with analyzers will return the exact same result if you replace "magenta" with "red"
 *    in the input. However, multi-word synonyms are more tricky. Let's say that you want to build
 *    a TokenStream where "IBM" is a synonym of "Internal Business Machines". Position increments
 *    are not enough anymore:
 * </p>
 * <table summary="position increments where international is zero">
 * <tr><td>Term</td><td>IBM</td><td>International</td><td>Business</td><td>Machines</td></tr>
 * <tr><td>Position increment</td><td>1</td><td>0</td><td>1</td><td>1</td></tr>
 * </table>
 * <p>
 *    The problem with this token stream is that "IBM" is at the same position as "International"
 *    although it is a synonym with "International Business Machines" as a whole. Setting
 *    the position increment of "Business" and "Machines" to 0 wouldn't help as it would mean
 *    than "International" is a synonym of "Business". The only way to solve this issue is to
 *    make "IBM" span across 3 positions, this is where position lengths come to rescue.
 * </p>
 * <table summary="position lengths where IBM is three">
 * <tr><td>Term</td><td>IBM</td><td>International</td><td>Business</td><td>Machines</td></tr>
 * <tr><td>Position increment</td><td>1</td><td>0</td><td>1</td><td>1</td></tr>
 * <tr><td>Position length</td><td>3</td><td>1</td><td>1</td><td>1</td></tr>
 * </table>
 * <p>
 *    This new attribute makes clear that "IBM" and "International Business Machines" start and end
 *    at the same positions.
 * </p>
 * <a name="corrupt"></a>
 * <h3>How to not write corrupt token streams</h3>
 * <p>
 *    There are a few rules to observe when writing custom Tokenizers and TokenFilters:
 * </p>
 * <ul>
 *   <li>The first position increment must be &gt; 0.</li>
 *   <li>Positions must not go backward.</li>
 *   <li>Tokens that have the same start position must have the same start offset.</li>
 *   <li>Tokens that have the same end position (taking into account the
 *   position length) must have the same end offset.</li>
 *   <li>Tokenizers must call {@link
 *   org.apache.lucene5_shaded.util.AttributeSource#clearAttributes()} in
 *   incrementToken().</li>
 *   <li>Tokenizers must override {@link
 *   org.apache.lucene5_shaded.analysis.TokenStream#end()}, and pass the final
 *   offset (the total number of input characters processed) to both
 *   parameters of {@link org.apache.lucene5_shaded.analysis.tokenattributes.OffsetAttribute#setOffset(int, int)}.</li>
 * </ul>
 * <p>
 *    Although these rules might seem easy to follow, problems can quickly happen when chaining
 *    badly implemented filters that play with positions and offsets, such as synonym or n-grams
 *    filters. Here are good practices for writing correct filters:
 * </p>
 * <ul>
 *   <li>Token filters should not modify offsets. If you feel that your filter would need to modify offsets, then it should probably be implemented as a tokenizer.</li>
 *   <li>Token filters should not insert positions. If a filter needs to add tokens, then they should all have a position increment of 0.</li>
 *   <li>When they add tokens, token filters should call {@link org.apache.lucene5_shaded.util.AttributeSource#clearAttributes()} first.</li>
 *   <li>When they remove tokens, token filters should increment the position increment of the following token.</li>
 *   <li>Token filters should preserve position lengths.</li>
 * </ul>
 * <h2>TokenStream API</h2>
 * <p>
 *   "Flexible Indexing" summarizes the effort of making the Lucene indexer
 *   pluggable and extensible for custom index formats.  A fully customizable
 *   indexer means that users will be able to store custom data structures on
 *   disk. Therefore the analysis API must transport custom types of
 *   data from the documents to the indexer. (It also supports communications
 *   amongst the analysis components.)
 * </p>
 * <h3>Attribute and AttributeSource</h3>
 * <p>
 *   Classes {@link org.apache.lucene5_shaded.util.Attribute} and
 *   {@link org.apache.lucene5_shaded.util.AttributeSource} serve as the basis upon which
 *   the analysis elements of "Flexible Indexing" are implemented. An Attribute 
 *   holds a particular piece of information about a text token. For example, 
 *   {@link org.apache.lucene5_shaded.analysis.tokenattributes.CharTermAttribute}
 *   contains the term text of a token, and 
 *   {@link org.apache.lucene5_shaded.analysis.tokenattributes.OffsetAttribute} contains
 *   the start and end character offsets of a token. An AttributeSource is a 
 *   collection of Attributes with a restriction: there may be only one instance
 *   of each attribute type. TokenStream now extends AttributeSource, which means
 *   that one can add Attributes to a TokenStream. Since TokenFilter extends
 *   TokenStream, all filters are also AttributeSources.
 * </p>
 * <p>
 * Lucene provides seven Attributes out of the box:
 * </p>
 * <table rules="all" frame="box" cellpadding="3" summary="common bundled attributes">
 *   <tr>
 *     <td>{@link org.apache.lucene5_shaded.analysis.tokenattributes.CharTermAttribute}</td>
 *     <td>
 *       The term text of a token.  Implements {@link java.lang.CharSequence} 
 *       (providing methods length() and charAt(), and allowing e.g. for direct
 *       use with regular expression {@link java.util.regex.Matcher}s) and 
 *       {@link java.lang.Appendable} (allowing the term text to be appended to.)
 *     </td>
 *   </tr>
 *   <tr>
 *     <td>{@link org.apache.lucene5_shaded.analysis.tokenattributes.OffsetAttribute}</td>
 *     <td>The start and end offset of a token in characters.</td>
 *   </tr>
 *   <tr>
 *     <td>{@link org.apache.lucene5_shaded.analysis.tokenattributes.PositionIncrementAttribute}</td>
 *     <td>See above for detailed information about position increment.</td>
 *   </tr>
 *   <tr>
 *     <td>{@link org.apache.lucene5_shaded.analysis.tokenattributes.PositionLengthAttribute}</td>
 *     <td>The number of positions occupied by a token.</td>
 *   </tr>
 *   <tr>
 *     <td>{@link org.apache.lucene5_shaded.analysis.tokenattributes.PayloadAttribute}</td>
 *     <td>The payload that a Token can optionally have.</td>
 *   </tr>
 *   <tr>
 *     <td>{@link org.apache.lucene5_shaded.analysis.tokenattributes.TypeAttribute}</td>
 *     <td>The type of the token. Default is 'word'.</td>
 *   </tr>
 *   <tr>
 *     <td>{@link org.apache.lucene5_shaded.analysis.tokenattributes.FlagsAttribute}</td>
 *     <td>Optional flags a token can have.</td>
 *   </tr>
 *   <tr>
 *     <td>{@link org.apache.lucene5_shaded.analysis.tokenattributes.KeywordAttribute}</td>
 *     <td>
 *       Keyword-aware TokenStreams/-Filters skip modification of tokens that
 *       return true from this attribute's isKeyword() method. 
 *     </td>
 *   </tr>
 * </table>
 * <h3>More Requirements for Analysis Component Classes</h3>
 * Due to the historical development of the API, there are some perhaps
 * less than obvious requirements to implement analysis components
 * classes.
 * <h4 id="analysis-lifetime">Token Stream Lifetime</h4>
 * The code fragment of the <a href="#analysis-workflow">analysis workflow
 * protocol</a> above shows a token stream being obtained, used, and then
 * left for garbage. However, that does not mean that the components of
 * that token stream will, in fact, be discarded. The default is just the
 * opposite. {@link org.apache.lucene5_shaded.analysis.Analyzer} applies a reuse
 * strategy to the tokenizer and the token filters. It will reuse
 * them. For each new input, it calls {@link org.apache.lucene5_shaded.analysis.Tokenizer#setReader(java.io.Reader)}
 * to set the input. Your components must be prepared for this scenario,
 * as described below.
 * <h4>Tokenizer</h4>
 * <ul>
 *   <li>
 *   You should create your tokenizer class by extending {@link org.apache.lucene5_shaded.analysis.Tokenizer}.
 *   </li>
 *   <li>
 *   Your tokenizer <strong>must</strong> override {@link org.apache.lucene5_shaded.analysis.TokenStream#end()}.
 *   Your implementation <strong>must</strong> call
 *   <code>super.end()</code>. It must set a correct final offset into
 *   the offset attribute, and finish up and other attributes to reflect
 *   the end of the stream.
 *   </li>
 *   <li>
 *   If your tokenizer overrides {@link org.apache.lucene5_shaded.analysis.TokenStream#reset()}
 *   or {@link org.apache.lucene5_shaded.analysis.TokenStream#close()}, it
 *   <strong>must</strong> call the corresponding superclass method.
 *   </li>
 * </ul>
 * <h4>Token Filter</h4>
 *   You should create your token filter class by extending {@link org.apache.lucene5_shaded.analysis.TokenFilter}.
 *   If your token filter overrides {@link org.apache.lucene5_shaded.analysis.TokenStream#reset()},
 *   {@link org.apache.lucene5_shaded.analysis.TokenStream#end()}
 *   or {@link org.apache.lucene5_shaded.analysis.TokenStream#close()}, it
 *   <strong>must</strong> call the corresponding superclass method.
 * <h4>Creating delegates</h4>
 *   Forwarding classes (those which extend {@link org.apache.lucene5_shaded.analysis.Tokenizer} but delegate
 *   selected logic to another tokenizer) must also set the reader to the delegate in the overridden
 *   {@link org.apache.lucene5_shaded.analysis.Tokenizer#reset()} method, e.g.:
 *   <pre class="prettyprint">
 *     public class ForwardingTokenizer extends Tokenizer {
 *        private Tokenizer delegate;
 *        ...
 *        {@literal @Override}
 *        public void reset() {
 *           super.reset();
 *           delegate.setReader(this.input);
 *           delegate.reset();
 *        }
 *     }
 *   </pre>
 * <h3>Testing Your Analysis Component</h3>
 * <p>
 *     The lucene5_shaded-test-framework component defines
 *     <a href="{@docRoot}/../test-framework/org/apache/lucene5_shaded/analysis/BaseTokenStreamTestCase.html">BaseTokenStreamTestCase</a>. By extending
 *     this class, you can create JUnit tests that validate that your
 *     Analyzer and/or analysis components correctly implement the
 *     protocol. The checkRandomData methods of that class are particularly effective in flushing out errors.
 * </p>
 * <h3>Using the TokenStream API</h3>
 * There are a few important things to know in order to use the new API efficiently which are summarized here. You may want
 * to walk through the example below first and come back to this section afterwards.
 * <ol><li>
 * Please keep in mind that an AttributeSource can only have one instance of a particular Attribute. Furthermore, if 
 * a chain of a TokenStream and multiple TokenFilters is used, then all TokenFilters in that chain share the Attributes
 * with the TokenStream.
 * </li>
 * <li>
 * Attribute instances are reused for all tokens of a document. Thus, a TokenStream/-Filter needs to update
 * the appropriate Attribute(s) in incrementToken(). The consumer, commonly the Lucene indexer, consumes the data in the
 * Attributes and then calls incrementToken() again until it returns false, which indicates that the end of the stream
 * was reached. This means that in each call of incrementToken() a TokenStream/-Filter can safely overwrite the data in
 * the Attribute instances.
 * </li>
 * <li>
 * For performance reasons a TokenStream/-Filter should add/get Attributes during instantiation; i.e., create an attribute in the
 * constructor and store references to it in an instance variable.  Using an instance variable instead of calling addAttribute()/getAttribute() 
 * in incrementToken() will avoid attribute lookups for every token in the document.
 * </li>
 * <li>
 * All methods in AttributeSource are idempotent, which means calling them multiple times always yields the same
 * result. This is especially important to know for addAttribute(). The method takes the <b>type</b> (<code>Class</code>)
 * of an Attribute as an argument and returns an <b>instance</b>. If an Attribute of the same type was previously added, then
 * the already existing instance is returned, otherwise a new instance is created and returned. Therefore TokenStreams/-Filters
 * can safely call addAttribute() with the same Attribute type multiple times. Even consumers of TokenStreams should
 * normally call addAttribute() instead of getAttribute(), because it would not fail if the TokenStream does not have this
 * Attribute (getAttribute() would throw an IllegalArgumentException, if the Attribute is missing). More advanced code
 * could simply check with hasAttribute(), if a TokenStream has it, and may conditionally leave out processing for
 * extra performance.
 * </li></ol>
 * <h3>Example</h3>
 * <p>
 *   In this example we will create a WhiteSpaceTokenizer and use a LengthFilter to suppress all words that have
 *   only two or fewer characters. The LengthFilter is part of the Lucene core and its implementation will be explained
 *   here to illustrate the usage of the TokenStream API.
 * </p>
 * <p>
 *   Then we will develop a custom Attribute, a PartOfSpeechAttribute, and add another filter to the chain which
 *   utilizes the new custom attribute, and call it PartOfSpeechTaggingFilter.
 * </p>
 * <h4>Whitespace tokenization</h4>
 * <pre class="prettyprint">
 * public class MyAnalyzer extends Analyzer {
 * 
 *   private Version matchVersion;
 *   
 *   public MyAnalyzer(Version matchVersion) {
 *     this.matchVersion = matchVersion;
 *   }
 * 
 *   {@literal @Override}
 *   protected TokenStreamComponents createComponents(String fieldName) {
 *     return new TokenStreamComponents(new WhitespaceTokenizer(matchVersion));
 *   }
 *   
 *   public static void main(String[] args) throws IOException {
 *     // text to tokenize
 *     final String text = "This is a demo of the TokenStream API";
 *     
 *     Version matchVersion = Version.LUCENE_XY; // Substitute desired Lucene version for XY
 *     MyAnalyzer analyzer = new MyAnalyzer(matchVersion);
 *     TokenStream stream = analyzer.tokenStream("field", new StringReader(text));
 *     
 *     // get the CharTermAttribute from the TokenStream
 *     CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
 * 
 *     try {
 *       stream.reset();
 *     
 *       // print all tokens until stream is exhausted
 *       while (stream.incrementToken()) {
 *         System.out.println(termAtt.toString());
 *       }
 *     
 *       stream.end();
 *     } finally {
 *       stream.close();
 *     }
 *   }
 * }
 * </pre>
 * In this easy example a simple white space tokenization is performed. In main() a loop consumes the stream and
 * prints the term text of the tokens by accessing the CharTermAttribute that the WhitespaceTokenizer provides. 
 * Here is the output:
 * <pre>
 * This
 * is
 * a
 * demo
 * of
 * the
 * new
 * TokenStream
 * API
 * </pre>
 * <h4>Adding a LengthFilter</h4>
 * We want to suppress all tokens that have 2 or less characters. We can do that
 * easily by adding a LengthFilter to the chain. Only the
 * <code>createComponents()</code> method in our analyzer needs to be changed:
 * <pre class="prettyprint">
 *   {@literal @Override}
 *   protected TokenStreamComponents createComponents(String fieldName) {
 *     final Tokenizer source = new WhitespaceTokenizer(matchVersion);
 *     TokenStream result = new LengthFilter(true, source, 3, Integer.MAX_VALUE);
 *     return new TokenStreamComponents(source, result);
 *   }
 * </pre>
 * Note how now only words with 3 or more characters are contained in the output:
 * <pre>
 * This
 * demo
 * the
 * new
 * TokenStream
 * API
 * </pre>
 * Now let's take a look how the LengthFilter is implemented:
 * <pre class="prettyprint">
 * public final class LengthFilter extends FilteringTokenFilter {
 * 
 *   private final int min;
 *   private final int max;
 *   
 *   private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
 * 
 *   &#47;**
 *    * Create a new LengthFilter. This will filter out tokens whose
 *    * CharTermAttribute is either too short
 *    * (&lt; min) or too long (&gt; max).
 *    * {@literal @param} version the Lucene match version
 *    * {@literal @param} in      the TokenStream to consume
 *    * {@literal @param} min     the minimum length
 *    * {@literal @param} max     the maximum length
 *    *&#47;
 *  public LengthFilter(Version version, TokenStream in, int min, int max) {
 *     super(version, in);
 *     this.min = min;
 *     this.max = max;
 *   }
 * 
 *   {@literal @Override}
 *   public boolean accept() {
 *     final int len = termAtt.length();
 *     return (len &gt;= min &amp;&amp; len &lt;= max);
 *   }
 * 
 * }
 * </pre>
 * <p>
 *   In LengthFilter, the CharTermAttribute is added and stored in the instance
 *   variable <code>termAtt</code>.  Remember that there can only be a single
 *   instance of CharTermAttribute in the chain, so in our example the
 *   <code>addAttribute()</code> call in LengthFilter returns the
 *   CharTermAttribute that the WhitespaceTokenizer already added.
 * </p>
 * <p>
 *   The tokens are retrieved from the input stream in FilteringTokenFilter's 
 *   <code>incrementToken()</code> method (see below), which calls LengthFilter's
 *   <code>accept()</code> method. By looking at the term text in the
 *   CharTermAttribute, the length of the term can be determined and tokens that
 *   are either too short or too long are skipped.  Note how
 *   <code>accept()</code> can efficiently access the instance variable; no 
 *   attribute lookup is necessary. The same is true for the consumer, which can
 *   simply use local references to the Attributes.
 * </p>
 * <p>
 *   LengthFilter extends FilteringTokenFilter:
 * </p>
 * 
 * <pre class="prettyprint">
 * public abstract class FilteringTokenFilter extends TokenFilter {
 * 
 *   private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
 * 
 *   &#47;**
 *    * Create a new FilteringTokenFilter.
 *    * {@literal @param} in      the TokenStream to consume
 *    *&#47;
 *   public FilteringTokenFilter(Version version, TokenStream in) {
 *     super(in);
 *   }
 * 
 *   &#47;** Override this method and return if the current input token should be returned by incrementToken. *&#47;
 *   protected abstract boolean accept() throws IOException;
 * 
 *   {@literal @Override}
 *   public final boolean incrementToken() throws IOException {
 *     int skippedPositions = 0;
 *     while (input.incrementToken()) {
 *       if (accept()) {
 *         if (skippedPositions != 0) {
 *           posIncrAtt.setPositionIncrement(posIncrAtt.getPositionIncrement() + skippedPositions);
 *         }
 *         return true;
 *       }
 *       skippedPositions += posIncrAtt.getPositionIncrement();
 *     }
 *     // reached EOS -- return false
 *     return false;
 *   }
 * 
 *   {@literal @Override}
 *   public void reset() throws IOException {
 *     super.reset();
 *   }
 * 
 * }
 * </pre>
 * 
 * <h4>Adding a custom Attribute</h4>
 * Now we're going to implement our own custom Attribute for part-of-speech tagging and call it consequently 
 * <code>PartOfSpeechAttribute</code>. First we need to define the interface of the new Attribute:
 * <pre class="prettyprint">
 *   public interface PartOfSpeechAttribute extends Attribute {
 *     public static enum PartOfSpeech {
 *       Noun, Verb, Adjective, Adverb, Pronoun, Preposition, Conjunction, Article, Unknown
 *     }
 *   
 *     public void setPartOfSpeech(PartOfSpeech pos);
 *   
 *     public PartOfSpeech getPartOfSpeech();
 *   }
 * </pre>
 * <p>
 *   Now we also need to write the implementing class. The name of that class is important here: By default, Lucene
 *   checks if there is a class with the name of the Attribute with the suffix 'Impl'. In this example, we would
 *   consequently call the implementing class <code>PartOfSpeechAttributeImpl</code>.
 * </p>
 * <p>
 *   This should be the usual behavior. However, there is also an expert-API that allows changing these naming conventions:
 *   {@link org.apache.lucene5_shaded.util.AttributeFactory}. The factory accepts an Attribute interface as argument
 *   and returns an actual instance. You can implement your own factory if you need to change the default behavior.
 * </p>
 * <p>
 *   Now here is the actual class that implements our new Attribute. Notice that the class has to extend
 *   {@link org.apache.lucene5_shaded.util.AttributeImpl}:
 * </p>
 * <pre class="prettyprint">
 * public final class PartOfSpeechAttributeImpl extends AttributeImpl 
 *                                   implements PartOfSpeechAttribute {
 *   
 *   private PartOfSpeech pos = PartOfSpeech.Unknown;
 *   
 *   public void setPartOfSpeech(PartOfSpeech pos) {
 *     this.pos = pos;
 *   }
 *   
 *   public PartOfSpeech getPartOfSpeech() {
 *     return pos;
 *   }
 * 
 *   {@literal @Override}
 *   public void clear() {
 *     pos = PartOfSpeech.Unknown;
 *   }
 * 
 *   {@literal @Override}
 *   public void copyTo(AttributeImpl target) {
 *     ((PartOfSpeechAttribute) target).setPartOfSpeech(pos);
 *   }
 * }
 * </pre>
 * <p>
 *   This is a simple Attribute implementation has only a single variable that
 *   stores the part-of-speech of a token. It extends the
 *   <code>AttributeImpl</code> class and therefore implements its abstract methods
 *   <code>clear()</code> and <code>copyTo()</code>. Now we need a TokenFilter that
 *   can set this new PartOfSpeechAttribute for each token. In this example we
 *   show a very naive filter that tags every word with a leading upper-case letter
 *   as a 'Noun' and all other words as 'Unknown'.
 * </p>
 * <pre class="prettyprint">
 *   public static class PartOfSpeechTaggingFilter extends TokenFilter {
 *     PartOfSpeechAttribute posAtt = addAttribute(PartOfSpeechAttribute.class);
 *     CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
 *     
 *     protected PartOfSpeechTaggingFilter(TokenStream input) {
 *       super(input);
 *     }
 *     
 *     public boolean incrementToken() throws IOException {
 *       if (!input.incrementToken()) {return false;}
 *       posAtt.setPartOfSpeech(determinePOS(termAtt.buffer(), 0, termAtt.length()));
 *       return true;
 *     }
 *     
 *     // determine the part of speech for the given term
 *     protected PartOfSpeech determinePOS(char[] term, int offset, int length) {
 *       // naive implementation that tags every uppercased word as noun
 *       if (length &gt; 0 &amp;&amp; Character.isUpperCase(term[0])) {
 *         return PartOfSpeech.Noun;
 *       }
 *       return PartOfSpeech.Unknown;
 *     }
 *   }
 * </pre>
 * <p>
 *   Just like the LengthFilter, this new filter stores references to the
 *   attributes it needs in instance variables. Notice how you only need to pass
 *   in the interface of the new Attribute and instantiating the correct class
 *   is automatically taken care of.
 * </p>
 * <p>Now we need to add the filter to the chain in MyAnalyzer:</p>
 * <pre class="prettyprint">
 *   {@literal @Override}
 *   protected TokenStreamComponents createComponents(String fieldName) {
 *     final Tokenizer source = new WhitespaceTokenizer(matchVersion);
 *     TokenStream result = new LengthFilter(true, source, 3, Integer.MAX_VALUE);
 *     result = new PartOfSpeechTaggingFilter(result);
 *     return new TokenStreamComponents(source, result);
 *   }
 * </pre>
 * Now let's look at the output:
 * <pre>
 * This
 * demo
 * the
 * new
 * TokenStream
 * API
 * </pre>
 * Apparently it hasn't changed, which shows that adding a custom attribute to a TokenStream/Filter chain does not
 * affect any existing consumers, simply because they don't know the new Attribute. Now let's change the consumer
 * to make use of the new PartOfSpeechAttribute and print it out:
 * <pre class="prettyprint">
 *   public static void main(String[] args) throws IOException {
 *     // text to tokenize
 *     final String text = "This is a demo of the TokenStream API";
 *     
 *     MyAnalyzer analyzer = new MyAnalyzer();
 *     TokenStream stream = analyzer.tokenStream("field", new StringReader(text));
 *     
 *     // get the CharTermAttribute from the TokenStream
 *     CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
 *     
 *     // get the PartOfSpeechAttribute from the TokenStream
 *     PartOfSpeechAttribute posAtt = stream.addAttribute(PartOfSpeechAttribute.class);
 * 
 *     try {
 *       stream.reset();
 * 
 *       // print all tokens until stream is exhausted
 *       while (stream.incrementToken()) {
 *         System.out.println(termAtt.toString() + ": " + posAtt.getPartOfSpeech());
 *       }
 *     
 *       stream.end();
 *     } finally {
 *       stream.close();
 *     }
 *   }
 * </pre>
 * The change that was made is to get the PartOfSpeechAttribute from the TokenStream and print out its contents in
 * the while loop that consumes the stream. Here is the new output:
 * <pre>
 * This: Noun
 * demo: Unknown
 * the: Unknown
 * new: Unknown
 * TokenStream: Noun
 * API: Noun
 * </pre>
 * Each word is now followed by its assigned PartOfSpeech tag. Of course this is a naive 
 * part-of-speech tagging. The word 'This' should not even be tagged as noun; it is only spelled capitalized because it
 * is the first word of a sentence. Actually this is a good opportunity for an exercise. To practice the usage of the new
 * API the reader could now write an Attribute and TokenFilter that can specify for each word if it was the first token
 * of a sentence or not. Then the PartOfSpeechTaggingFilter can make use of this knowledge and only tag capitalized words
 * as nouns if not the first word of a sentence (we know, this is still not a correct behavior, but hey, it's a good exercise). 
 * As a small hint, this is how the new Attribute class could begin:
 * <pre class="prettyprint">
 *   public class FirstTokenOfSentenceAttributeImpl extends AttributeImpl
 *                               implements FirstTokenOfSentenceAttribute {
 *     
 *     private boolean firstToken;
 *     
 *     public void setFirstToken(boolean firstToken) {
 *       this.firstToken = firstToken;
 *     }
 *     
 *     public boolean getFirstToken() {
 *       return firstToken;
 *     }
 * 
 *     {@literal @Override}
 *     public void clear() {
 *       firstToken = false;
 *     }
 * 
 *   ...
 * </pre>
 * <h4>Adding a CharFilter chain</h4>
 * Analyzers take Java {@link java.io.Reader}s as input. Of course you can wrap your Readers with {@link java.io.FilterReader}s
 * to manipulate content, but this would have the big disadvantage that character offsets might be inconsistent with your original
 * text.
 * <p>
 * {@link org.apache.lucene5_shaded.analysis.CharFilter} is designed to allow you to pre-process input like a FilterReader would, but also
 * preserve the original offsets associated with those characters. This way mechanisms like highlighting still work correctly.
 * CharFilters can be chained.
 * <p>
 * Example:
 * <pre class="prettyprint">
 * public class MyAnalyzer extends Analyzer {
 * 
 *   {@literal @Override}
 *   protected TokenStreamComponents createComponents(String fieldName) {
 *     return new TokenStreamComponents(new MyTokenizer());
 *   }
 *   
 *   {@literal @Override}
 *   protected Reader initReader(String fieldName, Reader reader) {
 *     // wrap the Reader in a CharFilter chain.
 *     return new SecondCharFilter(new FirstCharFilter(reader));
 *   }
 * }
 * </pre>
 */
package org.apache.lucene5_shaded.analysis;
