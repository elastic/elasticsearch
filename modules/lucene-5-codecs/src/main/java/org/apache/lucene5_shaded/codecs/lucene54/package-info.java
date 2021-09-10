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
 * Lucene 5.4 file format.
 * 
 * <h1>Apache Lucene - Index File Formats</h1>
 * <div>
 * <ul>
 * <li><a href="#Introduction">Introduction</a></li>
 * <li><a href="#Definitions">Definitions</a>
 *   <ul>
 *   <li><a href="#Inverted_Indexing">Inverted Indexing</a></li>
 *   <li><a href="#Types_of_Fields">Types of Fields</a></li>
 *   <li><a href="#Segments">Segments</a></li>
 *   <li><a href="#Document_Numbers">Document Numbers</a></li>
 *   </ul>
 * </li>
 * <li><a href="#Overview">Index Structure Overview</a></li>
 * <li><a href="#File_Naming">File Naming</a></li>
 * <li><a href="#file-names">Summary of File Extensions</a>
 *   <ul>
 *   <li><a href="#Lock_File">Lock File</a></li>
 *   <li><a href="#History">History</a></li>
 *   <li><a href="#Limitations">Limitations</a></li>
 *   </ul>
 * </li>
 * </ul>
 * </div>
 * <a name="Introduction"></a>
 * <h2>Introduction</h2>
 * <div>
 * <p>This document defines the index file formats used in this version of Lucene.
 * If you are using a different version of Lucene, please consult the copy of
 * <code>docs/</code> that was distributed with
 * the version you are using.</p>
 * <p>Apache Lucene is written in Java, but several efforts are underway to write
 * <a href="http://wiki.apache.org/lucene-java/LuceneImplementations">versions of
 * Lucene in other programming languages</a>. If these versions are to remain
 * compatible with Apache Lucene, then a language-independent definition of the
 * Lucene index format is required. This document thus attempts to provide a
 * complete and independent definition of the Apache Lucene file formats.</p>
 * <p>As Lucene evolves, this document should evolve. Versions of Lucene in
 * different programming languages should endeavor to agree on file formats, and
 * generate new versions of this document.</p>
 * </div>
 * <a name="Definitions"></a>
 * <h2>Definitions</h2>
 * <div>
 * <p>The fundamental concepts in Lucene are index, document, field and term.</p>
 * <p>An index contains a sequence of documents.</p>
 * <ul>
 * <li>A document is a sequence of fields.</li>
 * <li>A field is a named sequence of terms.</li>
 * <li>A term is a sequence of bytes.</li>
 * </ul>
 * <p>The same sequence of bytes in two different fields is considered a different 
 * term. Thus terms are represented as a pair: the string naming the field, and the
 * bytes within the field.</p>
 * <a name="Inverted_Indexing"></a>
 * <h3>Inverted Indexing</h3>
 * <p>The index stores statistics about terms in order to make term-based search
 * more efficient. Lucene's index falls into the family of indexes known as an
 * <i>inverted index.</i> This is because it can list, for a term, the documents
 * that contain it. This is the inverse of the natural relationship, in which
 * documents list terms.</p>
 * <a name="Types_of_Fields"></a>
 * <h3>Types of Fields</h3>
 * <p>In Lucene, fields may be <i>stored</i>, in which case their text is stored
 * in the index literally, in a non-inverted manner. Fields that are inverted are
 * called <i>indexed</i>. A field may be both stored and indexed.</p>
 * <p>The text of a field may be <i>tokenized</i> into terms to be indexed, or the
 * text of a field may be used literally as a term to be indexed. Most fields are
 * tokenized, but sometimes it is useful for certain identifier fields to be
 * indexed literally.</p>
 * <p>See the {@link org.apache.lucene5_shaded.document.Field Field}
 * java docs for more information on Fields.</p>
 * <a name="Segments"></a>
 * <h3>Segments</h3>
 * <p>Lucene indexes may be composed of multiple sub-indexes, or <i>segments</i>.
 * Each segment is a fully independent index, which could be searched separately.
 * Indexes evolve by:</p>
 * <ol>
 * <li>Creating new segments for newly added documents.</li>
 * <li>Merging existing segments.</li>
 * </ol>
 * <p>Searches may involve multiple segments and/or multiple indexes, each index
 * potentially composed of a set of segments.</p>
 * <a name="Document_Numbers"></a>
 * <h3>Document Numbers</h3>
 * <p>Internally, Lucene refers to documents by an integer <i>document number</i>.
 * The first document added to an index is numbered zero, and each subsequent
 * document added gets a number one greater than the previous.</p>
 * <p>Note that a document's number may change, so caution should be taken when
 * storing these numbers outside of Lucene. In particular, numbers may change in
 * the following situations:</p>
 * <ul>
 * <li>
 * <p>The numbers stored in each segment are unique only within the segment, and
 * must be converted before they can be used in a larger context. The standard
 * technique is to allocate each segment a range of values, based on the range of
 * numbers used in that segment. To convert a document number from a segment to an
 * external value, the segment's <i>base</i> document number is added. To convert
 * an external value back to a segment-specific value, the segment is identified
 * by the range that the external value is in, and the segment's base value is
 * subtracted. For example two five document segments might be combined, so that
 * the first segment has a base value of zero, and the second of five. Document
 * three from the second segment would have an external value of eight.</p>
 * </li>
 * <li>
 * <p>When documents are deleted, gaps are created in the numbering. These are
 * eventually removed as the index evolves through merging. Deleted documents are
 * dropped when segments are merged. A freshly-merged segment thus has no gaps in
 * its numbering.</p>
 * </li>
 * </ul>
 * </div>
 * <a name="Overview"></a>
 * <h2>Index Structure Overview</h2>
 * <div>
 * <p>Each segment index maintains the following:</p>
 * <ul>
 * <li>
 * {@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50SegmentInfoFormat Segment info}.
 *    This contains metadata about a segment, such as the number of documents,
 *    what files it uses, 
 * </li>
 * <li>
 * {@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50FieldInfosFormat Field names}.
 *    This contains the set of field names used in the index.
 * </li>
 * <li>
 * {@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50StoredFieldsFormat Stored Field values}.
 * This contains, for each document, a list of attribute-value pairs, where the attributes 
 * are field names. These are used to store auxiliary information about the document, such as 
 * its title, url, or an identifier to access a database. The set of stored fields are what is 
 * returned for each hit when searching. This is keyed by document number.
 * </li>
 * <li>
 * {@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat Term dictionary}.
 * A dictionary containing all of the terms used in all of the
 * indexed fields of all of the documents. The dictionary also contains the number
 * of documents which contain the term, and pointers to the term's frequency and
 * proximity data.
 * </li>
 * <li>
 * {@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat Term Frequency data}.
 * For each term in the dictionary, the numbers of all the
 * documents that contain that term, and the frequency of the term in that
 * document, unless frequencies are omitted (IndexOptions.DOCS_ONLY)
 * </li>
 * <li>
 * {@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat Term Proximity data}.
 * For each term in the dictionary, the positions that the
 * term occurs in each document. Note that this will not exist if all fields in
 * all documents omit position data.
 * </li>
 * <li>
 * {@link org.apache.lucene5_shaded.codecs.lucene53.Lucene53NormsFormat Normalization factors}.
 * For each field in each document, a value is stored
 * that is multiplied into the score for hits on that field.
 * </li>
 * <li>
 * {@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50TermVectorsFormat Term Vectors}.
 * For each field in each document, the term vector (sometimes
 * called document vector) may be stored. A term vector consists of term text and
 * term frequency. To add Term Vectors to your index see the 
 * {@link org.apache.lucene5_shaded.document.Field Field} constructors
 * </li>
 * <li>
 * {@link org.apache.lucene5_shaded.codecs.lucene54.Lucene54DocValuesFormat Per-document values}.
 * Like stored values, these are also keyed by document
 * number, but are generally intended to be loaded into main memory for fast
 * access. Whereas stored values are generally intended for summary results from
 * searches, per-document values are useful for things like scoring factors.
 * </li>
 * <li>
 * {@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50LiveDocsFormat Live documents}.
 * An optional file indicating which documents are live.
 * </li>
 * </ul>
 * <p>Details on each of these are provided in their linked pages.</p>
 * </div>
 * <a name="File_Naming"></a>
 * <h2>File Naming</h2>
 * <div>
 * <p>All files belonging to a segment have the same name with varying extensions.
 * The extensions correspond to the different file formats described below. When
 * using the Compound File format (default in 1.4 and greater) these files (except
 * for the Segment info file, the Lock file, and Deleted documents file) are collapsed 
 * into a single .cfs file (see below for details)</p>
 * <p>Typically, all segments in an index are stored in a single directory,
 * although this is not required.</p>
 * <p>As of version 2.1 (lock-less commits), file names are never re-used.
 * That is, when any file is saved
 * to the Directory it is given a never before used filename. This is achieved
 * using a simple generations approach. For example, the first segments file is
 * segments_1, then segments_2, etc. The generation is a sequential long integer
 * represented in alpha-numeric (base 36) form.</p>
 * </div>
 * <a name="file-names"></a>
 * <h2>Summary of File Extensions</h2>
 * <div>
 * <p>The following table summarizes the names and extensions of the files in
 * Lucene:</p>
 * <table cellspacing="1" cellpadding="4" summary="lucene5_shaded filenames by extension">
 * <tr>
 * <th>Name</th>
 * <th>Extension</th>
 * <th>Brief Description</th>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.index.SegmentInfos Segments File}</td>
 * <td>segments_N</td>
 * <td>Stores information about a commit point</td>
 * </tr>
 * <tr>
 * <td><a href="#Lock_File">Lock File</a></td>
 * <td>write.lock</td>
 * <td>The Write lock prevents multiple IndexWriters from writing to the same
 * file.</td>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50SegmentInfoFormat Segment Info}</td>
 * <td>.si</td>
 * <td>Stores metadata about a segment</td>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50CompoundFormat Compound File}</td>
 * <td>.cfs, .cfe</td>
 * <td>An optional "virtual" file consisting of all the other index files for
 * systems that frequently run out of file handles.</td>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50FieldInfosFormat Fields}</td>
 * <td>.fnm</td>
 * <td>Stores information about the fields</td>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50StoredFieldsFormat Field Index}</td>
 * <td>.fdx</td>
 * <td>Contains pointers to field data</td>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50StoredFieldsFormat Field Data}</td>
 * <td>.fdt</td>
 * <td>The stored fields for documents</td>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat Term Dictionary}</td>
 * <td>.tim</td>
 * <td>The term dictionary, stores term info</td>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat Term Index}</td>
 * <td>.tip</td>
 * <td>The index into the Term Dictionary</td>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat Frequencies}</td>
 * <td>.doc</td>
 * <td>Contains the list of docs which contain each term along with frequency</td>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat Positions}</td>
 * <td>.pos</td>
 * <td>Stores position information about where a term occurs in the index</td>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat Payloads}</td>
 * <td>.pay</td>
 * <td>Stores additional per-position metadata information such as character offsets and user payloads</td>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.codecs.lucene53.Lucene53NormsFormat Norms}</td>
 * <td>.nvd, .nvm</td>
 * <td>Encodes length and boost factors for docs and fields</td>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.codecs.lucene54.Lucene54DocValuesFormat Per-Document Values}</td>
 * <td>.dvd, .dvm</td>
 * <td>Encodes additional scoring factors or other per-document information.</td>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50TermVectorsFormat Term Vector Index}</td>
 * <td>.tvx</td>
 * <td>Stores offset into the document data file</td>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50TermVectorsFormat Term Vector Documents}</td>
 * <td>.tvd</td>
 * <td>Contains information about each document that has term vectors</td>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50TermVectorsFormat Term Vector Fields}</td>
 * <td>.tvf</td>
 * <td>The field level info about term vectors</td>
 * </tr>
 * <tr>
 * <td>{@link org.apache.lucene5_shaded.codecs.lucene50.Lucene50LiveDocsFormat Live Documents}</td>
 * <td>.liv</td>
 * <td>Info about what files are live</td>
 * </tr>
 * </table>
 * </div>
 * <a name="Lock_File"></a>
 * <h2>Lock File</h2>
 * The write lock, which is stored in the index directory by default, is named
 * "write.lock". If the lock directory is different from the index directory then
 * the write lock will be named "XXXX-write.lock" where XXXX is a unique prefix
 * derived from the full path to the index directory. When this file is present, a
 * writer is currently modifying the index (adding or removing documents). This
 * lock file ensures that only one writer is modifying the index at a time.
 * <a name="History"></a>
 * <h2>History</h2>
 * <p>Compatibility notes are provided in this document, describing how file
 * formats have changed from prior versions:</p>
 * <ul>
 * <li>In version 2.1, the file format was changed to allow lock-less commits (ie,
 * no more commit lock). The change is fully backwards compatible: you can open a
 * pre-2.1 index for searching or adding/deleting of docs. When the new segments
 * file is saved (committed), it will be written in the new file format (meaning
 * no specific "upgrade" process is needed). But note that once a commit has
 * occurred, pre-2.1 Lucene will not be able to read the index.</li>
 * <li>In version 2.3, the file format was changed to allow segments to share a
 * single set of doc store (vectors &amp; stored fields) files. This allows for
 * faster indexing in certain cases. The change is fully backwards compatible (in
 * the same way as the lock-less commits change in 2.1).</li>
 * <li>In version 2.4, Strings are now written as true UTF-8 byte sequence, not
 * Java's modified UTF-8. See <a href="http://issues.apache.org/jira/browse/LUCENE-510">
 * LUCENE-510</a> for details.</li>
 * <li>In version 2.9, an optional opaque Map&lt;String,String&gt; CommitUserData
 * may be passed to IndexWriter's commit methods (and later retrieved), which is
 * recorded in the segments_N file. See <a href="http://issues.apache.org/jira/browse/LUCENE-1382">
 * LUCENE-1382</a> for details. Also,
 * diagnostics were added to each segment written recording details about why it
 * was written (due to flush, merge; which OS/JRE was used; etc.). See issue
 * <a href="http://issues.apache.org/jira/browse/LUCENE-1654">LUCENE-1654</a> for details.</li>
 * <li>In version 3.0, compressed fields are no longer written to the index (they
 * can still be read, but on merge the new segment will write them, uncompressed).
 * See issue <a href="http://issues.apache.org/jira/browse/LUCENE-1960">LUCENE-1960</a> 
 * for details.</li>
 * <li>In version 3.1, segments records the code version that created them. See
 * <a href="http://issues.apache.org/jira/browse/LUCENE-2720">LUCENE-2720</a> for details. 
 * Additionally segments track explicitly whether or not they have term vectors. 
 * See <a href="http://issues.apache.org/jira/browse/LUCENE-2811">LUCENE-2811</a> 
 * for details.</li>
 * <li>In version 3.2, numeric fields are written as natively to stored fields
 * file, previously they were stored in text format only.</li>
 * <li>In version 3.4, fields can omit position data while still indexing term
 * frequencies.</li>
 * <li>In version 4.0, the format of the inverted index became extensible via
 * the {@link org.apache.lucene5_shaded.codecs.Codec Codec} api. Fast per-document storage
 * ({@code DocValues}) was introduced. Normalization factors need no longer be a 
 * single byte, they can be any {@link org.apache.lucene5_shaded.index.NumericDocValues NumericDocValues}.
 * Terms need not be unicode strings, they can be any byte sequence. Term offsets 
 * can optionally be indexed into the postings lists. Payloads can be stored in the 
 * term vectors.</li>
 * <li>In version 4.1, the format of the postings list changed to use either
 * of FOR compression or variable-byte encoding, depending upon the frequency
 * of the term. Terms appearing only once were changed to inline directly into
 * the term dictionary. Stored fields are compressed by default. </li>
 * <li>In version 4.2, term vectors are compressed by default. DocValues has 
 * a new multi-valued type (SortedSet), that can be used for faceting/grouping/joining
 * on multi-valued fields.</li>
 * <li>In version 4.5, DocValues were extended to explicitly represent missing values.</li>
 * <li>In version 4.6, FieldInfos were extended to support per-field DocValues generation, to 
 * allow updating NumericDocValues fields.</li>
 * <li>In version 4.8, checksum footers were added to the end of each index file 
 * for improved data integrity. Specifically, the last 8 bytes of every index file
 * contain the zlib-crc32 checksum of the file.</li>
 * <li>In version 4.9, DocValues has a new multi-valued numeric type (SortedNumeric)
 * that is suitable for faceting/sorting/analytics.
 * <li>In version 5.4, DocValues have been improved to store more information on disk:
 * addresses for binary fields and ord indexes for multi-valued fields.
 * </li>
 * </ul>
 * <a name="Limitations"></a>
 * <h2>Limitations</h2>
 * <div>
 * <p>Lucene uses a Java <code>int</code> to refer to
 * document numbers, and the index file format uses an <code>Int32</code>
 * on-disk to store document numbers. This is a limitation
 * of both the index file format and the current implementation. Eventually these
 * should be replaced with either <code>UInt64</code> values, or
 * better yet, {@link org.apache.lucene5_shaded.store.DataOutput#writeVInt VInt} values which have no limit.</p>
 * </div>
 */
package org.apache.lucene5_shaded.codecs.lucene54;
