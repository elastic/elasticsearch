package org.apache.lucene.search.vectorhighlight;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.highlight.Encoder;

import java.io.IOException;

/**
 * {@link org.apache.lucene.search.vectorhighlight.XFragmentsBuilder} is an interface for fragments (snippets) builder classes.
 * A {@link org.apache.lucene.search.vectorhighlight.XFragmentsBuilder} class can be plugged in to
 * {@link org.apache.lucene.search.vectorhighlight.XFastVectorHighlighter}.
 */
//LUCENE MONITOR - REMOVE ME WHEN LUCENE 4.5 IS OUT
public interface XFragmentsBuilder {

  /**
   * create a fragment.
   * 
   * @param reader IndexReader of the index
   * @param docId document id to be highlighted
   * @param fieldName field of the document to be highlighted
   * @param fieldFragList FieldFragList object
   * @return a created fragment or null when no fragment created
   * @throws IOException If there is a low-level I/O error
   */
  public String createFragment( IndexReader reader, int docId, String fieldName,
      XFieldFragList fieldFragList ) throws IOException;

  /**
   * create multiple fragments.
   * 
   * @param reader IndexReader of the index
   * @param docId document id to be highlighter
   * @param fieldName field of the document to be highlighted
   * @param fieldFragList FieldFragList object
   * @param maxNumFragments maximum number of fragments
   * @return created fragments or null when no fragments created.
   *         size of the array can be less than maxNumFragments
   * @throws IOException If there is a low-level I/O error
   */
  public String[] createFragments( IndexReader reader, int docId, String fieldName,
      XFieldFragList fieldFragList, int maxNumFragments ) throws IOException;

  /**
   * create a fragment.
   * 
   * @param reader IndexReader of the index
   * @param docId document id to be highlighted
   * @param fieldName field of the document to be highlighted
   * @param fieldFragList FieldFragList object
   * @param preTags pre-tags to be used to highlight terms
   * @param postTags post-tags to be used to highlight terms
   * @param encoder an encoder that generates encoded text
   * @return a created fragment or null when no fragment created
   * @throws IOException If there is a low-level I/O error
   */
  public String createFragment( IndexReader reader, int docId, String fieldName,
      XFieldFragList fieldFragList, String[] preTags, String[] postTags,
      Encoder encoder ) throws IOException;

  /**
   * create multiple fragments.
   * 
   * @param reader IndexReader of the index
   * @param docId document id to be highlighter
   * @param fieldName field of the document to be highlighted
   * @param fieldFragList FieldFragList object
   * @param maxNumFragments maximum number of fragments
   * @param preTags pre-tags to be used to highlight terms
   * @param postTags post-tags to be used to highlight terms
   * @param encoder an encoder that generates encoded text
   * @return created fragments or null when no fragments created.
   *         size of the array can be less than maxNumFragments
   * @throws IOException If there is a low-level I/O error
   */
  public String[] createFragments( IndexReader reader, int docId, String fieldName,
      XFieldFragList fieldFragList, int maxNumFragments, String[] preTags, String[] postTags,
      Encoder encoder ) throws IOException;
}
