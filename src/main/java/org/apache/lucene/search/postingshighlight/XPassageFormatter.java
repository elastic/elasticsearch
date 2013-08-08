package org.apache.lucene.search.postingshighlight;

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

import org.elasticsearch.Version;

/**
 * Creates a formatted snippet from the top passages.
 *
 * @lucene.experimental
 */
//LUCENE MONITOR - REMOVE ME WHEN LUCENE 4.6 IS OUT
//Applied LUCENE-4906 to be able to return arbitrary objects
public abstract class XPassageFormatter {

    static {
        assert Version.CURRENT.luceneVersion.compareTo(org.apache.lucene.util.Version.LUCENE_45) == 0 : "Remove XPassageFormatter once 4.6 is out";
    }

    /**
     * Formats the top <code>passages</code> from <code>content</code>
     * into a human-readable text snippet.
     *
     * @param passages top-N passages for the field. Note these are sorted in
     *        the order that they appear in the document for convenience.
     * @param content content for the field.
     * @return formatted highlight
     */
    public abstract Object format(Passage passages[], String content);

}