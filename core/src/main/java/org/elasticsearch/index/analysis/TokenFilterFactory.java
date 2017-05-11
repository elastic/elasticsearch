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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.elasticsearch.search.fetch.subphase.highlight.FastVectorHighlighter;

public interface TokenFilterFactory {
    String name();

    TokenStream create(TokenStream tokenStream);

    /**
     * Does this analyzer mess up the {@link OffsetAttribute}s in such as way as to break the
     * {@link FastVectorHighlighter}? If this is {@code true} then the
     * {@linkplain FastVectorHighlighter} will attempt to work around the broken offsets.
     */
    default boolean breaksFastVectorHighlighter() {
        return false;
    }
}
