/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.util.io.stream.Streamable;
import org.elasticsearch.util.json.ToJson;

import java.util.Map;

/**
 * A single search hit.
 *
 * @author kimchy (shay.banon)
 * @see SearchHits
 */
public interface SearchHit extends Streamable, ToJson, Iterable<SearchHitField> {

    /**
     * The index of the hit.
     */
    String index();

    /**
     * The id of the document.
     */
    String id();

    /**
     * The type of the document.
     */
    String type();

    /**
     * The source of the document (can be <tt>null</tt>).
     */
    byte[] source();

    /**
     * The source of the document as string (can be <tt>null</tt>).
     */
    String sourceAsString();

    /**
     * The source of the document as a map (can be <tt>null</tt>).
     */
    Map<String, Object> sourceAsMap() throws ElasticSearchParseException;

    /**
     * If enabled, the explanation of the search hit.
     */
    Explanation explanation();

    /**
     * A map of hit fields (from field name to hit fields) if additional fields
     * were required to be loaded.
     */
    Map<String, SearchHitField> fields();

    /**
     * A map of highlighted fields.
     */
    Map<String, HighlightField> highlightFields();

    /**
     * The shard of the search hit.
     */
    SearchShardTarget shard();
}
