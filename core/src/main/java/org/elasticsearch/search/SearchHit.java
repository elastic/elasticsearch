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

package org.elasticsearch.search;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.highlight.HighlightField;

import java.util.Map;

/**
 * A single search hit.
 *
 * @see SearchHits
 */
public interface SearchHit extends Streamable, ToXContent, Iterable<SearchHitField> {

    /**
     * The score.
     */
    float score();

    /**
     * The score.
     */
    float getScore();

    /**
     * The index of the hit.
     */
    String index();

    /**
     * The index of the hit.
     */
    String getIndex();

    /**
     * The id of the document.
     */
    String id();

    /**
     * The id of the document.
     */
    String getId();

    /**
     * The type of the document.
     */
    String type();

    /**
     * The type of the document.
     */
    String getType();

    /**
     * If this is a nested hit then nested reference information is returned otherwise <code>null</code> is returned.
     */
    NestedIdentity getNestedIdentity();

    /**
     * The version of the hit.
     */
    long version();

    /**
     * The version of the hit.
     */
    long getVersion();

    /**
     * Returns bytes reference, also un compress the source if needed.
     */
    BytesReference sourceRef();

    /**
     * Returns bytes reference, also un compress the source if needed.
     */
    BytesReference getSourceRef();

    /**
     * The source of the document (can be <tt>null</tt>). Note, its a copy of the source
     * into a byte array, consider using {@link #sourceRef()} so there won't be a need to copy.
     */
    byte[] source();

    /**
     * Is the source available or not. A source with no fields will return true. This will return false if {@code fields} doesn't contain
     * {@code _source} or if source is disabled in the mapping.
     */
    boolean hasSource();

    /**
     * The source of the document as a map (can be <tt>null</tt>).
     */
    Map<String, Object> getSource();

    /**
     * The source of the document as string (can be <tt>null</tt>).
     */
    String sourceAsString();

    /**
     * The source of the document as string (can be <tt>null</tt>).
     */
    String getSourceAsString();

    /**
     * The source of the document as a map (can be <tt>null</tt>).
     */
    Map<String, Object> sourceAsMap() throws ElasticsearchParseException;

    /**
     * If enabled, the explanation of the search hit.
     */
    Explanation explanation();

    /**
     * If enabled, the explanation of the search hit.
     */
    Explanation getExplanation();

    /**
     * The hit field matching the given field name.
     */
    public SearchHitField field(String fieldName);

    /**
     * A map of hit fields (from field name to hit fields) if additional fields
     * were required to be loaded.
     */
    Map<String, SearchHitField> fields();

    /**
     * A map of hit fields (from field name to hit fields) if additional fields
     * were required to be loaded.
     */
    Map<String, SearchHitField> getFields();

    /**
     * A map of highlighted fields.
     */
    Map<String, HighlightField> highlightFields();

    /**
     * A map of highlighted fields.
     */
    Map<String, HighlightField> getHighlightFields();

    /**
     * An array of the sort values used.
     */
    Object[] sortValues();

    /**
     * An array of the sort values used.
     */
    Object[] getSortValues();

    /**
     * The set of query and filter names the query matched with. Mainly makes sense for compound filters and queries.
     */
    String[] matchedQueries();

    /**
     * The set of query and filter names the query matched with. Mainly makes sense for compound filters and queries.
     */
    String[] getMatchedQueries();

    /**
     * The shard of the search hit.
     */
    SearchShardTarget shard();

    /**
     * The shard of the search hit.
     */
    SearchShardTarget getShard();

    /**
     * @return Inner hits or <code>null</code> if there are none
     */
    Map<String, SearchHits> getInnerHits();

    /**
     * Encapsulates the nested identity of a hit.
     */
    public interface NestedIdentity {

        /**
         * Returns the nested field in the source this hit originates from
         */
        public Text getField();

        /**
         * Returns the offset in the nested array of objects in the source this hit
         */
        public int getOffset();

        /**
         * Returns the next child nested level if there is any, otherwise <code>null</code> is returned.
         *
         * In the case of mappings with multiple levels of nested object fields
         */
        public NestedIdentity getChild();
    }
}
