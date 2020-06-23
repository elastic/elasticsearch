/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

/**
 * Encapsulates information about how to perform text searches over a field
 */
public class TextSearchInfo {

    private static final FieldType SIMPLE_MATCH_ONLY_FIELD_TYPE = new FieldType();
    static {
        SIMPLE_MATCH_ONLY_FIELD_TYPE.setTokenized(false);
        SIMPLE_MATCH_ONLY_FIELD_TYPE.setOmitNorms(true);
        SIMPLE_MATCH_ONLY_FIELD_TYPE.freeze();
    }

    /**
     * Defines indexing information for fields that support only simple match text queries
     *
     * Note that the results of {@link #isStored()} for this may not be accurate
     */
    public static final TextSearchInfo SIMPLE_MATCH_ONLY = new TextSearchInfo(SIMPLE_MATCH_ONLY_FIELD_TYPE);

    /**
     * Specifies that this field does not support text searching of any kind
     */
    public static final TextSearchInfo NONE = new TextSearchInfo(SIMPLE_MATCH_ONLY_FIELD_TYPE);

    private final FieldType luceneFieldType;

    /**
     * Create a TextSearchInfo by wrapping a lucene FieldType
     */
    public TextSearchInfo(FieldType luceneFieldType) {
        this.luceneFieldType = luceneFieldType;
    }

    /**
     * @return whether or not this field supports positional queries
     */
    public boolean hasPositions() {
        return luceneFieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }

    /**
     * @return whether or not this field has indexed offsets for highlighting
     */
    public boolean hasOffsets() {
        return luceneFieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    }

    /**
     * @return whether or not this field has indexed norms
     */
    public boolean hasNorms() {
        return luceneFieldType.omitNorms() == false;
    }

    /**
     * @return whether or not this field is tokenized
     */
    public boolean isTokenized() {
        return luceneFieldType.tokenized();
    }

    /**
     * @return whether or not this field is stored
     */
    public boolean isStored() {
        return luceneFieldType.stored();    // TODO move this directly to MappedFieldType? It's not text specific...
    }

    /**
     * What sort of term vectors are available
     */
    public enum TermVector { NONE, DOCS, POSITIONS, OFFSETS }

    /**
     * @return the type of term vectors available for this field
     */
    public TermVector termVectors() {
        if (luceneFieldType.storeTermVectors() == false) {
            return TermVector.NONE;
        }
        if (luceneFieldType.storeTermVectorOffsets()) {
            return TermVector.OFFSETS;
        }
        if (luceneFieldType.storeTermVectorPositions()) {
            return TermVector.POSITIONS;
        }
        return TermVector.DOCS;
    }

}
