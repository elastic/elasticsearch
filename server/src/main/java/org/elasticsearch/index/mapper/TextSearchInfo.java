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

    public enum TermVector { NONE, DOCS, POSITIONS, OFFSETS }

    private final FieldType luceneFieldType;

    public TextSearchInfo(FieldType luceneFieldType) {
        this.luceneFieldType = luceneFieldType;
    }

    /**
     * @return whether or not this field supports positional queries
     */
    public boolean hasPositions() {
        return luceneFieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }

    public boolean hasOffsets() {
        return luceneFieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    }

    /**
     * @return whether or not this field has indexed norms
     */
    public boolean hasNorms() {
        return luceneFieldType.omitNorms() == false;
    }

    public boolean isTokenized() {
        return luceneFieldType.tokenized();
    }

    public boolean isStored() {
        return luceneFieldType.stored();    // TODO move this directly to MappedFieldType? It's not text specific...
    }

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

    private static final FieldType NON_TEXT_FIELD_TYPE = new FieldType();
    static {
        NON_TEXT_FIELD_TYPE.setOmitNorms(true);
        NON_TEXT_FIELD_TYPE.freeze();
    }

    /**
     * The field type used by numeric fields
     */
    public static TextSearchInfo NUMERIC = new TextSearchInfo(NON_TEXT_FIELD_TYPE);

    /**
     * The field type used by geometry fields
     */
    public static TextSearchInfo GEOMETRY = new TextSearchInfo(NON_TEXT_FIELD_TYPE);

    private static final FieldType KEYWORD_FIELD_TYPE = new FieldType();
    static {
        KEYWORD_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        KEYWORD_FIELD_TYPE.setTokenized(false);
        KEYWORD_FIELD_TYPE.freeze();
    }

    /**
     * The field type used by un-analyzed text fields
     */
    public static TextSearchInfo KEYWORD = new TextSearchInfo(KEYWORD_FIELD_TYPE);

}
