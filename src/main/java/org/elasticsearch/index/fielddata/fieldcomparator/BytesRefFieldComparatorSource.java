/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.index.fielddata.IndexFieldData;

import java.io.IOException;

/**
 */
public class BytesRefFieldComparatorSource extends IndexFieldData.XFieldComparatorSource {

    /** UTF-8 term containing a single code point: {@link Character#MAX_CODE_POINT} which will compare greater than all other index terms
     *  since {@link Character#MAX_CODE_POINT} is a noncharacter and thus shouldn't appear in an index term. */
    public static final BytesRef MAX_TERM;
    static {
        MAX_TERM = new BytesRef();
        final char[] chars = Character.toChars(Character.MAX_CODE_POINT);
        UnicodeUtil.UTF16toUTF8(chars, 0, chars.length, MAX_TERM);
    }

    private final IndexFieldData<?> indexFieldData;
    private final SortMode sortMode;
    private final Object missingValue;

    public BytesRefFieldComparatorSource(IndexFieldData<?> indexFieldData, Object missingValue, SortMode sortMode) {
        this.indexFieldData = indexFieldData;
        this.sortMode = sortMode;
        this.missingValue = missingValue;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.STRING;
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
        assert fieldname.equals(indexFieldData.getFieldNames().indexName());
        BytesRef missingBytes = null;
        if (missingValue == null || "_last".equals(missingValue)) {
            missingBytes = reversed ? null : MAX_TERM;
        } else if ("_first".equals(missingValue)) {
            missingBytes = reversed ? MAX_TERM : null;
        } else if (missingValue instanceof BytesRef) {
            missingBytes = (BytesRef) missingValue;
        } else if (missingValue instanceof String) {
            missingBytes = new BytesRef((String) missingValue);
        } else if (missingValue instanceof byte[]) {
            missingBytes = new BytesRef((byte[]) missingValue);
        } else {
            throw new ElasticSearchIllegalArgumentException("Unsupported missing value: " + missingValue);
        }

        if (indexFieldData.valuesOrdered() && indexFieldData instanceof IndexFieldData.WithOrdinals) {
            return new BytesRefOrdValComparator((IndexFieldData.WithOrdinals<?>) indexFieldData, numHits, sortMode, missingBytes);
        }
        return new BytesRefValComparator(indexFieldData, numHits, sortMode, missingBytes);
    }

}
