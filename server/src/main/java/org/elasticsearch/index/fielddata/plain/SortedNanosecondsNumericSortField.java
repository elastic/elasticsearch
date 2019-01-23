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

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;

/**
 * This class is a wrapper around a sorted numeric field in order to find out if we need to execute our specialised merging/conversion
 * logic when querying indices where fields are milliseconds on the one hand and nanoseconds on the other hand
 */
public class SortedNanosecondsNumericSortField extends SortedNumericSortField {

    public SortedNanosecondsNumericSortField(String fieldName, boolean reverse, SortedNumericSelector.Type selectorType) {
        super(fieldName, SortField.Type.LONG, reverse, selectorType);
    }

    public SortedNanosecondsNumericSortField(String fieldName, boolean reverse) {
        super(fieldName, SortField.Type.LONG, reverse);
    }
}
