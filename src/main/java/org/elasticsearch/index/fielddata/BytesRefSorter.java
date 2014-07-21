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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.Sorter;

/**
 * A {@link Sorter} for {@link BytesRef}s.
 */
public class BytesRefSorter extends InPlaceMergeSorter {

    private final BytesRef[] refs;

    public BytesRefSorter(BytesRef[] refs) {
        this.refs = refs;
    }

    @Override
    protected int compare(int i, int j) {
        return refs[i].compareTo(refs[j]);
    }

    @Override
    protected void swap(int i, int j) {
        ArrayUtil.swap(refs, i, j);
    }

}
