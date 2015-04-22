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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 * A filter implementation that resolves details at the last possible moment between filter parsing and execution.
 * For example a date filter based on 'now'.
 */
public abstract class ResolvableFilter extends Filter {

    /**
     * @return The actual filter instance to be executed containing the latest details.
     */
    public abstract Filter resolve();

    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
        Filter resolvedFilter = resolve();
        if (resolvedFilter != null) {
            return resolvedFilter.getDocIdSet(context, acceptDocs);
        } else {
            return null;
        }
    }
}
