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

package org.elasticsearch.search.facet.terms.comparator;

import org.elasticsearch.search.facet.terms.TermsFacet.Entry;

/**
 * A comparator for terms facet counts
 */
public class TermsFacetCountComparator extends AbstractTermsFacetComparator {

    public TermsFacetCountComparator(String type, boolean reverse) {
        super(type, reverse);
    }
    
    @Override
    public int compare(Entry o1, Entry o2) {
        int i = o2.count() - o1.count();
        if (i == 0) {
            i = o2.compareTo(o1);
            if (i == 0) {
                i = System.identityHashCode(o2) - System.identityHashCode(o1);
            }
        }
        return reverse ? -i : i;
    }
}
