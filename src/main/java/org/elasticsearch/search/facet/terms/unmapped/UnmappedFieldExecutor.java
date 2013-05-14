/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.facet.terms.unmapped;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.strings.InternalStringTermsFacet;

import java.io.IOException;
import java.util.Collection;

/**
 * A facet executor that only aggregates the missing count for unmapped fields and builds a terms facet over it
 */
public class UnmappedFieldExecutor extends FacetExecutor {

    final int size;
    final TermsFacet.ComparatorType comparatorType;

    long missing;

    public UnmappedFieldExecutor(int size, TermsFacet.ComparatorType comparatorType) {
        this.size = size;
        this.comparatorType = comparatorType;
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        Collection<InternalStringTermsFacet.TermEntry> entries = ImmutableList.of();
        return new InternalStringTermsFacet(facetName, comparatorType, size, entries, missing, 0);
    }

    @Override
    public Collector collector() {
        return new Collector();
    }


    final class Collector extends FacetExecutor.Collector {

        private long  missing;

        @Override
        public void postCollection() {
            UnmappedFieldExecutor.this.missing = missing;
        }

        @Override
        public void collect(int doc) throws IOException {
            missing++;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
        }
    }
}
