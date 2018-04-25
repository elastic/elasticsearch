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
package org.elasticsearch.search.aggregations.metrics.tophits;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Results of the {@link TopHitsAggregator}.
 */
public class InternalTopHits extends InternalAggregation implements TopHits {
    private int from;
    private int size;
    private TopDocs topDocs;
    private SearchHits searchHits;

    public InternalTopHits(String name, int from, int size, TopDocs topDocs, SearchHits searchHits,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.from = from;
        this.size = size;
        this.topDocs = topDocs;
        this.searchHits = searchHits;
    }

    /**
     * Read from a stream.
     */
    public InternalTopHits(StreamInput in) throws IOException {
        super(in);
        from = in.readVInt();
        size = in.readVInt();
        topDocs = Lucene.readTopDocs(in);
        assert topDocs != null;
        searchHits = SearchHits.readSearchHits(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(from);
        out.writeVInt(size);
        Lucene.writeTopDocs(out, topDocs);
        searchHits.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return TopHitsAggregationBuilder.NAME;
    }

    @Override
    public SearchHits getHits() {
        return searchHits;
    }

    TopDocs getTopDocs() {
        return topDocs;
    }

    int getFrom() {
        return from;
    }

    int getSize() {
        return size;
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        final SearchHits[] shardHits = new SearchHits[aggregations.size()];
        final int from;
        final int size;
        if (reduceContext.isFinalReduce()) {
            from = this.from;
            size = this.size;
        } else {
            // if we are not in the final reduce we need to ensure we maintain all possible elements during reduce
            // hence for pagination we need to maintain all hits until we are in the final phase.
            from = 0;
            size = this.from + this.size;
        }

        final TopDocs reducedTopDocs;
        final TopDocs[] shardDocs;

        if (topDocs instanceof TopFieldDocs) {
            Sort sort = new Sort(((TopFieldDocs) topDocs).fields);
            shardDocs = new TopFieldDocs[aggregations.size()];
            for (int i = 0; i < shardDocs.length; i++) {
                InternalTopHits topHitsAgg = (InternalTopHits) aggregations.get(i);
                shardDocs[i] = topHitsAgg.topDocs;
                shardHits[i] = topHitsAgg.searchHits;
            }
            reducedTopDocs = TopDocs.merge(sort, from, size, (TopFieldDocs[]) shardDocs, true);
        } else {
            shardDocs = new TopDocs[aggregations.size()];
            for (int i = 0; i < shardDocs.length; i++) {
                InternalTopHits topHitsAgg = (InternalTopHits) aggregations.get(i);
                shardDocs[i] = topHitsAgg.topDocs;
                shardHits[i] = topHitsAgg.searchHits;
            }
            reducedTopDocs = TopDocs.merge(from, size, shardDocs, true);
        }

        final int[] tracker = new int[shardHits.length];
        SearchHit[] hits = new SearchHit[reducedTopDocs.scoreDocs.length];
        for (int i = 0; i < reducedTopDocs.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = reducedTopDocs.scoreDocs[i];
            int position;
            do {
                position = tracker[scoreDoc.shardIndex]++;
            } while (shardDocs[scoreDoc.shardIndex].scoreDocs[position] != scoreDoc);
            hits[i] = shardHits[scoreDoc.shardIndex].getAt(position);
        }
        return new InternalTopHits(name, this.from, this.size, reducedTopDocs, new SearchHits(hits, reducedTopDocs.totalHits,
                reducedTopDocs.getMaxScore()),
                pipelineAggregators(), getMetaData());
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        searchHits.toXContent(builder, params);
        return builder;
    }

    // Equals and hashcode implemented for testing round trips
    @Override
    protected boolean doEquals(Object obj) {
        InternalTopHits other = (InternalTopHits) obj;
        if (from != other.from) return false;
        if (size != other.size) return false;
        if (topDocs.totalHits != other.topDocs.totalHits) return false;
        if (topDocs.scoreDocs.length != other.topDocs.scoreDocs.length) return false;
        for (int d = 0; d < topDocs.scoreDocs.length; d++) {
            ScoreDoc thisDoc = topDocs.scoreDocs[d];
            ScoreDoc otherDoc = other.topDocs.scoreDocs[d];
            if (thisDoc.doc != otherDoc.doc) return false;
            if (Double.compare(thisDoc.score, otherDoc.score) != 0) return false;
            if (thisDoc.shardIndex != otherDoc.shardIndex) return false;
            if (thisDoc instanceof FieldDoc) {
                if (false == (otherDoc instanceof FieldDoc)) return false;
                FieldDoc thisFieldDoc = (FieldDoc) thisDoc;
                FieldDoc otherFieldDoc = (FieldDoc) otherDoc;
                if (thisFieldDoc.fields.length != otherFieldDoc.fields.length) return false;
                for (int f = 0; f < thisFieldDoc.fields.length; f++) {
                    if (false == thisFieldDoc.fields[f].equals(otherFieldDoc.fields[f])) return false;
                }
            }
        }
        return searchHits.equals(other.searchHits);
    }

    @Override
    protected int doHashCode() {
        int hashCode = from;
        hashCode = 31 * hashCode + size;
        hashCode = 31 * hashCode + Long.hashCode(topDocs.totalHits);
        for (int d = 0; d < topDocs.scoreDocs.length; d++) {
            ScoreDoc doc = topDocs.scoreDocs[d];
            hashCode = 31 * hashCode + doc.doc;
            hashCode = 31 * hashCode + Float.floatToIntBits(doc.score);
            hashCode = 31 * hashCode + doc.shardIndex;
            if (doc instanceof FieldDoc) {
                FieldDoc fieldDoc = (FieldDoc) doc;
                hashCode = 31 * hashCode + Arrays.hashCode(fieldDoc.fields);
            }
        }
        hashCode = 31 * hashCode + searchHits.hashCode();
        return hashCode;
    }
}
