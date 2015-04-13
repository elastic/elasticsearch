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

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalMetricsAggregation;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;

import java.io.IOException;
import java.util.List;

/**
 */
public class InternalTopHits extends InternalMetricsAggregation implements TopHits {

    public static final InternalAggregation.Type TYPE = new Type("top_hits");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalTopHits readResult(StreamInput in) throws IOException {
            InternalTopHits buckets = new InternalTopHits();
            buckets.readFrom(in);
            return buckets;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private int from;
    private int size;
    private TopDocs topDocs;
    private InternalSearchHits searchHits;

    InternalTopHits() {
    }

    public InternalTopHits(String name, int from, int size, TopDocs topDocs, InternalSearchHits searchHits) {
        this.name = name;
        this.from = from;
        this.size = size;
        this.topDocs = topDocs;
        this.searchHits = searchHits;
    }


    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public SearchHits getHits() {
        return searchHits;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        InternalSearchHits[] shardHits = new InternalSearchHits[aggregations.size()];

        final TopDocs reducedTopDocs;
        final TopDocs[] shardDocs;

        try {
            if (topDocs instanceof TopFieldDocs) {
                Sort sort = new Sort(((TopFieldDocs) topDocs).fields);
                shardDocs = new TopFieldDocs[aggregations.size()];
                for (int i = 0; i < shardDocs.length; i++) {
                    InternalTopHits topHitsAgg = (InternalTopHits) aggregations.get(i);
                    shardDocs[i] = (TopFieldDocs) topHitsAgg.topDocs;
                    shardHits[i] = topHitsAgg.searchHits;
                }
                reducedTopDocs = TopDocs.merge(sort, from, size, (TopFieldDocs[]) shardDocs);
            } else {
                shardDocs = new TopDocs[aggregations.size()];
                for (int i = 0; i < shardDocs.length; i++) {
                    InternalTopHits topHitsAgg = (InternalTopHits) aggregations.get(i);
                    shardDocs[i] = topHitsAgg.topDocs;
                    shardHits[i] = topHitsAgg.searchHits;
                }
                reducedTopDocs = TopDocs.merge(from, size, shardDocs);
            }

            final int[] tracker = new int[shardHits.length];
            InternalSearchHit[] hits = new InternalSearchHit[reducedTopDocs.scoreDocs.length];
            for (int i = 0; i < reducedTopDocs.scoreDocs.length; i++) {
                ScoreDoc scoreDoc = reducedTopDocs.scoreDocs[i];
                int position;
                do {
                    position = tracker[scoreDoc.shardIndex]++;
                } while (shardDocs[scoreDoc.shardIndex].scoreDocs[position] != scoreDoc);
                hits[i] = (InternalSearchHit) shardHits[scoreDoc.shardIndex].getAt(position);
            }
            return new InternalTopHits(name, from, size, reducedTopDocs, new InternalSearchHits(hits, reducedTopDocs.totalHits, reducedTopDocs.getMaxScore()));
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else {
            throw new ElasticsearchIllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        from = in.readVInt();
        size = in.readVInt();
        topDocs = Lucene.readTopDocs(in);
        assert topDocs != null;
        searchHits = InternalSearchHits.readSearchHits(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(from);
        out.writeVInt(size);
        Lucene.writeTopDocs(out, topDocs);
        searchHits.writeTo(out);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        searchHits.toXContent(builder, params);
        return builder;
    }
}
