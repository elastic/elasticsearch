/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever.rankdoc;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

public class RankDocsQueryBuilder extends AbstractQueryBuilder<RankDocsQueryBuilder> {

    public static final String NAME = "rank_docs_query";

    private final RankDoc[] rankDocs;

    public RankDocsQueryBuilder(RankDoc[] rankDocs) {
        this.rankDocs = rankDocs;
    }

    public RankDocsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.rankDocs = in.readArray(c -> c.readNamedWriteable(RankDoc.class), RankDoc[]::new);
    }

    RankDoc[] rankDocs() {
        return rankDocs;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeArray(StreamOutput::writeNamedWriteable, rankDocs);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        RankDoc[] shardRankDocs = Arrays.stream(rankDocs)
            .filter(r -> r.shardIndex == context.getShardRequestIndex())
            .sorted(Comparator.comparingInt(r -> r.doc))
            .toArray(RankDoc[]::new);
        IndexReader reader = context.getIndexReader();
        int[] segmentStarts = findSegmentStarts(reader, shardRankDocs);
        return new RankDocsQuery(shardRankDocs, segmentStarts, reader.getContext().id());
    }

    private static int[] findSegmentStarts(IndexReader reader, RankDoc[] docs) {
        int[] starts = new int[reader.leaves().size() + 1];
        starts[starts.length - 1] = docs.length;
        if (starts.length == 2) {
            return starts;
        }
        int resultIndex = 0;
        for (int i = 1; i < starts.length - 1; i++) {
            int upper = reader.leaves().get(i).docBase;
            resultIndex = Arrays.binarySearch(docs, resultIndex, docs.length, upper, (a, b) -> Integer.compare(((RankDoc) a).doc, (int) b));
            if (resultIndex < 0) {
                resultIndex = -1 - resultIndex;
            }
            starts[i] = resultIndex;
        }
        return starts;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray("rank_docs");
        for (RankDoc doc : rankDocs) {
            builder.startObject();
            doc.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
    }

    @Override
    protected boolean doEquals(RankDocsQueryBuilder other) {
        return Arrays.equals(rankDocs, other.rankDocs);
    }

    @Override
    protected int doHashCode() {
        return Arrays.hashCode(rankDocs);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.RANK_DOCS_RETRIEVER;
    }
}
