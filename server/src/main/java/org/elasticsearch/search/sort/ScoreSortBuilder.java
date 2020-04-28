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

package org.elasticsearch.search.sort;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.Objects;

/**
 * A sort builder allowing to sort by score.
 */
public class ScoreSortBuilder extends SortBuilder<ScoreSortBuilder> {

    public static final String NAME = "_score";
    private static final SortFieldAndFormat SORT_SCORE = new SortFieldAndFormat(
            new SortField(null, SortField.Type.SCORE), DocValueFormat.RAW);
    private static final SortFieldAndFormat SORT_SCORE_REVERSE = new SortFieldAndFormat(
            new SortField(null, SortField.Type.SCORE, true), DocValueFormat.RAW);

    /**
     * Build a ScoreSortBuilder default to descending sort order.
     */
    public ScoreSortBuilder() {
        // order defaults to desc when sorting on the _score
        order(SortOrder.DESC);
    }

    /**
     * Read from a stream.
     */
    public ScoreSortBuilder(StreamInput in) throws IOException {
        order(SortOrder.readFromStream(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        order.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(ORDER_FIELD.getPreferredName(), order);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    /**
     * Creates a new {@link ScoreSortBuilder} from the query held by the {@link XContentParser} in
     * {@link org.elasticsearch.common.xcontent.XContent} format.
     *
     * @param parser the input parser. The state on the parser contained in this context will be changed as a side effect of this
     *        method call
     * @param fieldName in some sort syntax variations the field name precedes the xContent object that specifies further parameters, e.g.
     *        in '{ "foo": { "order" : "asc"} }'. When parsing the inner object, the field name can be passed in via this argument
     */
    public static ScoreSortBuilder fromXContent(XContentParser parser, String fieldName) {
        return PARSER.apply(parser, null);
    }

    private static final ObjectParser<ScoreSortBuilder, Void> PARSER = new ObjectParser<>(NAME, ScoreSortBuilder::new);

    static {
        PARSER.declareString((builder, order) -> builder.order(SortOrder.fromString(order)), ORDER_FIELD);
    }

    @Override
    public SortFieldAndFormat build(QueryShardContext context) {
        if (order == SortOrder.DESC) {
            return SORT_SCORE;
        } else {
            return SORT_SCORE_REVERSE;
        }
    }

    @Override
    public BucketedSort buildBucketedSort(QueryShardContext context, int bucketSize, BucketedSort.ExtraData extra) throws IOException {
        return new BucketedSort.ForFloats(context.bigArrays(), order, DocValueFormat.RAW, bucketSize, extra) {
            @Override
            public boolean needsScores() { return true; }

            @Override
            public Leaf forLeaf(LeafReaderContext ctx) throws IOException {
                return new BucketedSort.ForFloats.Leaf(ctx) {
                    private Scorable scorer;
                    private float score;

                    @Override
                    public void setScorer(Scorable scorer) {
                        this.scorer = scorer;
                    }

                    @Override
                    protected boolean advanceExact(int doc) throws IOException {
                        assert doc == scorer.docID() : "expected scorer to be on [" + doc + "] but was on [" + scorer.docID() + "]";
                        /* We will never be called by documents that don't match the
                         * query and they'll all have a score, thus `true`. */
                        score = scorer.score();
                        return true;
                    }

                    @Override
                    protected float docValue() {
                        return score;
                    }
                };
            }
        };
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        ScoreSortBuilder other = (ScoreSortBuilder) object;
        return Objects.equals(order, other.order);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.order);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public ScoreSortBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        return this;
    }
}
