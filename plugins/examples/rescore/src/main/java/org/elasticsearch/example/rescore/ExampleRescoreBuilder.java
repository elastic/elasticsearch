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

package org.elasticsearch.example.rescore;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.search.rescore.RescorerBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Example rescorer that multiplies the score of the hit by some factor and doesn't resort them.
 */
public class ExampleRescoreBuilder extends RescorerBuilder<ExampleRescoreBuilder> {
    public static final String NAME = "example";

    private final float factor;

    public ExampleRescoreBuilder(float factor) {
        this.factor = factor;
    }

    ExampleRescoreBuilder(StreamInput in) throws IOException {
        super(in);
        factor = in.readFloat();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeFloat(factor);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public RescorerBuilder<ExampleRescoreBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
        return this;
    }

    private static final ParseField FACTOR = new ParseField("factor");
    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FACTOR.getPreferredName(), factor);
    }

    private static final ConstructingObjectParser<ExampleRescoreBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> new ExampleRescoreBuilder((float) args[0]));
    static {
        PARSER.declareFloat(constructorArg(), FACTOR);
    }
    public static ExampleRescoreBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public RescoreContext innerBuildContext(RescoreContextSupport support) throws IOException {
        return new RescoreContext(support, new Rescorer() {
            @Override
            public String name() {
                return NAME;
            }

            @Override
            public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {
                int end = Math.min(topDocs.scoreDocs.length, rescoreContext.getWindowSize());
                for (int i = 0; i < end; i++) {
                    topDocs.scoreDocs[i].score *= factor;
                }
                return topDocs;
            }

            @Override
            public Explanation explain(int topLevelDocId, SearchContext context, RescoreContext rescoreContext,
                    Explanation sourceExplanation) throws IOException {
                return Explanation.match(factor, "test", singletonList(sourceExplanation));
            }

            @Override
            public void extractTerms(SearchContext context, RescoreContext rescoreContext, Set<Term> termsSet) {
            }
        });
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        ExampleRescoreBuilder other = (ExampleRescoreBuilder) obj;
        return factor == other.factor;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), factor);
    }

    float factor() {
        return factor;
    }
}
