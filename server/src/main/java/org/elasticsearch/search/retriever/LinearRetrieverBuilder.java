/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.rank.LinearRankDoc;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The {@code LinearRetrieverBuilder} supports the combination of different retrievers through a weighted linear combination.
 * For example, assume that we have retrievers r1 and r2, the final score of the {@code LinearRetrieverBuilder} is defined as
 * {@code score(r)=w1*score(r1) + w2*score(r2)}.
 * Each sub-retriever score can be normalized before being considered for the weighted linear sum, by setting the appropriate
 * normalizer parameter.
 *
 */
public class LinearRetrieverBuilder extends CompoundRetrieverBuilder<LinearRetrieverBuilder> {

    public static final String NAME = "linear_retriever";

    public static final NodeFeature LINEAR_RETRIEVER_SUPPORTED = new NodeFeature("linear_retriever_support");

    public static final ParseField RETRIEVERS_FIELD = new ParseField("retrievers");
    public static final ParseField RANK_WINDOW_SIZE_FIELD = new ParseField("rank_window_size");

    private final List<WrapperRetrieverBuilder> wrappedRetrievers;

    static final float DEFAULT_WEIGHT = 1f;
    static final ScoreNormalizer DEFAULT_NORMALIZER = ScoreNormalizer.IDENTITY;

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<LinearRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> {
            List<WrapperRetrieverBuilder> childRetrievers = (List<WrapperRetrieverBuilder>) args[0];
            List<RetrieverSource> innerRetrievers = childRetrievers.stream().map(r -> new RetrieverSource(r.retriever, null)).toList();
            int rankWindowSize = args[1] == null ? RankBuilder.DEFAULT_RANK_WINDOW_SIZE : (int) args[1];
            return new LinearRetrieverBuilder(childRetrievers, innerRetrievers, rankWindowSize);
        }
    );

    // public record WrappedRetriever(RetrieverBuilder retrieverBuilder, float weight, ScoreNormalizer normalizer) {}

    static {
        PARSER.declareObjectArray(constructorArg(), (p, c) -> {
            // float weight = -1f;
            // ScoreNormalizer normalizer = null;
            // RetrieverBuilder retrieverBuilder = null;
            // while (p.nextToken() != null && p.currentName() != null) {
            // String name = p.currentName();
            // switch (name) {
            // case "weight":
            // p.nextToken();
            // weight = p.floatValue();
            // break;
            // case "retriever":
            // p.nextToken();
            // p.nextToken();
            // retrieverBuilder = p.namedObject(RetrieverBuilder.class, p.currentName(), c);
            // c.trackRetrieverUsage(retrieverBuilder.getName());
            // p.nextToken();
            // break;
            // case "normalizer":
            // p.nextToken();
            // String normalizerName = p.text();
            // normalizer = ScoreNormalizer.find(normalizerName);
            // break;
            // default:
            // throw new ParsingException(p.getTokenLocation(), "Unknown key {" + name + "} provided");
            // }
            // }
            // ;
            // return new WrappedRetriever(retrieverBuilder, weight, normalizer);
            p.nextToken();
            WrapperRetrieverBuilder retrieverBuilder = WrapperRetrieverBuilder.fromXContent(p, c);
            p.nextToken();
            return retrieverBuilder;
        }, RETRIEVERS_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        RetrieverBuilder.declareBaseParserFields(NAME, PARSER);
    }

    public static LinearRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        if (context.clusterSupportsFeature(LINEAR_RETRIEVER_SUPPORTED) == false) {
            throw new ParsingException(parser.getTokenLocation(), "unknown retriever [" + NAME + "]");
        }
        return PARSER.apply(parser, context);
    }

    protected LinearRetrieverBuilder(
        List<WrapperRetrieverBuilder> wrappedRetrievers,
        List<RetrieverSource> innerRetrievers,
        int rankWindowSize
    ) {
        super(innerRetrievers, rankWindowSize);
        this.wrappedRetrievers = wrappedRetrievers;
    }

    @Override
    protected LinearRetrieverBuilder clone(List<RetrieverSource> newChildRetrievers, List<QueryBuilder> newPreFilterQueryBuilders) {
        LinearRetrieverBuilder clone = new LinearRetrieverBuilder(wrappedRetrievers, newChildRetrievers, rankWindowSize);
        clone.preFilterQueryBuilders = newPreFilterQueryBuilders;
        return clone;
    }

    @Override
    protected RankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults) {
        Map<RankDoc.RankKey, LinearRankDoc> docsToRankResults = Maps.newMapWithExpectedSize(rankWindowSize);
        for (int resIndex = 0; resIndex < rankResults.size(); resIndex++) {
            ScoreDoc[] originalScoreDocs = rankResults.get(resIndex);
            ScoreDoc[] normalizedScoreDocs = wrappedRetrievers.get(resIndex).normalizer.normalizeScores(originalScoreDocs);
            for (int i = 0; i < normalizedScoreDocs.length; i++) {
                int finalResIndex = resIndex;
                int finalI = i;
                docsToRankResults.compute(new RankDoc.RankKey(originalScoreDocs[i].doc, originalScoreDocs[i].shardIndex), (key, value) -> {
                    if (value == null) {
                        value = new LinearRankDoc(
                            originalScoreDocs[finalI].doc,
                            0,
                            originalScoreDocs[finalI].shardIndex,
                            rankResults.size()
                        );
                    }
                    value.scores[finalResIndex] = normalizedScoreDocs[finalI].score;
                    value.weights[finalResIndex] = wrappedRetrievers.get(finalResIndex).weight;
                    value.normalizers[finalResIndex] = wrappedRetrievers.get(finalResIndex).normalizer.name();
                    value.score += wrappedRetrievers.get(finalResIndex).weight * normalizedScoreDocs[finalI].score;
                    return value;
                });
            }
        }
        // sort the results based on rrf score, tiebreaker based on smaller doc id
        LinearRankDoc[] sortedResults = docsToRankResults.values().toArray(LinearRankDoc[]::new);
        Arrays.sort(sortedResults);
        // trim the results if needed, otherwise each shard will always return `rank_window_size` results.
        LinearRankDoc[] topResults = new LinearRankDoc[Math.min(rankWindowSize, sortedResults.length)];
        for (int rank = 0; rank < topResults.length; ++rank) {
            topResults[rank] = sortedResults[rank];
            topResults[rank].rank = rank + 1;
        }
        return topResults;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {

    }

    enum ScoreNormalizer {
        IDENTITY("identity") {
            @Override
            public ScoreDoc[] normalizeScores(ScoreDoc[] docs) {
                // no-op
                return docs;
            }
        },
        MINMAX("minmax") {
            @Override
            public ScoreDoc[] normalizeScores(ScoreDoc[] docs) {
                // create a new array to avoid changing ScoreDocs in place
                ScoreDoc[] scoreDocs = new ScoreDoc[docs.length];
                // to avoid 0 scores
                float epsilon = Float.MIN_NORMAL;
                float min = Float.MAX_VALUE;
                float max = Float.MIN_VALUE;
                for (ScoreDoc rd : docs) {
                    if (rd.score > max) {
                        max = rd.score;
                    }
                    if (rd.score < min) {
                        min = rd.score;
                    }
                }
                for (int i = 0; i < docs.length; i++) {
                    float score = epsilon + ((docs[i].score - min) / (max - min));
                    scoreDocs[i] = new ScoreDoc(docs[i].doc, score, docs[i].shardIndex);
                }
                return scoreDocs;
            }
        };

        private final String name;

        ScoreNormalizer(String name) {
            this.name = name;
        }

        abstract ScoreDoc[] normalizeScores(ScoreDoc[] docs);

        static ScoreNormalizer find(String name) {
            for (ScoreNormalizer normalizer : values()) {
                if (normalizer.name.equalsIgnoreCase(name)) {
                    return normalizer;
                }
            }
            throw new IllegalArgumentException(
                "Unknown normalizer [" + name + "] provided. Supported values are: " + Arrays.stream(values()).map(Enum::name).toList()
            );
        }
    }
}
