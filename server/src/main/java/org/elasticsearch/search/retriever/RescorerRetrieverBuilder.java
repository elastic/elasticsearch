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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.search.builder.SearchSourceBuilder.RESCORE_FIELD;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A {@link CompoundRetrieverBuilder} that re-scores only the results produced by its child retriever.
 */
public final class RescorerRetrieverBuilder extends CompoundRetrieverBuilder<RescorerRetrieverBuilder> {

    public static final String NAME = "rescorer";
    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<RescorerRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        NAME,
        args -> new RescorerRetrieverBuilder((RetrieverBuilder) args[0], (List<RescorerBuilder<?>>) args[1])
    );

    static {
        PARSER.declareNamedObject(constructorArg(), (parser, context, n) -> {
            RetrieverBuilder innerRetriever = parser.namedObject(RetrieverBuilder.class, n, context);
            context.trackRetrieverUsage(innerRetriever.getName());
            return innerRetriever;
        }, RETRIEVER_FIELD);
        PARSER.declareField(constructorArg(), (parser, context) -> {
            if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                List<RescorerBuilder<?>> rescorers = new ArrayList<>();
                while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    rescorers.add(RescorerBuilder.parseFromXContent(parser, name -> context.trackRescorerUsage(name)));
                }
                return rescorers;
            } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                return List.of(RescorerBuilder.parseFromXContent(parser, name -> context.trackRescorerUsage(name)));
            } else {
                throw new IllegalArgumentException(
                    "Unknown format for [rescorer.rescore], expects an object or an array of objects, got: " + parser.currentToken()
                );
            }
        }, RESCORE_FIELD, ObjectParser.ValueType.OBJECT_ARRAY);
        RetrieverBuilder.declareBaseParserFields(PARSER);
    }

    public static RescorerRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        try {
            return PARSER.apply(parser, context);
        } catch (Exception e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    private final List<RescorerBuilder<?>> rescorers;

    public RescorerRetrieverBuilder(RetrieverBuilder retriever, List<RescorerBuilder<?>> rescorers) {
        super(List.of(RetrieverSource.from(retriever)), extractMinWindowSize(rescorers));
        if (rescorers.isEmpty()) {
            throw new IllegalArgumentException("Missing rescore definition");
        }
        this.rescorers = rescorers;
    }

    private RescorerRetrieverBuilder(RetrieverSource retriever, List<RescorerBuilder<?>> rescorers) {
        super(List.of(retriever), extractMinWindowSize(rescorers));
        this.rescorers = rescorers;
    }

    /**
     * The minimum window size is used as the {@link CompoundRetrieverBuilder#rankWindowSize},
     * the final number of top documents to return in this retriever.
     */
    private static int extractMinWindowSize(List<RescorerBuilder<?>> rescorers) {
        int windowSize = Integer.MAX_VALUE;
        for (var rescore : rescorers) {
            windowSize = Math.min(rescore.windowSize() == null ? RescorerBuilder.DEFAULT_WINDOW_SIZE : rescore.windowSize(), windowSize);
        }
        return windowSize;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public ParseField getRankWindowSizeField() {
        return RescorerBuilder.WINDOW_SIZE_FIELD;
    }

    @Override
    protected SearchSourceBuilder finalizeSourceBuilder(SearchSourceBuilder source) {
        /**
         * The re-scorer is passed downstream because this query operates only on
         * the top documents retrieved by the child retriever.
         *
         * - If the sub-retriever is a {@link CompoundRetrieverBuilder}, only the top
         *   documents are re-scored since they are already determined at this stage.
         * - For other retrievers that do not require a rewrite, the re-scorer's window
         *   size is applied per shard. As a result, more documents are re-scored
         *   compared to the final top documents produced by these retrievers in isolation.
         */
        for (var rescorer : rescorers) {
            source.addRescorer(rescorer);
        }
        return source;
    }

    @Override
    public void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(RETRIEVER_FIELD.getPreferredName(), innerRetrievers.getFirst().retriever());
        builder.startArray(RESCORE_FIELD.getPreferredName());
        for (RescorerBuilder<?> rescorer : rescorers) {
            rescorer.toXContent(builder, params);
        }
        builder.endArray();
    }

    @Override
    protected RescorerRetrieverBuilder clone(List<RetrieverSource> newChildRetrievers, List<QueryBuilder> newPreFilterQueryBuilders) {
        var newInstance = new RescorerRetrieverBuilder(newChildRetrievers.get(0), rescorers);
        newInstance.preFilterQueryBuilders = newPreFilterQueryBuilders;
        newInstance.retrieverName = retrieverName;
        return newInstance;
    }

    @Override
    protected RankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults, boolean explain) {
        assert rankResults.size() == 1;
        ScoreDoc[] scoreDocs = rankResults.getFirst();
        RankDoc[] rankDocs = new RankDoc[scoreDocs.length];
        for (int i = 0; i < scoreDocs.length; i++) {
            ScoreDoc scoreDoc = scoreDocs[i];
            rankDocs[i] = new RankDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex);
            rankDocs[i].rank = i + 1;
        }
        return rankDocs;
    }

    @Override
    public boolean doEquals(Object o) {
        RescorerRetrieverBuilder that = (RescorerRetrieverBuilder) o;
        return super.doEquals(o) && Objects.equals(rescorers, that.rescorers);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(super.doHashCode(), rescorers);
    }
}
