/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules.retriever;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RankDocsQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilderWrapper;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent.Params;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder;
import org.elasticsearch.xpack.searchbusinessrules.SpecifiedDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A pinned retriever applies pinned documents to the underlying retriever.
 * This retriever will rewrite to a PinnedQueryBuilder.
 */
public final class PinnedRetrieverBuilder extends CompoundRetrieverBuilder<PinnedRetrieverBuilder> {

    public static final String NAME = "pinned";

    public static final ParseField IDS_FIELD = new ParseField("ids");
    public static final ParseField DOCS_FIELD = new ParseField("docs");
    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");

    public static final NodeFeature PINNED_RETRIEVER_FEATURE = new NodeFeature("pinned_retriever_supported");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<PinnedRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        NAME,
        args -> {
            List<String> ids = (List<String>) args[0];
            List<SpecifiedDocument> docs = (List<SpecifiedDocument>) args[1];
            RetrieverBuilder retrieverBuilder = (RetrieverBuilder) args[2];
            int rankWindowSize = args[3] == null ? DEFAULT_RANK_WINDOW_SIZE : (int) args[3];
            return new PinnedRetrieverBuilder(ids, docs, retrieverBuilder, rankWindowSize);
        }
    );

    static {
        PARSER.declareStringArray(optionalConstructorArg(), IDS_FIELD);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> SpecifiedDocument.PARSER.apply(p, null), DOCS_FIELD);
        PARSER.declareNamedObject(constructorArg(), (p, c, n) -> {
            RetrieverBuilder innerRetriever = p.namedObject(RetrieverBuilder.class, n, c);
            c.trackRetrieverUsage(innerRetriever.getName());
            return innerRetriever;
        }, RETRIEVER_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        RetrieverBuilder.declareBaseParserFields(NAME, PARSER);
    }

    public static PinnedRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        try {
            return PARSER.apply(parser, context);
        } catch (Exception e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    private final List<String> ids;
    private final List<SpecifiedDocument> docs;

    private void validateIdsAndDocs(List<String> ids, List<SpecifiedDocument> docs) {
        if (ids != null && docs != null) {
            throw new IllegalArgumentException("Both 'ids' and 'docs' cannot be specified at the same time");
        }

        boolean validIds = ids != null && ids.isEmpty() == false;
        boolean validDocs = docs != null && docs.isEmpty() == false;

        if (validIds == false && validDocs == false) {
            throw new IllegalArgumentException("Either 'ids' or 'docs' must be provided and non-empty for pinned retriever");
        }
    }

    private void validateSort(SearchSourceBuilder source) {
        List<SortBuilder<?>> sorts = source.sorts();
        if (sorts == null || sorts.isEmpty()) {
            return;
        }
        for (SortBuilder<?> sort : sorts) {
            if (sort instanceof ScoreSortBuilder) {
                continue;
            }
            if (sort instanceof FieldSortBuilder) {
                FieldSortBuilder fieldSort = (FieldSortBuilder) sort;
                if (ShardDocSortField.NAME.equals(fieldSort.getFieldName())) {
                    continue;
                }
            }
            throw new IllegalArgumentException(
                "[" + NAME + "] retriever only supports sorting by score, invalid sort criterion: " + sort.toString()
            );
        }
    }

    public PinnedRetrieverBuilder(List<String> ids, List<SpecifiedDocument> docs, RetrieverBuilder retrieverBuilder, int rankWindowSize) {
        super(new ArrayList<>(), rankWindowSize);
        validateIdsAndDocs(ids, docs);
        this.ids = ids;
        this.docs = docs;
        addChild(new PinnedRetrieverBuilderWrapper(retrieverBuilder));
    }

    public PinnedRetrieverBuilder(
        List<String> ids,
        List<SpecifiedDocument> docs,
        List<RetrieverSource> retrieverSource,
        int rankWindowSize,
        String retrieverName,
        List<QueryBuilder> preFilterQueryBuilders
    ) {
        super(retrieverSource, rankWindowSize);
        validateIdsAndDocs(ids, docs);
        this.ids = ids;
        this.docs = docs;
        this.retrieverName = retrieverName;
        this.preFilterQueryBuilders = preFilterQueryBuilders;
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Creates a PinnedQueryBuilder with the appropriate pinned documents.
     *
     * @param baseQuery the base query to pin documents to
     * @return a PinnedQueryBuilder
     * @throws IllegalArgumentException if baseQuery is null
     */
    private QueryBuilder createPinnedQuery(QueryBuilder baseQuery) {
        Objects.requireNonNull(baseQuery, "pinned retriever requires retriever with associated query");

        if (docs != null && docs.isEmpty() == false) {
            return new PinnedQueryBuilder(baseQuery, docs.toArray(new SpecifiedDocument[0]));
        }
        return new PinnedQueryBuilder(baseQuery, ids.toArray(new String[0]));
    }

    @Override
    protected SearchSourceBuilder finalizeSourceBuilder(SearchSourceBuilder source) {
        validateSort(source);
        QueryBuilder underlyingQuery = source.query();
        if (underlyingQuery == null) {
            throw new IllegalArgumentException("pinned retriever requires retriever with associated query");
        }
        source.query(createPinnedQuery(underlyingQuery));
        return source;
    }

    @Override
    public void doToXContent(XContentBuilder builder, Params params) throws IOException {
        if (ids != null) {
            builder.array(IDS_FIELD.getPreferredName(), ids.toArray());
        }
        if (docs != null) {
            builder.startArray(DOCS_FIELD.getPreferredName());
            for (SpecifiedDocument doc : docs) {
                builder.value(doc);
            }
            builder.endArray();
        }
        builder.field(RETRIEVER_FIELD.getPreferredName(), innerRetrievers.get(0).retriever());
        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);
    }

    @Override
    protected PinnedRetrieverBuilder clone(List<RetrieverSource> newChildRetrievers, List<QueryBuilder> newPreFilterQueryBuilders) {
        return new PinnedRetrieverBuilder(ids, docs, newChildRetrievers, rankWindowSize, retrieverName, newPreFilterQueryBuilders);
    }

    @Override
    protected RankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults, boolean explain) {
        assert rankResults.size() == 1;
        ScoreDoc[] scoreDocs = rankResults.get(0);
        RankDoc[] rankDocs = new RankDoc[scoreDocs.length];
        for (int i = 0; i < scoreDocs.length; i++) {
            ScoreDoc scoreDoc = scoreDocs[i];

            if (explain) {
                boolean isPinned = scoreDoc.score > PinnedQueryBuilder.MAX_ORGANIC_SCORE;
                if (isPinned) {
                    String pinnedBy = (this.ids != null && this.ids.isEmpty() == false) ? "ids" : "docs";
                    rankDocs[i] = new PinnedRankDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex, true);
                } else {
                    rankDocs[i] = new RankDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex);
                }
            } else {
                rankDocs[i] = new RankDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex);
            }

            rankDocs[i].rank = i + 1;
        }
        return rankDocs;
    }

    @Override
    public boolean doEquals(Object o) {
        PinnedRetrieverBuilder that = (PinnedRetrieverBuilder) o;
        return super.doEquals(o) && Objects.equals(ids, that.ids) && Objects.equals(docs, that.docs);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(super.doHashCode(), ids, docs);
    }

    /**
     * We need to wrap the PinnedRetrieverBuilder in order to ensure that the top docs query that is generated
     * by this retriever correctly generates and executes a Pinned query.
     */
    class PinnedRetrieverBuilderWrapper extends RetrieverBuilderWrapper<PinnedRetrieverBuilderWrapper> {
        protected PinnedRetrieverBuilderWrapper(RetrieverBuilder in) {
            super(in);
        }

        @Override
        protected PinnedRetrieverBuilderWrapper clone(RetrieverBuilder in) {
            return new PinnedRetrieverBuilderWrapper(in);
        }

        @Override
        public QueryBuilder topDocsQuery() {
            return createPinnedQuery(in.topDocsQuery());
        }

        @Override
        public QueryBuilder explainQuery() {
            RankDoc[] currentRankDocs = in.getRankDocs();
            if (currentRankDocs == null) {
                return in.explainQuery();
            }
            return new RankDocsQueryBuilder(currentRankDocs, new QueryBuilder[] { in.explainQuery() }, true);
        }
    }
}
