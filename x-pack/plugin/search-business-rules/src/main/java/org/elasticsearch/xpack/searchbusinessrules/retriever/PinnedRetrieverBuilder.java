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
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
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

    public static final NodeFeature PINNED_RETRIEVER_FEATURE = new NodeFeature(NAME);

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
        RetrieverBuilder.declareBaseParserFields(PARSER);
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
        boolean hasIds = ids != null && ids.isEmpty() == false;
        boolean hasDocs = docs != null && docs.isEmpty() == false;

        if (hasIds && hasDocs) {
            throw new IllegalArgumentException("Both 'ids' and 'docs' cannot be specified at the same time");
        }
        if (hasIds == false && hasDocs == false) {
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
            throw new IllegalArgumentException("Pinned retriever only supports sorting by score. Custom sorting is not allowed.");
        }
    }

    public PinnedRetrieverBuilder(List<String> ids, List<SpecifiedDocument> docs, RetrieverBuilder retrieverBuilder, int rankWindowSize) {
        super(new ArrayList<>(), rankWindowSize);
        validateIdsAndDocs(ids, docs);
        this.ids = ids != null ? ids : new ArrayList<>();
        this.docs = docs != null ? docs : new ArrayList<>();
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

    public int rankWindowSize() {
        return rankWindowSize;
    }

    /**
     * Creates a PinnedQueryBuilder with the appropriate pinned documents.
     * Prioritizes docs over ids if both are present.
     *
     * @param baseQuery the base query to pin documents to
     * @return a PinnedQueryBuilder or the original query if no pinned documents
     */
    private QueryBuilder createPinnedQuery(QueryBuilder baseQuery) {
        if (baseQuery == null) {
            baseQuery = new MatchAllQueryBuilder();
        }
        if (docs.isEmpty() == false) {
            return new PinnedQueryBuilder(baseQuery, docs.toArray(new SpecifiedDocument[0]));
        } else if (ids.isEmpty() == false) {
            return new PinnedQueryBuilder(baseQuery, ids.toArray(new String[0]));
        } else {
            return baseQuery;
        }
    }

    @Override
    protected SearchSourceBuilder finalizeSourceBuilder(SearchSourceBuilder source) {
        validateSort(source);
        source.query(createPinnedQuery(source.query()));
        return source;
    }

    @Override
    public void doToXContent(XContentBuilder builder, Params params) throws IOException {
        if (ids != null && ids.isEmpty() == false) {
            builder.array(IDS_FIELD.getPreferredName(), ids.toArray());
        }
        if (docs != null && docs.isEmpty() == false) {
            builder.startArray(DOCS_FIELD.getPreferredName());
            for (SpecifiedDocument doc : docs) {
                builder.value(doc);
            }
            builder.endArray();
        }
        builder.field(RETRIEVER_FIELD.getPreferredName(), innerRetrievers.getFirst().retriever());
        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);
    }

    @Override
    protected PinnedRetrieverBuilder clone(List<RetrieverSource> newChildRetrievers, List<QueryBuilder> newPreFilterQueryBuilders) {
        return new PinnedRetrieverBuilder(ids, docs, newChildRetrievers, rankWindowSize, retrieverName, newPreFilterQueryBuilders);
    }

    @Override
    protected RankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults, boolean explain) {
        assert rankResults.size() == 1;
        ScoreDoc[] scoreDocs = rankResults.getFirst();
        RankDoc[] rankDocs = new RankDoc[scoreDocs.length];
        for (int i = 0; i < scoreDocs.length; i++) {
            ScoreDoc scoreDoc = scoreDocs[i];
            boolean isPinned = docs.stream().anyMatch(doc -> doc.id().equals(String.valueOf(scoreDoc.doc)))
                || ids.contains(String.valueOf(scoreDoc.doc));
            String pinnedBy = docs.stream().anyMatch(doc -> doc.id().equals(String.valueOf(scoreDoc.doc))) ? "docs" : "ids";
            rankDocs[i] = new PinnedRankDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex, isPinned, pinnedBy);
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
            QueryBuilder baseQuery = in.explainQuery();
            return createPinnedQuery(baseQuery);
        }
    }
}
