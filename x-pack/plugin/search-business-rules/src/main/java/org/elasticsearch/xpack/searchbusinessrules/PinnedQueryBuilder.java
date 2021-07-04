/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.CappedScoreQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A query that will promote selected documents (identified by ID) above matches produced by an "organic" query. In practice, some upstream
 * system will identify the promotions associated with a user's query string and use this object to ensure these are "pinned" to the top of
 * the other search results.
 */
public class PinnedQueryBuilder extends AbstractQueryBuilder<PinnedQueryBuilder> {
    public static final String NAME = "pinned";
    public static final int MAX_NUM_PINNED_HITS = 100;

    private static final ParseField IDS_FIELD = new ParseField("ids");
    private static final ParseField DOCUMENTS_FIELD = new ParseField("documents");
    public static final ParseField ORGANIC_QUERY_FIELD = new ParseField("organic");

    private final Optional<List<String>> ids;
    private final Optional<List<Item>> documents;
    private QueryBuilder organicQuery;

    // Organic queries will have their scores capped to this number range,
    // We reserve the highest float exponent for scores of pinned queries
    private static final float MAX_ORGANIC_SCORE = Float.intBitsToFloat((0xfe << 23)) - 1;

    /**
     * A single item to be used for a {@link PinnedQueryBuilder}.
     */
    public static final class Item implements ToXContentObject, NamedWriteable {
        public static final String NAME = "item";

        private static final ParseField INDEX_FIELD = new ParseField("_index");
        private static final ParseField ID_FIELD = new ParseField("_id");

        private final String index;
        private final String id;

        /**
         * Constructor for a given item request
         *
         * @param index the index where the document is located
         * @param id and its id
         */
        public Item(@Nullable String index, String id) {
            if (id == null) {
                throw new IllegalArgumentException("Item requires id to be non-null");
            }
            this.index = index;
            this.id = id;
        }

        /**
         * Read from a stream.
         */
        Item(StreamInput in) throws IOException {
            index = in.readOptionalString();
            id = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(index);
            out.writeString(id);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (this.index != null) {
                builder.field(INDEX_FIELD.getPreferredName(), this.index);
            }
            builder.field(ID_FIELD.getPreferredName(), this.id);
            return builder.endObject();
        }

        private static final ConstructingObjectParser<Item, Void> PARSER = new ConstructingObjectParser<>(
            NAME,
            a -> new Item((String) a[1], (String) a[0])
        );

        static {
            PARSER.declareString(constructorArg(), ID_FIELD);
            PARSER.declareString(optionalConstructorArg(), INDEX_FIELD);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public String toString() {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.prettyPrint();
                toXContent(builder, EMPTY_PARAMS);
                return Strings.toString(builder);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o instanceof Item) == false) return false;
            Item other = (Item) o;
            return Objects.equals(index, other.index) && Objects.equals(id, other.id);
        }
    }

    public PinnedQueryBuilder(QueryBuilder organicQuery) {
        this(organicQuery, new ArrayList<>(), null);
    }

    public PinnedQueryBuilder(QueryBuilder organicQuery, String... ids) {
        this(organicQuery, Arrays.asList(ids), null);
    }

    public PinnedQueryBuilder(QueryBuilder organicQuery, Item... documents) {
        this(organicQuery, null, Arrays.asList(documents));
    }

    /**
     * Creates a new PinnedQueryBuilder
     */
    public PinnedQueryBuilder(QueryBuilder organicQuery, List<String> ids, List<Item> documents) {
        if (organicQuery == null) {
            throw new IllegalArgumentException("[" + NAME + "] organicQuery cannot be null");
        }
        this.organicQuery = organicQuery;
        if (ids == null && documents == null) {
            throw new IllegalArgumentException("[" + NAME + "] ids and documents cannot both be null");
        }
        if (ids != null && documents != null) {
            throw new IllegalArgumentException("[" + NAME + "] ids and documents cannot both be used");
        }
        if (ids != null) {
            if (ids.size() > MAX_NUM_PINNED_HITS) {
                throw new IllegalArgumentException(
                    "[" + NAME + "] Max of " + MAX_NUM_PINNED_HITS + " ids exceeded: " + ids.size() + " provided."
                );
            }
            LinkedHashSet<String> deduped = new LinkedHashSet<>();
            for (String id : ids) {
                if (id == null) {
                    throw new IllegalArgumentException("[" + NAME + "] id cannot be null");
                }
                if (deduped.add(id) == false) {
                    throw new IllegalArgumentException("[" + NAME + "] duplicate id found in list: " + id);
                }
            }
        }
        if (documents != null) {
            if (documents.size() > MAX_NUM_PINNED_HITS) {
                throw new IllegalArgumentException(
                    "[" + NAME + "] Max of " + MAX_NUM_PINNED_HITS + " documents exceeded: " + documents.size() + " provided."
                );
            }
            LinkedHashSet<Item> deduped = new LinkedHashSet<>();
            for (Item document : documents) {
                if (document == null) {
                    throw new IllegalArgumentException("[" + NAME + "] document cannot be null");
                }
                if (deduped.add(document) == false) {
                    throw new IllegalArgumentException("[" + NAME + "] duplicate document found in list: " + document);
                }
            }
        }
        this.ids = Optional.ofNullable(ids);
        this.documents = Optional.ofNullable(documents);
    }

    /**
     * Read from a stream.
     */
    public PinnedQueryBuilder(StreamInput in) throws IOException {
        super(in);
        ids = Optional.ofNullable(in.readOptionalStringList());
        if (in.readBoolean()) {
            documents = Optional.ofNullable(in.readNamedWriteableList(Item.class));
        } else {
            documents = Optional.empty();
        }
        organicQuery = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalStringCollection(this.ids.orElse(null));
        if (documents.isPresent()) {
            out.writeBoolean(true);
            out.writeNamedWriteableList(documents.get());
        } else {
            out.writeBoolean(false);
        }
        out.writeNamedWriteable(organicQuery);
    }

    /**
     * @return the organic query set in the constructor
     */
    public QueryBuilder organicQuery() {
        return this.organicQuery;
    }

    /**
     * Returns the pinned ids for the query.
     */
    public List<String> ids() {
        return this.ids.map(Collections::unmodifiableList).orElse(Collections.emptyList());
    }

    /**
     * @return the pinned documents for the query.
     */
    public List<Item> documents() {
        return this.documents.map(Collections::unmodifiableList).orElse(Collections.emptyList());
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (organicQuery != null) {
            builder.field(ORGANIC_QUERY_FIELD.getPreferredName());
            organicQuery.toXContent(builder, params);
        }
        if (ids.isPresent()) {
            builder.startArray(IDS_FIELD.getPreferredName());
            for (String value : ids.get()) {
                builder.value(value);
            }
            builder.endArray();
        }
        if (documents.isPresent()) {
            builder.startArray(DOCUMENTS_FIELD.getPreferredName());
            for (Item item : documents.get()) {
                builder.value(item);
            }
            builder.endArray();
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }



    private static final ConstructingObjectParser<PinnedQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a ->
                {
                    QueryBuilder organicQuery = (QueryBuilder) a[0];
                    @SuppressWarnings("unchecked")
                    List<String> ids = (List<String>) a[1];
                    @SuppressWarnings("unchecked")
                    List<Item> documents = (List<Item>) a[2];
                    return new PinnedQueryBuilder(organicQuery, ids, documents);
                }
             );
    static {
        PARSER.declareObject(constructorArg(), (p, c) -> parseInnerQueryBuilder(p), ORGANIC_QUERY_FIELD);
        PARSER.declareStringArray(optionalConstructorArg(), IDS_FIELD);
        PARSER.declareObjectArray(optionalConstructorArg(), Item.PARSER, DOCUMENTS_FIELD);
        declareStandardFields(PARSER);
    }

    public static PinnedQueryBuilder fromXContent(XContentParser parser) {
        try {
            return PARSER.apply(parser, null);
        } catch (IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder newOrganicQuery = organicQuery.rewrite(queryRewriteContext);
        if (newOrganicQuery != organicQuery) {
            PinnedQueryBuilder result = new PinnedQueryBuilder(newOrganicQuery, ids.orElse(null), documents.orElse(null));
            result.boost(this.boost);
            return result;
        }
        return this;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType idField = context.getFieldType(IdFieldMapper.NAME);
        MappedFieldType indexField = context.getFieldType(IndexFieldMapper.NAME);
        if (idField == null || indexField == null) {
            return new MatchNoDocsQuery("No mappings");
        }
        List<Item> items = documents.orElseGet(() -> ids.get().stream().map(id -> new Item(null, id)).collect(Collectors.toList()));
        if (items.isEmpty()) {
            return new CappedScoreQuery(organicQuery.toQuery(context), MAX_ORGANIC_SCORE);
        } else {
            List<Query> pinnedQueries = new ArrayList<>();

            // Ensure each pin order using a Boost query with the relevant boost factor
            int minPin = NumericUtils.floatToSortableInt(MAX_ORGANIC_SCORE) + 1;
            int boostNum = minPin + items.size();
            float lastScore = Float.MAX_VALUE;
            for (Item item : items) {
                float pinScore = NumericUtils.sortableIntToFloat(boostNum);
                assert pinScore < lastScore;
                lastScore = pinScore;
                boostNum--;
                BooleanQuery.Builder documentQueries = new BooleanQuery.Builder();
                documentQueries.add(idField.termQuery(item.id, context), BooleanClause.Occur.FILTER);
                if (item.index != null) {
                    documentQueries.add(indexField.termQuery(item.index, context), BooleanClause.Occur.FILTER);
                }
                // Ensure the pin order using a Boost query with the relevant boost factor
                Query documentQuery = new BoostQuery(new ConstantScoreQuery(documentQueries.build()), pinScore);
                pinnedQueries.add(documentQuery);
            }

            // Score for any pinned query clause should be used, regardless of any organic clause score, to preserve pin order.
            // Use dismax to always take the larger (ie pinned) of the organic vs pinned scores
            List<Query> organicAndPinned = new ArrayList<>();
            organicAndPinned.add(new DisjunctionMaxQuery(pinnedQueries, 0));
            // Cap the scores of the organic query
            organicAndPinned.add(new CappedScoreQuery(organicQuery.toQuery(context), MAX_ORGANIC_SCORE));
            return new DisjunctionMaxQuery(organicAndPinned, 0);
        }

    }

    @Override
    protected int doHashCode() {
        return Objects.hash(ids, documents, organicQuery);
    }

    @Override
    protected boolean doEquals(PinnedQueryBuilder other) {
        return Objects.equals(ids, other.ids)
            && Objects.equals(documents, other.documents)
            && Objects.equals(organicQuery, other.organicQuery)
            && boost == other.boost;
    }
}
