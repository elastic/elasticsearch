/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A query that will promote selected documents (identified by ID) above matches produced by an "organic" query. In practice, some upstream
 * system will identify the promotions associated with a user's query string and use this object to ensure these are "pinned" to the top of
 * the other search results.
 */
public class PinnedQueryBuilder extends AbstractQueryBuilder<PinnedQueryBuilder> {
    public static final String NAME = "pinned";
    public static final int MAX_NUM_PINNED_HITS = 100;

    private static final ParseField IDS_FIELD = new ParseField("ids");
    public static final ParseField ORGANIC_QUERY_FIELD = new ParseField("organic");

    private final List<String> ids;
    private QueryBuilder organicQuery;

    // Organic queries will have their scores capped to this number range,
    // We reserve the highest float exponent for scores of pinned queries
    private static final float MAX_ORGANIC_SCORE = Float.intBitsToFloat((0xfe << 23)) - 1;

    /**
     * Creates a new PinnedQueryBuilder
     */
    public PinnedQueryBuilder(QueryBuilder organicQuery, String... ids) {
      if (organicQuery == null) {
          throw new IllegalArgumentException("[" + NAME + "] organicQuery cannot be null");
      }
      this.organicQuery = organicQuery;
      if (ids == null) {
          throw new IllegalArgumentException("[" + NAME + "] ids cannot be null");
      }
      if (ids.length > MAX_NUM_PINNED_HITS) {
          throw new IllegalArgumentException("[" + NAME + "] Max of "+MAX_NUM_PINNED_HITS+" ids exceeded: "+
                  ids.length+" provided.");          
      }
      LinkedHashSet<String> deduped = new LinkedHashSet<>();
      for (String id : ids) {
          if (id == null) {
              throw new IllegalArgumentException("[" + NAME + "] id cannot be null");
          }          
          if(deduped.add(id) == false) {
              throw new IllegalArgumentException("[" + NAME + "] duplicate id found in list: "+id);              
          }
      }
      this.ids = new ArrayList<>();
      Collections.addAll(this.ids, ids);
      
    }

    /**
     * Read from a stream.
     */
    public PinnedQueryBuilder(StreamInput in) throws IOException {
        super(in);
        ids = in.readStringList();
        organicQuery = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeStringCollection(this.ids);
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
        return Collections.unmodifiableList(this.ids);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (organicQuery != null) {
            builder.field(ORGANIC_QUERY_FIELD.getPreferredName());
            organicQuery.toXContent(builder, params);
        }
        builder.startArray(IDS_FIELD.getPreferredName());
        for (String value : ids) {
            builder.value(value);
        }
        builder.endArray();
        printBoostAndQueryName(builder);
        builder.endObject();
    }
    
    
    
    private static final ConstructingObjectParser<PinnedQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> 
                {
                    QueryBuilder organicQuery = (QueryBuilder) a[0];
                    @SuppressWarnings("unchecked")
                    List<String> ids = (List<String>) a[1];
                    return new PinnedQueryBuilder(organicQuery, ids.toArray(String[]::new));
                }
             );
    static {
        PARSER.declareObject(constructorArg(), (p, c) -> parseInnerQueryBuilder(p), ORGANIC_QUERY_FIELD);        
        PARSER.declareStringArray(constructorArg(), IDS_FIELD);
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
    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        QueryBuilder newOrganicQuery = organicQuery.rewrite(queryShardContext);
        if (newOrganicQuery != organicQuery) {
            PinnedQueryBuilder result = new PinnedQueryBuilder(newOrganicQuery, ids.toArray(String[]::new));
            result.boost(this.boost);
            return result;
        }
        return this;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType idField = context.fieldMapper(IdFieldMapper.NAME);
        if (idField == null) {
            return new MatchNoDocsQuery("No mappings");
        }
        if (this.ids.isEmpty()) {
            return new CappedScoreQuery(organicQuery.toQuery(context), MAX_ORGANIC_SCORE);
        } else {
            BooleanQuery.Builder pinnedQueries = new BooleanQuery.Builder();

            // Ensure each pin order using a Boost query with the relevant boost factor
            int minPin = NumericUtils.floatToSortableInt(MAX_ORGANIC_SCORE) + 1;
            int boostNum = minPin + ids.size();
            float lastScore = Float.MAX_VALUE;
            for (String id : ids) {
                float pinScore = NumericUtils.sortableIntToFloat(boostNum);
                assert pinScore < lastScore;
                lastScore = pinScore;
                boostNum--;
                // Ensure the pin order using a Boost query with the relevant boost factor
                Query idQuery = new BoostQuery(new ConstantScoreQuery(idField.termQuery(id, context)), pinScore);
                pinnedQueries.add(idQuery, BooleanClause.Occur.SHOULD);
            }

            // Score for any pinned query clause should be used, regardless of any organic clause score, to preserve pin order.
            // Use dismax to always take the larger (ie pinned) of the organic vs pinned scores
            List<Query> organicAndPinned = new ArrayList<>();
            organicAndPinned.add(pinnedQueries.build());
            // Cap the scores of the organic query
            organicAndPinned.add(new CappedScoreQuery(organicQuery.toQuery(context), MAX_ORGANIC_SCORE));
            return new DisjunctionMaxQuery(organicAndPinned, 0);
        }

    }

    @Override
    protected int doHashCode() {
        return Objects.hash(ids, organicQuery);
    }

    @Override
    protected boolean doEquals(PinnedQueryBuilder other) {
        return Objects.equals(ids, other.ids) && Objects.equals(organicQuery, other.organicQuery) && boost == other.boost;
    }
}
