/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A query that will promote selected documents (identified by ID) above matches produced by an "organic" query. In practice, some upstream
 * system will identify the promotions associated with a user's query string and use this object to ensure these are "pinned" to the top of
 * the other search results.
 */
public class PinnedQueryBuilder extends AbstractQueryBuilder<PinnedQueryBuilder> {
    public static final String NAME = "pinned";
    protected final QueryBuilder organicQuery;
    protected final List<String> ids;    
    protected static final ParseField IDS_FIELD = new ParseField("ids");
    protected static final ParseField ORGANIC_QUERY_FIELD = new ParseField("organic");

    @Override
    public String getWriteableName() {
        return NAME;
    }

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
      this.ids = new ArrayList<>();
      Collections.addAll(this.ids, ids);
      
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

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        throw new UnsupportedOperationException("Client side-only class for use in HLRC");
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
