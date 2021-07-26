/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.Arrays;
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
    private final List<Item> docs;
    protected static final ParseField IDS_FIELD = new ParseField("ids");
    private static final ParseField DOCS_FIELD = new ParseField("docs");
    protected static final ParseField ORGANIC_QUERY_FIELD = new ParseField("organic");

    /**
     * A single item to be used for a {@link PinnedQueryBuilder}.
     */
    public static final class Item implements ToXContentObject, Writeable {
        private static final ParseField INDEX_FIELD = new ParseField("_index");
        private static final ParseField ID_FIELD = new ParseField("_id");

        private final String index;
        private final String id;

        public Item(String index, String id) {
            if (index == null) {
                throw new IllegalArgumentException("Item requires index to be non-null");
            }
            if (Regex.isSimpleMatchPattern(index)) {
                throw new IllegalArgumentException("Item index cannot contain wildcard expressions");
            }
            if (id == null) {
                throw new IllegalArgumentException("Item requires id to be non-null");
            }
            this.index = index;
            this.id = id;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeString(id);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INDEX_FIELD.getPreferredName(), index);
            builder.field(ID_FIELD.getPreferredName(), id);
            return builder.endObject();
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public PinnedQueryBuilder(QueryBuilder organicQuery, String... ids) {
        this(organicQuery, Arrays.asList(ids), null);
    }

    public PinnedQueryBuilder(QueryBuilder organicQuery, Item... docs) {
        this(organicQuery, null, Arrays.asList(docs));
    }

    /**
     * Creates a new PinnedQueryBuilder
     */
    public PinnedQueryBuilder(QueryBuilder organicQuery, List<String> ids, List<Item> docs) {
      if (organicQuery == null) {
          throw new IllegalArgumentException("[" + NAME + "] organicQuery cannot be null");
      }
      this.organicQuery = organicQuery;
      if (ids == null && docs == null) {
          throw new IllegalArgumentException("[" + NAME + "] ids and docs cannot both be null");
      }
      if (ids != null && docs != null) {
          throw new IllegalArgumentException("[" + NAME + "] ids and docs cannot both be used");
      }
      this.ids = ids;
      this.docs = docs;
    }


    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalStringCollection(this.ids);
        if (docs == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeList(docs);
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
        if (this.ids == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(this.ids);
    }

    /**
     * Returns the pinned docs for the query.
     */
    public List<Item> docs() {
        if (this.docs == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(this.docs);
    }


    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (organicQuery != null) {
            builder.field(ORGANIC_QUERY_FIELD.getPreferredName());
            organicQuery.toXContent(builder, params);
        }
        if (ids != null) {
            builder.startArray(IDS_FIELD.getPreferredName());
            for (String value : ids) {
                builder.value(value);
            }
            builder.endArray();
        }
        if (docs != null) {
            builder.startArray(DOCS_FIELD.getPreferredName());
            for (Item item : docs) {
                builder.value(item);
            }
            builder.endArray();
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        throw new UnsupportedOperationException("Client side-only class for use in HLRC");
    }


    @Override
    protected int doHashCode() {
        return Objects.hash(ids, docs, organicQuery);
    }

    @Override
    protected boolean doEquals(PinnedQueryBuilder other) {
        return Objects.equals(ids, other.ids)
            && Objects.equals(docs, other.docs)
            && Objects.equals(organicQuery, other.organicQuery)
            && boost == other.boost;
    }

}
