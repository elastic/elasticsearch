/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

public class ReverseNestedQueryBuilder extends AbstractQueryBuilder<ReverseNestedQueryBuilder> {
    public static final String NAME = "reverse_nested";
    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    private static final ParseField PATH_FIELD = new ParseField("path");
    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    private final String path;
    private final QueryBuilder query;
    private boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

    private ReverseNestedQueryBuilder(String path, QueryBuilder query) {
        this.path = requireValue(path, "[" + NAME + "] requires 'path' field");
        this.query = requireValue(query, "[" + NAME + "] requires 'query' field");
    }

    /**
     * Read from a stream.
     */
    public ReverseNestedQueryBuilder(StreamInput in) throws IOException {
        super(in);
        path = in.readString();
        query = in.readNamedWriteable(QueryBuilder.class);
        ignoreUnmapped = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(path);
        out.writeNamedWriteable(query);
        out.writeBoolean(ignoreUnmapped);
    }

    /**
     * Returns the nested query to execute.
     */
    public QueryBuilder query() {
        return query;
    }

    /**
     * Sets whether the query builder should ignore unmapped paths (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the path is unmapped.
     */
    public ReverseNestedQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
        this.ignoreUnmapped = ignoreUnmapped;
        return this;
    }

    /**
     * Gets whether the query builder will ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the path is unmapped.
     */
    public boolean ignoreUnmapped() {
        return ignoreUnmapped;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName());
        query.toXContent(builder, params);
        builder.field(PATH_FIELD.getPreferredName(), path);
        builder.field(IGNORE_UNMAPPED_FIELD.getPreferredName(), ignoreUnmapped);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static ReverseNestedQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        QueryBuilder query = null;
        String path = null;
        String currentFieldName = null;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    query = parseInnerQueryBuilder(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[nested] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (PATH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    path = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (IGNORE_UNMAPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[nested] query does not support [" + currentFieldName + "]");
                }
            }
        }
        ReverseNestedQueryBuilder queryBuilder = new ReverseNestedQueryBuilder(path, query).ignoreUnmapped(ignoreUnmapped)
            .queryName(queryName)
            .boost(boost);
        return queryBuilder;
    }

    @Override
    public final String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(ReverseNestedQueryBuilder that) {
        return Objects.equals(query, that.query)
            && Objects.equals(path, that.path)
            && Objects.equals(ignoreUnmapped, that.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, path, ignoreUnmapped);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "[joining] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }

        NestedObjectMapper mapper = context.nestedLookup().getNestedMappers().get(path);
        if (mapper == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new IllegalStateException("[" + NAME + "] failed to find nested object under path [" + path + "]");
            }
        }
        final BitSetProducer parentFilter;
        Query innerQuery;
        NestedObjectMapper objectMapper = context.nestedScope().getObjectMapper();
        if (objectMapper == null) {
            parentFilter = context.bitsetFilter(Queries.newNonNestedFilter(context.indexVersionCreated()));
        } else {
            parentFilter = context.bitsetFilter(objectMapper.nestedTypeFilter());
        }

        try {
            context.nestedScope().nextLevel(mapper);
            innerQuery = this.query.toQuery(context);
        } finally {
            context.nestedScope().previousLevel();
        }

        // ToChildBlockJoinQuery requires that the inner query only matches documents
        // in its child space
        NestedHelper nestedHelper = new NestedHelper(context.nestedLookup(), context::isFieldMapped);
        if (nestedHelper.mightMatchNestedDocs(innerQuery)) {
            innerQuery = Queries.newNonNestedFilter(context.indexVersionCreated());
        }

        return new ToChildBlockJoinQuery(innerQuery, parentFilter);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewrittenQuery = query.rewrite(queryRewriteContext);
        if (rewrittenQuery != query) {
            ReverseNestedQueryBuilder nestedQuery = new ReverseNestedQueryBuilder(path, rewrittenQuery);
            nestedQuery.ignoreUnmapped(ignoreUnmapped);
            return nestedQuery;
        }
        return this;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_500_006;
    }
}
