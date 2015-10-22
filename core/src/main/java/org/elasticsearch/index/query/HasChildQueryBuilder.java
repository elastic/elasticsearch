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
package org.elasticsearch.index.query;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexParentChildFieldData;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.query.support.QueryInnerHits;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.fetch.innerhits.InnerHitsSubSearchContext;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * A query builder for <tt>has_child</tt> queries.
 */
public class HasChildQueryBuilder extends AbstractQueryBuilder<HasChildQueryBuilder> {

    /**
     * The queries name
     */
    public static final String NAME = "has_child";

    /**
     * The default maximum number of children that are required to match for the parent to be considered a match.
     */
    public static final int DEFAULT_MAX_CHILDREN = Integer.MAX_VALUE;
    /**
     * The default minimum number of children that are required to match for the parent to be considered a match.
     */
    public static final int DEFAULT_MIN_CHILDREN = 0;
    /*
     * The default score mode that is used to combine score coming from multiple parent documents.
     */
    public static final ScoreMode DEFAULT_SCORE_MODE = ScoreMode.None;

    private final QueryBuilder query;

    private final String type;

    private ScoreMode scoreMode = DEFAULT_SCORE_MODE;

    private int minChildren = DEFAULT_MIN_CHILDREN;

    private int maxChildren = DEFAULT_MAX_CHILDREN;

    private QueryInnerHits queryInnerHits;

    static final HasChildQueryBuilder PROTOTYPE = new HasChildQueryBuilder("", EmptyQueryBuilder.PROTOTYPE);

    public HasChildQueryBuilder(String type, QueryBuilder query, int maxChildren, int minChildren, ScoreMode scoreMode, QueryInnerHits queryInnerHits) {
        this(type, query);
        scoreMode(scoreMode);
        this.maxChildren = maxChildren;
        this.minChildren = minChildren;
        this.queryInnerHits = queryInnerHits;
    }

    public HasChildQueryBuilder(String type, QueryBuilder query) {
        if (type == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires 'type' field");
        }
        if (query == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires 'query' field");
        }
        this.type = type;
        this.query = query;
    }

    /**
     * Defines how the scores from the matching child documents are mapped into the parent document.
     */
    public HasChildQueryBuilder scoreMode(ScoreMode scoreMode) {
        if (scoreMode == null) {
            throw new IllegalArgumentException("[" + NAME + "]  requires 'score_mode' field");
        }
        this.scoreMode = scoreMode;
        return this;
    }

    /**
     * Defines the minimum number of children that are required to match for the parent to be considered a match.
     */
    public HasChildQueryBuilder minChildren(int minChildren) {
        if (minChildren < 0) {
            throw new IllegalArgumentException("[" + NAME + "]  requires non-negative 'min_children' field");
        }
        this.minChildren = minChildren;
        return this;
    }

    /**
     * Defines the maximum number of children that are required to match for the parent to be considered a match.
     */
    public HasChildQueryBuilder maxChildren(int maxChildren) {
        if (maxChildren < 0) {
            throw new IllegalArgumentException("[" + NAME + "]  requires non-negative 'max_children' field");
        }
        this.maxChildren = maxChildren;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public HasChildQueryBuilder innerHit(QueryInnerHits queryInnerHits) {
        this.queryInnerHits = queryInnerHits;
        return this;
    }

    /**
     * Returns inner hit definition in the scope of this query and reusing the defined type and query.
     */
    public QueryInnerHits innerHit() {
        return queryInnerHits;
    }

    /**
     * Returns the children query to execute.
     */
    public QueryBuilder query() {
        return query;
    }

    /**
     * Returns the child type
     */
    public String childType() {
        return type;
    }

    /**
     * Returns how the scores from the matching child documents are mapped into the parent document.
     */
    public ScoreMode scoreMode() {
        return scoreMode;
    }

    /**
     * Returns the minimum number of children that are required to match for the parent to be considered a match.
     * The default is {@value #DEFAULT_MAX_CHILDREN}
     */
    public int minChildren() {
        return minChildren;
    }

    /**
     * Returns the maximum number of children that are required to match for the parent to be considered a match.
     * The default is {@value #DEFAULT_MIN_CHILDREN}
     */
    public int maxChildren() { return maxChildren; }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("query");
        query.toXContent(builder, params);
        builder.field("child_type", type);
        builder.field("score_mode", scoreMode.name().toLowerCase(Locale.ROOT));
        builder.field("min_children", minChildren);
        builder.field("max_children", maxChildren);
        printBoostAndQueryName(builder);
        if (queryInnerHits != null) {
            queryInnerHits.toXContent(builder, params);
        }
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        String[] previousTypes = QueryShardContext.setTypesWithPrevious(type);
        Query innerQuery;
        try {
            innerQuery = query.toQuery(context);
        } finally {
            QueryShardContext.setTypes(previousTypes);
        }
        if (innerQuery == null) {
            return null;
        }
        innerQuery.setBoost(boost);

        DocumentMapper childDocMapper = context.mapperService().documentMapper(type);
        if (childDocMapper == null) {
            throw new QueryShardException(context, "[" + NAME + "] no mapping found for type [" + type + "]");
        }
        ParentFieldMapper parentFieldMapper = childDocMapper.parentFieldMapper();
        if (parentFieldMapper.active() == false) {
            throw new QueryShardException(context, "[" + NAME + "] _parent field has no parent type configured");
        }
        if (queryInnerHits != null) {
            try (XContentParser parser = queryInnerHits.getXcontentParser()) {
                XContentParser.Token token = parser.nextToken();
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new IllegalStateException("start object expected but was: [" + token + "]");
                }
                InnerHitsSubSearchContext innerHits = context.indexQueryParserService().getInnerHitsQueryParserHelper().parse(parser);
                if (innerHits != null) {
                    ParsedQuery parsedQuery = new ParsedQuery(innerQuery, context.copyNamedQueries());
                    InnerHitsContext.ParentChildInnerHits parentChildInnerHits = new InnerHitsContext.ParentChildInnerHits(innerHits.getSubSearchContext(), parsedQuery, null, context.mapperService(), childDocMapper);
                    String name = innerHits.getName() != null ? innerHits.getName() : type;
                    context.addInnerHits(name, parentChildInnerHits);
                }
            }
        }

        String parentType = parentFieldMapper.type();
        DocumentMapper parentDocMapper = context.mapperService().documentMapper(parentType);
        if (parentDocMapper == null) {
            throw new QueryShardException(context, "[" + NAME + "] Type [" + type + "] points to a non existent parent type ["
                    + parentType + "]");
        }

        if (maxChildren > 0 && maxChildren < minChildren) {
            throw new QueryShardException(context, "[" + NAME + "] 'max_children' is less than 'min_children'");
        }

        // wrap the query with type query
        innerQuery = Queries.filtered(innerQuery, childDocMapper.typeFilter());

        final ParentChildIndexFieldData parentChildIndexFieldData = context.getForField(parentFieldMapper.fieldType());
        int maxChildren = maxChildren();
        // 0 in pre 2.x p/c impl means unbounded
        if (maxChildren == 0) {
            maxChildren = Integer.MAX_VALUE;
        }
        return new LateParsingQuery(parentDocMapper.typeFilter(), innerQuery, minChildren(), maxChildren, parentType, scoreMode, parentChildIndexFieldData);
    }

    final static class LateParsingQuery extends Query {

        private final Query toQuery;
        private final Query innerQuery;
        private final int minChildren;
        private final int maxChildren;
        private final String parentType;
        private final ScoreMode scoreMode;
        private final ParentChildIndexFieldData parentChildIndexFieldData;

        LateParsingQuery(Query toQuery, Query innerQuery, int minChildren, int maxChildren, String parentType, ScoreMode scoreMode, ParentChildIndexFieldData parentChildIndexFieldData) {
            this.toQuery = toQuery;
            this.innerQuery = innerQuery;
            this.minChildren = minChildren;
            this.maxChildren = maxChildren;
            this.parentType = parentType;
            this.scoreMode = scoreMode;
            this.parentChildIndexFieldData = parentChildIndexFieldData;
        }

        @Override
        public Query rewrite(IndexReader reader) throws IOException {
            if (getBoost() != 1.0F) {
                return super.rewrite(reader);
            }
            if (reader instanceof DirectoryReader) {
                String joinField = ParentFieldMapper.joinField(parentType);
                IndexSearcher indexSearcher = new IndexSearcher(reader);
                indexSearcher.setQueryCache(null);
                IndexParentChildFieldData indexParentChildFieldData = parentChildIndexFieldData.loadGlobal((DirectoryReader) reader);
                MultiDocValues.OrdinalMap ordinalMap = ParentChildIndexFieldData.getOrdinalMap(indexParentChildFieldData, parentType);
                return JoinUtil.createJoinQuery(joinField, innerQuery, toQuery, indexSearcher, scoreMode, ordinalMap, minChildren, maxChildren);
            } else {
                if (reader.leaves().isEmpty() && reader.numDocs() == 0) {
                    // asserting reader passes down a MultiReader during rewrite which makes this
                    // blow up since for this query to work we have to have a DirectoryReader otherwise
                    // we can't load global ordinals - for this to work we simply check if the reader has no leaves
                    // and rewrite to match nothing
                    return new MatchNoDocsQuery();
                }
                throw new IllegalStateException("can't load global ordinals for reader of type: " + reader.getClass() + " must be a DirectoryReader");
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;

            LateParsingQuery that = (LateParsingQuery) o;

            if (minChildren != that.minChildren) return false;
            if (maxChildren != that.maxChildren) return false;
            if (!toQuery.equals(that.toQuery)) return false;
            if (!innerQuery.equals(that.innerQuery)) return false;
            if (!parentType.equals(that.parentType)) return false;
            return scoreMode == that.scoreMode;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + toQuery.hashCode();
            result = 31 * result + innerQuery.hashCode();
            result = 31 * result + minChildren;
            result = 31 * result + maxChildren;
            result = 31 * result + parentType.hashCode();
            result = 31 * result + scoreMode.hashCode();
            return result;
        }

        @Override
        public String toString(String s) {
            return "LateParsingQuery {parentType=" + parentType + "}";
        }

        public int getMinChildren() {
            return minChildren;
        }

        public int getMaxChildren() {
            return maxChildren;
        }

        public ScoreMode getScoreMode() {
            return scoreMode;
        }

        public Query getInnerQuery() {
            return innerQuery;
        }
    }

    @Override
    protected boolean doEquals(HasChildQueryBuilder that) {
        return Objects.equals(query, that.query)
                && Objects.equals(type, that.type)
                && Objects.equals(scoreMode, that.scoreMode)
                && Objects.equals(minChildren, that.minChildren)
                && Objects.equals(maxChildren, that.maxChildren)
                && Objects.equals(queryInnerHits, that.queryInnerHits);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, type, scoreMode, minChildren, maxChildren, queryInnerHits);
    }

    protected HasChildQueryBuilder(StreamInput in) throws IOException {
        type = in.readString();
        minChildren = in.readInt();
        maxChildren = in.readInt();
        final int ordinal = in.readVInt();
        scoreMode = ScoreMode.values()[ordinal];
        query = in.readQuery();
        if (in.readBoolean()) {
            queryInnerHits = new QueryInnerHits(in);
        }
    }

    @Override
    protected HasChildQueryBuilder doReadFrom(StreamInput in) throws IOException {
        return new HasChildQueryBuilder(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeInt(minChildren());
        out.writeInt(maxChildren());
        out.writeVInt(scoreMode.ordinal());
        out.writeQuery(query);
        if (queryInnerHits != null) {
            out.writeBoolean(true);
            queryInnerHits.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }
}
