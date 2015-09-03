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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.fielddata.IndexParentChildFieldData;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.query.support.QueryInnerHits;
import org.elasticsearch.index.search.child.ChildrenConstantScoreQuery;
import org.elasticsearch.index.search.child.ChildrenQuery;
import org.elasticsearch.index.search.child.ScoreType;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.fetch.innerhits.InnerHitsSubSearchContext;
import org.elasticsearch.search.internal.SearchContext;

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
     * The default cut off point only to evaluate parent documents that contain the matching parent id terms
     * instead of evaluating all parent docs.
     */
    public static final int DEFAULT_SHORT_CIRCUIT_CUTOFF = 8192;
    /**
     * The default maximum number of children that are required to match for the parent to be considered a match.
     */
    public static final int DEFAULT_MAX_CHILDREN = Integer.MAX_VALUE;
    /**
     * The default minimum number of children that are required to match for the parent to be considered a match.
     */
    public static final int DEFAULT_MIN_CHILDREN = 0;

    private final QueryBuilder query;

    private final String type;

    private ScoreType scoreType = ScoreType.NONE;

    private int minChildren = DEFAULT_MIN_CHILDREN;

    private int maxChildren = DEFAULT_MAX_CHILDREN;

    private int shortCircuitCutoff = DEFAULT_SHORT_CIRCUIT_CUTOFF;

    private QueryInnerHits queryInnerHits;

    static final HasChildQueryBuilder PROTOTYPE = new HasChildQueryBuilder("", EmptyQueryBuilder.PROTOTYPE);

    public HasChildQueryBuilder(String type, QueryBuilder query, Integer maxChildren, Integer minChildren, Integer shortCircuitCutoff, ScoreType scoreType, QueryInnerHits queryInnerHits) {
        this(type, query);
        scoreType(scoreType);
        this.maxChildren = maxChildren;
        this.minChildren = minChildren;
        this.shortCircuitCutoff = shortCircuitCutoff;
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
    public HasChildQueryBuilder scoreType(ScoreType scoreType) {
        if (scoreType == null) {
            throw new IllegalArgumentException("[" + NAME + "]  requires 'score_type' field");
        }
        this.scoreType = scoreType;
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
     * Configures at what cut off point only to evaluate parent documents that contain the matching parent id terms
     * instead of evaluating all parent docs.
     */
    public HasChildQueryBuilder shortCircuitCutoff(int shortCircuitCutoff) {
        if (shortCircuitCutoff < 0) {
            throw new IllegalArgumentException("[" + NAME + "]  requires non-negative 'short_circuit_cutoff' field");
        }
        this.shortCircuitCutoff = shortCircuitCutoff;
        return this;
    }

    /**
     * Sets inner hit definition in the scope of this query and reusing the defined type and query.
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
    public ScoreType scoreType() {
        return scoreType;
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

    /**
     * Returns what cut off point only to evaluate parent documents that contain the matching parent id terms
     * instead of evaluating all parent docs. The default is {@value #DEFAULT_SHORT_CIRCUIT_CUTOFF}
     */
    public int shortCircuitCutoff() {
        return shortCircuitCutoff;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("query");
        query.toXContent(builder, params);
        builder.field("child_type", type);
        builder.field("score_type", scoreType.name().toLowerCase(Locale.ROOT));
        builder.field("min_children", minChildren);
        builder.field("max_children", maxChildren);
        builder.field("short_circuit_cutoff", shortCircuitCutoff);
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
        Query innerQuery = query.toQuery(context);
        if (innerQuery == null) {
            return null;
        }
        innerQuery.setBoost(boost);

        DocumentMapper childDocMapper = context.mapperService().documentMapper(type);
        if (childDocMapper == null) {
            throw new QueryShardException(context, "[" + NAME + "] no mapping for for type [" + type + "]");
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

        BitSetProducer nonNestedDocsFilter = null;
        if (parentDocMapper.hasNestedObjects()) {
            nonNestedDocsFilter = context.bitsetFilter(Queries.newNonNestedFilter());
        }

        // wrap the query with type query
        innerQuery = Queries.filtered(innerQuery, childDocMapper.typeFilter());

        final Query query;
        final ParentChildIndexFieldData parentChildIndexFieldData = context.getForField(parentFieldMapper.fieldType());
        if (context.indexVersionCreated().onOrAfter(Version.V_2_0_0_beta1)) {
            int maxChildren = maxChildren();
            // 0 in pre 2.x p/c impl means unbounded
            if (maxChildren == 0) {
                maxChildren = Integer.MAX_VALUE;
            }
            query = new LateParsingQuery(parentDocMapper.typeFilter(), innerQuery, minChildren(), maxChildren, parentType, scoreTypeToScoreMode(scoreType), parentChildIndexFieldData);
        } else {
            // TODO: use the query API
            Filter parentFilter = new QueryWrapperFilter(parentDocMapper.typeFilter());
            if (minChildren > 1 || maxChildren > 0 || scoreType != ScoreType.NONE) {
                query = new ChildrenQuery(parentChildIndexFieldData, parentType, type, parentFilter, innerQuery, scoreType, minChildren,
                        maxChildren, shortCircuitCutoff, nonNestedDocsFilter);
            } else {
                query = new ChildrenConstantScoreQuery(parentChildIndexFieldData, innerQuery, parentType, type, parentFilter,
                        shortCircuitCutoff, nonNestedDocsFilter);
            }
        }
        return query;
    }

    static ScoreMode scoreTypeToScoreMode(ScoreType scoreType) {
        ScoreMode scoreMode;
        // TODO: move entirely over from ScoreType to org.apache.lucene.join.ScoreMode, when we drop the 1.x parent child code.
        switch (scoreType) {
            case NONE:
                scoreMode = ScoreMode.None;
                break;
            case MIN:
                scoreMode = ScoreMode.Min;
                break;
            case MAX:
                scoreMode = ScoreMode.Max;
                break;
            case SUM:
                scoreMode = ScoreMode.Total;
                break;
            case AVG:
                scoreMode = ScoreMode.Avg;
                break;
            default:
                throw new IllegalArgumentException("score type [" + scoreType + "] not supported");
        }
        return scoreMode;
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
            SearchContext searchContext = SearchContext.current();
            if (searchContext == null) {
                throw new IllegalArgumentException("Search context is required to be set");
            }

            IndexSearcher indexSearcher = searchContext.searcher();
            String joinField = ParentFieldMapper.joinField(parentType);
            IndexParentChildFieldData indexParentChildFieldData = parentChildIndexFieldData.loadGlobal(indexSearcher.getIndexReader());
            MultiDocValues.OrdinalMap ordinalMap = ParentChildIndexFieldData.getOrdinalMap(indexParentChildFieldData, parentType);
            return JoinUtil.createJoinQuery(joinField, innerQuery, toQuery, indexSearcher, scoreMode, ordinalMap, minChildren, maxChildren);
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
    }

    @Override
    protected boolean doEquals(HasChildQueryBuilder that) {
        return Objects.equals(query, that.query)
                && Objects.equals(type, that.type)
                && Objects.equals(scoreType, that.scoreType)
                && Objects.equals(minChildren, that.minChildren)
                && Objects.equals(maxChildren, that.maxChildren)
                && Objects.equals(shortCircuitCutoff, that.shortCircuitCutoff)
                && Objects.equals(queryInnerHits, that.queryInnerHits);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, type, scoreType, minChildren, maxChildren, shortCircuitCutoff, queryInnerHits);
    }

    protected HasChildQueryBuilder(StreamInput in) throws IOException {
        type = in.readString();
        minChildren = in.readInt();
        maxChildren = in.readInt();
        shortCircuitCutoff = in.readInt();
        final int ordinal = in.readVInt();
        scoreType = ScoreType.values()[ordinal];
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
        out.writeInt(shortCircuitCutoff());
        out.writeVInt(scoreType.ordinal());
        out.writeQuery(query);
        if (queryInnerHits != null) {
            out.writeBoolean(true);
            queryInnerHits.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }
}
