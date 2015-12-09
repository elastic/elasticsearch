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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.query.support.QueryInnerHits;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.fetch.innerhits.InnerHitsSubSearchContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Builder for the 'has_parent' query.
 */
public class HasParentQueryBuilder extends AbstractQueryBuilder<HasParentQueryBuilder> {

    public static final String NAME = "has_parent";
    public static final boolean DEFAULT_SCORE = false;
    private final QueryBuilder query;
    private final String type;
    private boolean score = DEFAULT_SCORE;
    private QueryInnerHits innerHit;

    /**
     * @param type  The parent type
     * @param query The query that will be matched with parent documents
     */
    public HasParentQueryBuilder(String type, QueryBuilder query) {
        if (type == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires 'parent_type' field");
        }
        if (query == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires 'query' field");
        }
        this.type = type;
        this.query = query;
    }

    public HasParentQueryBuilder(String type, QueryBuilder query, boolean score, QueryInnerHits innerHits) {
        this(type, query);
        this.score = score;
        this.innerHit = innerHits;
    }

    /**
     * Defines if the parent score is mapped into the child documents.
     */
    public HasParentQueryBuilder score(boolean score) {
        this.score = score;
        return this;
    }

    /**
     * Sets inner hit definition in the scope of this query and reusing the defined type and query.
     */
    public HasParentQueryBuilder innerHit(QueryInnerHits innerHit) {
        this.innerHit = innerHit;
        return this;
    }

    /**
     * Returns the query to execute.
     */
    public QueryBuilder query() {
        return query;
    }

    /**
     * Returns <code>true</code> if the parent score is mapped into the child documents
     */
    public boolean score() {
        return score;
    }

    /**
     * Returns the parents type name
     */
    public String type() {
        return type;
    }

    /**
     *  Returns inner hit definition in the scope of this query and reusing the defined type and query.
     */
    public QueryInnerHits innerHit() {
        return innerHit;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query innerQuery;
        String[] previousTypes = QueryShardContext.setTypesWithPrevious(type);
        try {
            innerQuery = query.toQuery(context);
        } finally {
            QueryShardContext.setTypes(previousTypes);
        }

        if (innerQuery == null) {
            return null;
        }
        DocumentMapper parentDocMapper = context.getMapperService().documentMapper(type);
        if (parentDocMapper == null) {
            throw new QueryShardException(context, "[has_parent] query configured 'parent_type' [" + type
                    + "] is not a valid type");
        }

        if (innerHit != null) {
            try (XContentParser parser = innerHit.getXcontentParser()) {
                XContentParser.Token token = parser.nextToken();
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new IllegalStateException("start object expected but was: [" + token + "]");
                }
                InnerHitsSubSearchContext innerHits = context.getInnerHitsContext(parser);
                if (innerHits != null) {
                    ParsedQuery parsedQuery = new ParsedQuery(innerQuery, context.copyNamedQueries());
                    InnerHitsContext.ParentChildInnerHits parentChildInnerHits = new InnerHitsContext.ParentChildInnerHits(innerHits.getSubSearchContext(), parsedQuery, null, context.getMapperService(), parentDocMapper);
                    String name = innerHits.getName() != null ? innerHits.getName() : type;
                    context.addInnerHits(name, parentChildInnerHits);
                }
            }
        }

        Set<String> parentTypes = new HashSet<>(5);
        parentTypes.add(parentDocMapper.type());
        ParentChildIndexFieldData parentChildIndexFieldData = null;
        for (DocumentMapper documentMapper : context.getMapperService().docMappers(false)) {
            ParentFieldMapper parentFieldMapper = documentMapper.parentFieldMapper();
            if (parentFieldMapper.active()) {
                DocumentMapper parentTypeDocumentMapper = context.getMapperService().documentMapper(parentFieldMapper.type());
                parentChildIndexFieldData = context.getForField(parentFieldMapper.fieldType());
                if (parentTypeDocumentMapper == null) {
                    // Only add this, if this parentFieldMapper (also a parent)  isn't a child of another parent.
                    parentTypes.add(parentFieldMapper.type());
                }
            }
        }
        if (parentChildIndexFieldData == null) {
            throw new QueryShardException(context, "[has_parent] no _parent field configured");
        }

        Query parentTypeQuery = null;
        if (parentTypes.size() == 1) {
            DocumentMapper documentMapper = context.getMapperService().documentMapper(parentTypes.iterator().next());
            if (documentMapper != null) {
                parentTypeQuery = documentMapper.typeFilter();
            }
        } else {
            BooleanQuery.Builder parentsFilter = new BooleanQuery.Builder();
            for (String parentTypeStr : parentTypes) {
                DocumentMapper documentMapper = context.getMapperService().documentMapper(parentTypeStr);
                if (documentMapper != null) {
                    parentsFilter.add(documentMapper.typeFilter(), BooleanClause.Occur.SHOULD);
                }
            }
            parentTypeQuery = parentsFilter.build();
        }

        if (parentTypeQuery == null) {
            return null;
        }

        // wrap the query with type query
        innerQuery = Queries.filtered(innerQuery, parentDocMapper.typeFilter());
        Query childrenFilter = Queries.not(parentTypeQuery);
        return new HasChildQueryBuilder.LateParsingQuery(childrenFilter, innerQuery, HasChildQueryBuilder.DEFAULT_MIN_CHILDREN, HasChildQueryBuilder.DEFAULT_MAX_CHILDREN, type, score ? ScoreMode.Max : ScoreMode.None, parentChildIndexFieldData);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(HasParentQueryParser.QUERY_FIELD.getPreferredName());
        query.toXContent(builder, params);
        builder.field(HasParentQueryParser.TYPE_FIELD.getPreferredName(), type);
        builder.field(HasParentQueryParser.SCORE_FIELD.getPreferredName(), score);
        printBoostAndQueryName(builder);
        if (innerHit != null) {
           innerHit.toXContent(builder, params);
        }
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    protected HasParentQueryBuilder(StreamInput in) throws IOException {
        type = in.readString();
        score = in.readBoolean();
        query = in.readQuery();
        if (in.readBoolean()) {
            innerHit = new QueryInnerHits(in);
        }
    }

    @Override
    protected HasParentQueryBuilder doReadFrom(StreamInput in) throws IOException {
        return new HasParentQueryBuilder(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeBoolean(score);
        out.writeQuery(query);
        if (innerHit != null) {
            out.writeBoolean(true);
            innerHit.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    protected boolean doEquals(HasParentQueryBuilder that) {
        return Objects.equals(query, that.query)
                && Objects.equals(type, that.type)
                && Objects.equals(score, that.score)
                && Objects.equals(innerHit, that.innerHit);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, type, score, innerHit);
    }
}
