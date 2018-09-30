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

import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.queries.SpanMatchNoDocsQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.ScoringRewrite;
import org.apache.lucene.search.TopTermsRewrite;
import org.apache.lucene.search.spans.SpanBoostQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Query that allows wrapping a {@link MultiTermQueryBuilder} (one of wildcard, fuzzy, prefix, term, range or regexp query)
 * as a {@link SpanQueryBuilder} so it can be nested.
 */
public class SpanMultiTermQueryBuilder extends AbstractQueryBuilder<SpanMultiTermQueryBuilder>
    implements SpanQueryBuilder {

    public static final String NAME = "span_multi";
    private static final ParseField MATCH_FIELD = new ParseField("match");
    private final MultiTermQueryBuilder multiTermQueryBuilder;

    public SpanMultiTermQueryBuilder(MultiTermQueryBuilder multiTermQueryBuilder) {
        if (multiTermQueryBuilder == null) {
            throw new IllegalArgumentException("inner multi term query cannot be null");
        }
        this.multiTermQueryBuilder = multiTermQueryBuilder;
    }

    /**
     * Read from a stream.
     */
    public SpanMultiTermQueryBuilder(StreamInput in) throws IOException {
        super(in);
        multiTermQueryBuilder = (MultiTermQueryBuilder) in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(multiTermQueryBuilder);
    }

    public MultiTermQueryBuilder innerQuery() {
        return this.multiTermQueryBuilder;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params)
        throws IOException {
        builder.startObject(NAME);
        builder.field(MATCH_FIELD.getPreferredName());
        multiTermQueryBuilder.toXContent(builder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static SpanMultiTermQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String currentFieldName = null;
        MultiTermQueryBuilder subQuery = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (MATCH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    QueryBuilder query = parseInnerQueryBuilder(parser);
                    if (query instanceof MultiTermQueryBuilder == false) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "[span_multi] [" + MATCH_FIELD.getPreferredName() + "] must be of type multi term query");
                    }
                    subQuery = (MultiTermQueryBuilder) query;
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_multi] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_multi] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (subQuery == null) {
            throw new ParsingException(parser.getTokenLocation(),
                "[span_multi] must have [" + MATCH_FIELD.getPreferredName() + "] multi term query clause");
        }

        return new SpanMultiTermQueryBuilder(subQuery).queryName(queryName).boost(boost);
    }

    static class TopTermSpanBooleanQueryRewriteWithMaxClause extends SpanMultiTermQueryWrapper.SpanRewriteMethod {
        private final long maxExpansions;

        TopTermSpanBooleanQueryRewriteWithMaxClause() {
            this.maxExpansions = BooleanQuery.getMaxClauseCount();
        }

        @Override
        public SpanQuery rewrite(IndexReader reader, MultiTermQuery query) throws IOException {
            final MultiTermQuery.RewriteMethod delegate = new ScoringRewrite<List<SpanQuery>>() {
                @Override
                protected List<SpanQuery> getTopLevelBuilder() {
                    return new ArrayList();
                }

                @Override
                protected Query build(List<SpanQuery> builder) {
                    return new SpanOrQuery((SpanQuery[]) builder.toArray(new SpanQuery[builder.size()]));
                }

                @Override
                protected void checkMaxClauseCount(int count) {
                    if (count > maxExpansions) {
                        throw new RuntimeException("[" + query.toString() + " ] " +
                            "exceeds maxClauseCount [ Boolean maxClauseCount is set to " + BooleanQuery.getMaxClauseCount() + "]");
                    }
                }

                @Override
                protected void addClause(List<SpanQuery> topLevel, Term term, int docCount, float boost, TermStates states) {
                    SpanTermQuery q = new SpanTermQuery(term, states);
                    topLevel.add(q);
                }
            };
            return (SpanQuery) delegate.rewrite(reader, query);
        }
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query subQuery = multiTermQueryBuilder.toQuery(context);
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        while (true) {
            if (subQuery instanceof ConstantScoreQuery) {
                subQuery = ((ConstantScoreQuery) subQuery).getQuery();
                boost = 1;
            } else if (subQuery instanceof BoostQuery) {
                BoostQuery boostQuery = (BoostQuery) subQuery;
                subQuery = boostQuery.getQuery();
                boost *= boostQuery.getBoost();
            } else {
                break;
            }
        }
        // no MultiTermQuery extends SpanQuery, so SpanBoostQuery is not supported here
        assert subQuery instanceof SpanBoostQuery == false;

        if (subQuery instanceof MatchNoDocsQuery) {
            return new SpanMatchNoDocsQuery(multiTermQueryBuilder.fieldName(), subQuery.toString());
        }

        final SpanQuery spanQuery;
        if (subQuery instanceof TermQuery) {
            /**
             * Text fields that index prefixes can rewrite prefix queries
             * into term queries. See {@link TextFieldMapper.TextFieldType#prefixQuery}.
             */
            if (multiTermQueryBuilder.getClass() != PrefixQueryBuilder.class) {
                throw new UnsupportedOperationException("unsupported inner query generated by " +
                    multiTermQueryBuilder.getClass().getName() + ", should be " + MultiTermQuery.class.getName()
                    + " but was " + subQuery.getClass().getName());
            }

            PrefixQueryBuilder prefixBuilder = (PrefixQueryBuilder) multiTermQueryBuilder;
            MappedFieldType fieldType = context.fieldMapper(prefixBuilder.fieldName());
            String fieldName = fieldType != null ? fieldType.name() : prefixBuilder.fieldName();

            if (context.getIndexSettings().getIndexVersionCreated().before(Version.V_6_4_0)) {
                /**
                 * Indices created in this version do not index positions on the prefix field
                 * so we cannot use it to match positional queries. Instead, we explicitly create the prefix
                 * query on the main field to avoid the rewrite.
                 */
                PrefixQuery prefixQuery = new PrefixQuery(new Term(fieldName, prefixBuilder.value()));
                if (prefixBuilder.rewrite() != null) {
                    MultiTermQuery.RewriteMethod rewriteMethod =
                        QueryParsers.parseRewriteMethod(prefixBuilder.rewrite(), null, LoggingDeprecationHandler.INSTANCE);
                    prefixQuery.setRewriteMethod(rewriteMethod);
                }
                subQuery = prefixQuery;
                spanQuery = new SpanMultiTermQueryWrapper<>(prefixQuery);
            } else {
                /**
                 * Prefixes are indexed in a different field so we mask the term query with the original field
                 * name. This is required because span_near and span_or queries don't work across different field.
                 * The masking is safe because the prefix field is indexed using the same content than the original field
                 * and the prefix analyzer preserves positions.
                 */
                SpanTermQuery spanTermQuery = new SpanTermQuery(((TermQuery) subQuery).getTerm());
                spanQuery = new FieldMaskingSpanQuery(spanTermQuery, fieldName);
            }
        } else {
            if (subQuery instanceof MultiTermQuery == false) {
                throw new UnsupportedOperationException("unsupported inner query, should be "
                    + MultiTermQuery.class.getName() + " but was " + subQuery.getClass().getName());
            }
            spanQuery = new SpanMultiTermQueryWrapper<>((MultiTermQuery) subQuery);
        }
        if (subQuery instanceof MultiTermQuery) {
            MultiTermQuery multiTermQuery = (MultiTermQuery) subQuery;
            SpanMultiTermQueryWrapper<?> wrapper = (SpanMultiTermQueryWrapper<?>) spanQuery;
            if (multiTermQuery.getRewriteMethod() instanceof TopTermsRewrite == false) {
                wrapper.setRewriteMethod(new TopTermSpanBooleanQueryRewriteWithMaxClause());
            }
        }
        if (boost != AbstractQueryBuilder.DEFAULT_BOOST) {
            return new SpanBoostQuery(spanQuery, boost);
        }

        return spanQuery;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(multiTermQueryBuilder);
    }

    @Override
    protected boolean doEquals(SpanMultiTermQueryBuilder other) {
        return Objects.equals(multiTermQueryBuilder, other.multiTermQueryBuilder);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
