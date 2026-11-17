/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Stops a stage query from rewriting against another source's shared-PIT shard. Timestamp bounds and field
 * predicates may be invalid for an unrelated index mapping, even when an index filter would return no hits.
 */
public final class SequenceSourceQueryBuilder extends AbstractQueryBuilder<SequenceSourceQueryBuilder> {
    private static final String KEY_FILTER_ENVELOPE = "__esql_sequence_waiting_keys_v1";
    public static final TransportVersion ESQL_SEQUENCE_R6_SOURCE_SCOPE = TransportVersion.fromName("esql_sequence_r6_source_scope");
    public static final String NAME = "esql_sequence_source_scope";
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        QueryBuilder.class,
        NAME,
        SequenceSourceQueryBuilder::new
    );

    private final QueryBuilder query;
    private final List<String> indices;

    /**
     * Retains the source's physical indices so unrelated shared-PIT slots stay empty before delegate rewriting.
     * Even a clause that ultimately matches nothing can otherwise parse timestamp bounds against a foreign mapping.
     */
    public SequenceSourceQueryBuilder(QueryBuilder query, List<String> indices) {
        this.query = Objects.requireNonNull(query);
        this.indices = List.copyOf(indices);
    }

    /** Restores source isolation only on peers that understand this internal query. */
    public SequenceSourceQueryBuilder(StreamInput in) throws IOException {
        super(checkSupported(in));
        query = in.readNamedWriteable(QueryBuilder.class);
        indices = in.readStringCollectionAsList();
    }

    private static StreamInput checkSupported(StreamInput in) {
        checkSupported(in.getTransportVersion());
        return in;
    }

    private static void checkSupported(TransportVersion version) {
        if (version.supports(ESQL_SEQUENCE_R6_SOURCE_SCOPE) == false) {
            throw new IllegalArgumentException("SEQUENCE source isolation is not supported by transport version [" + version + "]");
        }
    }

    /** Public Query DSL cannot create this compiler-owned validation boundary. */
    public static SequenceSourceQueryBuilder fromXContent(XContentParser parser) {
        throw new XContentParseException(parser.getTokenLocation(), "[" + NAME + "] is reserved for internal ES|QL searches");
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext context) throws IOException {
        var index = context.getFullyQualifiedIndex();
        if (index != null) {
            if (indices.contains(index.getName()) == false) {
                return new MatchNoneQueryBuilder("PIT slot is outside this SEQUENCE source");
            }
            validateTimestamp(context);
        }
        // Coordinator/own-shard rewrites retain ordinary async fetches such as terms lookup. A known foreign
        // shard must never inspect the delegate's field mappings or register one of its async actions.
        BoolQueryBuilder envelope = keyFilterEnvelope(query);
        if (envelope == null) {
            QueryBuilder rewritten = query.rewrite(context);
            return rewritten == query ? this : new SequenceSourceQueryBuilder(rewritten, indices);
        }
        QueryBuilder base = envelope.filter().getFirst();
        QueryBuilder predicate = envelope.filter().get(1);
        QueryBuilder rewritten = base.rewrite(context);
        if (index != null && (predicate instanceof MatchAllQueryBuilder) == false && indexedKeyPredicate(predicate, context, 0) == false) {
            // Keep the structural envelope until Lucene conversion: a user-named base must never become a compiler tag.
            return new SequenceSourceQueryBuilder(withKeyFilter(rewritten, new MatchAllQueryBuilder()), indices);
        }
        // Rewriting the entire Bool could erase its base on MatchNone. Keep generated values opaque until toQuery.
        return rewritten == base ? this : new SequenceSourceQueryBuilder(withKeyFilter(rewritten, predicate), indices);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context, MaxClauseCountQueryVisitor visitor) throws IOException {
        if (indices.contains(context.index().getName()) == false) {
            return new MatchNoneQueryBuilder("PIT slot is outside this SEQUENCE source").toQuery(context, visitor);
        }
        validateTimestamp(context);
        BoolQueryBuilder envelope = keyFilterEnvelope(query);
        if (envelope == null) {
            return Rewriteable.rewrite(query, context, true).toQuery(context, visitor);
        }
        // Compile the entire ordinary base first so an optional MatchNone cannot hide its mapping/query refusals.
        Query base = Rewriteable.rewrite(envelope.filter().getFirst(), context, true).toQuery(context, visitor);
        QueryBuilder predicate = envelope.filter().get(1);
        int remainingClauses = visitor.getMaxClauseCount() - visitor.getNumClauses();
        if (remainingClauses <= 0 || predicate instanceof MatchAllQueryBuilder || indexedKeyPredicate(predicate, context, 0) == false) {
            return base;
        }
        var queryBreaker = context.getCircuitBreaker();
        MaxClauseCountQueryVisitor additional = new MaxClauseCountQueryVisitor(
            remainingClauses,
            queryBreaker,
            context::isQueryMemoryPreCharged,
            MaxClauseCountQueryVisitor.segmentCountOrDefault(context.getIndexReader()),
            context.getIndexReader()
        );
        Query filter;
        try {
            filter = Rewriteable.rewrite(predicate, context, true).toQuery(context, additional);
        } catch (IndexSearcher.TooManyNestedClauses | CircuitBreakingException optionalLimit) {
            // The shared visitor is untouched: an optional predicate must not turn a valid base into a limit failure.
            return base;
        }
        // Real mapper-only contexts may omit a query-construction breaker, as permitted by SearchExecutionContext.
        if (queryBreaker != null) {
            long limit = queryBreaker.getLimit();
            if (limit >= 0 && queryBreaker.getUsed() + visitor.getEstimatedBytes() + additional.getEstimatedBytes() > limit) {
                return base;
            }
        }
        visitor.merge(additional);
        // The compiler marker never enters named-query matching; user names inside the base remain unchanged.
        return new BooleanQuery.Builder().add(base, BooleanClause.Occur.FILTER).add(filter, BooleanClause.Occur.FILTER).build();
    }

    /** Adds an exact compiler-owned envelope only after all ordinary stage and time predicates are assembled. */
    static QueryBuilder withKeyFilter(QueryBuilder base, QueryBuilder predicate) {
        return new BoolQueryBuilder().filter(base).filter(predicate).queryName(KEY_FILTER_ENVELOPE);
    }

    private static BoolQueryBuilder keyFilterEnvelope(QueryBuilder candidate) {
        if (KEY_FILTER_ENVELOPE.equals(candidate.queryName()) == false) {
            return null;
        }
        if (candidate instanceof BoolQueryBuilder bool
            && bool.filter().size() == 2
            && bool.must().isEmpty()
            && bool.mustNot().isEmpty()
            && bool.should().isEmpty()
            && bool.minimumShouldMatch() == null
            && bool.boost() == DEFAULT_BOOST
            && bool.adjustPureNegative()) {
            return bool;
        }
        throw new IllegalStateException("Invalid compiler-owned SEQUENCE key-filter envelope");
    }

    private static boolean indexedKeyPredicate(QueryBuilder predicate, QueryRewriteContext context, int depth) {
        if (depth > 2 || predicate.queryName() != null || predicate.boost() != DEFAULT_BOOST) {
            return false;
        }
        if (predicate instanceof TermQueryBuilder term) {
            return term.caseInsensitive() == false && indexedKeyTerm(term, context);
        }
        if (predicate instanceof BoolQueryBuilder bool && bool.must().isEmpty() && bool.mustNot().isEmpty() && bool.adjustPureNegative()) {
            if (bool.filter().isEmpty() && bool.should().isEmpty() == false && "1".equals(bool.minimumShouldMatch())) {
                return bool.should().stream().allMatch(child -> indexedKeyPredicate(child, context, depth + 1));
            }
            if (bool.should().isEmpty() && bool.filter().isEmpty() == false && bool.minimumShouldMatch() == null) {
                return bool.filter().stream().allMatch(child -> indexedKeyPredicate(child, context, depth + 1));
            }
        }
        return false;
    }

    private static boolean indexedKeyTerm(TermQueryBuilder term, QueryRewriteContext context) {
        if (context.getIndexSettings() == null || context.getIndexSettings().getMode() != IndexMode.STANDARD) {
            return false;
        }
        var effective = context.getFieldType(term.fieldName());
        if (effective == null
            || effective.name().equals(term.fieldName()) == false
            || effective.hasDocValues() == false
            || effective.hasScriptValues()) {
            return false;
        }
        var indexed = context.getMappingLookup().getMapper(term.fieldName());
        if (indexed instanceof FieldMapper == false || ((FieldMapper) indexed).fieldType() != effective) {
            return false;
        }
        Object value = term.value();
        if (effective.getClass() == KeywordFieldMapper.KeywordFieldType.class
            && effective instanceof KeywordFieldMapper.KeywordFieldType keyword) {
            // Under NONE/DOC_VALUES this exact mapper loads indexed sorted-set bytes. Historical ignore_above
            // exclusions have no such bytes; current source-fetch settings are never reapplied to these keys.
            return value instanceof String text
                && keyword.indexType().hasTerms()
                && keyword.usesBinaryDocValues() == false
                && keyword.hasNormalizer() == false
                && keyword.getTextSearchInfo().searchAnalyzer() == Lucene.KEYWORD_ANALYZER
                && keyword.hasNullValue() == false
                && keyword.hasScriptValues() == false
                && (text.isEmpty() == false || keyword.emptyStringReadsAsNull() == false);
        }
        if (effective instanceof BooleanFieldMapper.BooleanFieldType) {
            return value instanceof Boolean && effective.indexType().hasTerms();
        }
        if (effective.getClass() == NumberFieldMapper.NumberFieldType.class
            && effective instanceof NumberFieldMapper.NumberFieldType number
            && number.indexType().hasPoints()) {
            if (number.numberType() == NumberFieldMapper.NumberType.BYTE) {
                return value instanceof Integer integer && integer >= Byte.MIN_VALUE && integer <= Byte.MAX_VALUE;
            }
            if (number.numberType() == NumberFieldMapper.NumberType.SHORT) {
                return value instanceof Integer integer && integer >= Short.MIN_VALUE && integer <= Short.MAX_VALUE;
            }
            if (number.numberType() == NumberFieldMapper.NumberType.INTEGER) {
                return value instanceof Integer;
            }
            if (number.numberType() == NumberFieldMapper.NumberType.LONG) {
                return value instanceof Long;
            }
        }
        return false;
    }

    /**
     * Source time must keep the indexed identity across stage searches. Resolve aliases through the effective
     * type's target so an ordinary mapped timestamp alias remains supported while runtime shadows are refused.
     */
    private static void validateTimestamp(QueryRewriteContext context) {
        var effective = context.getFieldType("@timestamp");
        if (effective == null) {
            return;
        }
        var indexed = context.getMappingLookup().getMapper(effective.name());
        if (indexed instanceof FieldMapper field && field.fieldType() == effective) {
            return;
        }
        throw new IllegalArgumentException("SEQUENCE does not support a runtime [@timestamp]");
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        checkSupported(out.getTransportVersion());
        out.writeNamedWriteable(query);
        out.writeStringCollection(indices);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("query", query, params);
        builder.stringListField("indices", indices);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ESQL_SEQUENCE_R6_SOURCE_SCOPE;
    }

    @Override
    protected boolean doEquals(SequenceSourceQueryBuilder other) {
        return query.equals(other.query) && indices.equals(other.indices);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, indices);
    }
}
