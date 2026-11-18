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
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.index.query.RegexpQueryBuilder;
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
import java.util.function.Consumer;

/**
 * Stops a stage query from rewriting against another source's shared-PIT shard. Timestamp bounds and field
 * predicates may be invalid for an unrelated index mapping, even when an index filter would return no hits.
 */
public final class SequenceSourceQueryBuilder extends AbstractQueryBuilder<SequenceSourceQueryBuilder> {
    private static final String KEY_FILTER_ENVELOPE = "__esql_sequence_waiting_keys_v1";
    private static final String WILDCARD_OPTION = "__esql_sequence_wildcard_option_v1";
    public static final TransportVersion ESQL_SEQUENCE_R6_SOURCE_SCOPE = TransportVersion.fromName("esql_sequence_r6_source_scope");
    public static final String NAME = "esql_sequence_source_scope";
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        QueryBuilder.class,
        NAME,
        SequenceSourceQueryBuilder::new
    );

    private final QueryBuilder query;
    private final List<String> indices;
    @Nullable
    private final ThreadContext threadContext;

    /**
     * Retains the source's physical indices so unrelated shared-PIT slots stay empty before delegate rewriting.
     * Even a clause that ultimately matches nothing can otherwise parse timestamp bounds against a foreign mapping.
     */
    public SequenceSourceQueryBuilder(QueryBuilder query, List<String> indices) {
        this(query, indices, null);
    }

    public SequenceSourceQueryBuilder(QueryBuilder query, List<String> indices, @Nullable ThreadContext threadContext) {
        this.query = Objects.requireNonNull(query);
        this.indices = List.copyOf(indices);
        this.threadContext = threadContext;
    }

    /** Restores source isolation only on peers that understand this internal query. */
    public SequenceSourceQueryBuilder(StreamInput in) throws IOException {
        this(in, null);
    }

    public SequenceSourceQueryBuilder(StreamInput in, @Nullable ThreadContext threadContext) throws IOException {
        super(checkSupported(in));
        query = in.readNamedWriteable(QueryBuilder.class);
        indices = in.readStringCollectionAsList();
        this.threadContext = threadContext;
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
            return rewritten == query ? this : new SequenceSourceQueryBuilder(rewritten, indices, threadContext);
        }
        QueryBuilder base = envelope.filter().getFirst();
        QueryBuilder predicate = envelope.filter().get(1);
        QueryBuilder rewritten = base.rewrite(context);
        if (index != null && (predicate instanceof MatchAllQueryBuilder) == false && wildcardOption(predicate) == null) {
            String rejection = keyPredicateRejection(predicate, context, 0);
            if (rejection != null) {
                // Preserve the ordinary fallback. Its compiler-owned name, when requested, is never converted to a named query.
                var fallback = new MatchAllQueryBuilder();
                if (SequenceKeyFilterShardProfile.enabled(threadContext)) {
                    fallback.queryName(rejection);
                }
                return new SequenceSourceQueryBuilder(withKeyFilter(rewritten, fallback), indices, threadContext);
            }
        }
        // Rewriting the entire Bool could erase its base on MatchNone. Keep generated values opaque until toQuery.
        return rewritten == base ? this : new SequenceSourceQueryBuilder(withKeyFilter(rewritten, predicate), indices, threadContext);
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
        BoolQueryBuilder option = wildcardOption(predicate);
        if (option != null) {
            // The required search is already compiled. Each option keeps its own mapping and resource fallback.
            if (option.filter().isEmpty() == false) {
                base = applyKeyFilter(base, option.filter().getFirst(), context, visitor);
            }
            RegexpQueryBuilder regexp = (RegexpQueryBuilder) option.must().getFirst();
            if (context.getIndexSettings() != null
                && regexp.value().length() <= context.getIndexSettings().getMaxRegexLength()
                && mappedKeyRejection(regexp.fieldName(), "", context) == null) {
                base = optionalFilter(base, regexp, context, visitor, reason -> {});
            }
            return base;
        }
        return applyKeyFilter(base, predicate, context, visitor);
    }

    private Query applyKeyFilter(Query base, QueryBuilder predicate, SearchExecutionContext context, MaxClauseCountQueryVisitor visitor)
        throws IOException {
        if (visitor.getNumClauses() >= visitor.getMaxClauseCount()) {
            recordKeyFilterDecision(context, "clause_limit");
            return base;
        }
        String rejection = predicate instanceof MatchAllQueryBuilder
            ? Objects.requireNonNullElse(predicate.queryName(), "ineligible_predicate")
            : keyPredicateRejection(predicate, context, 0);
        if (rejection != null) {
            recordKeyFilterDecision(context, rejection);
            return base;
        }
        return optionalFilter(base, predicate, context, visitor, reason -> recordKeyFilterDecision(context, reason));
    }

    private static Query optionalFilter(
        Query base,
        QueryBuilder predicate,
        SearchExecutionContext context,
        MaxClauseCountQueryVisitor visitor,
        Consumer<String> decision
    ) throws IOException {
        int remaining = visitor.getMaxClauseCount() - visitor.getNumClauses();
        if (remaining <= 0) {
            decision.accept("clause_limit");
            return base;
        }
        boolean regexp = predicate instanceof RegexpQueryBuilder;
        var queryBreaker = context.getCircuitBreaker();
        var additional = new MaxClauseCountQueryVisitor(
            remaining,
            regexp ? null : queryBreaker,
            context::isQueryMemoryPreCharged,
            MaxClauseCountQueryVisitor.segmentCountOrDefault(context.getIndexReader()),
            context.getIndexReader()
        );
        Query filtered;
        try {
            Query filter;
            if (regexp) {
                boolean allowExpensive = context.allowExpensiveQueries();
                if (allowExpensive == false) {
                    return base;
                }
                // The mapper precharges and caches automata. Release its private construction state on every exit.
                // The outer visitor estimates the full retained Lucene leaf and core charges it on successful admission.
                var construction = new SearchExecutionContext(context) {
                    @Override
                    public boolean allowExpensiveQueries() {
                        return allowExpensive; // Use the same shard decision if the live setting changes during construction.
                    }
                };
                try {
                    filter = Rewriteable.rewrite(predicate, construction, true).toQuery(construction, additional);
                } finally {
                    construction.releaseQueryConstructionMemory();
                }
            } else {
                filter = Rewriteable.rewrite(predicate, context, true).toQuery(context, additional);
            }
            filtered = new BooleanQuery.Builder().add(base, BooleanClause.Occur.FILTER).add(filter, BooleanClause.Occur.FILTER).build();
            // Core charges the full root later. Trial the portion visible here, including child overhead and the parent.
            if (queryBreaker != null) {
                long bytes = visitor.getEstimatedBytes() + additional.getEstimatedBytes();
                // merge also checks the raw projection, even when breaker overhead is less than one.
                long limit = queryBreaker.getLimit();
                if (limit >= 0 && queryBreaker.getUsed() + bytes > limit) {
                    decision.accept("query_breaker");
                    return base;
                }
                queryBreaker.addEstimateBytesAndMaybeBreak(bytes, ChildMemoryCircuitBreaker.CATEGORY_QUERY);
                queryBreaker.addWithoutBreaking(-bytes, ChildMemoryCircuitBreaker.CATEGORY_QUERY);
            }
        } catch (IndexSearcher.TooManyClauses e) {
            decision.accept("clause_limit");
            return base;
        } catch (CircuitBreakingException e) {
            decision.accept("query_breaker");
            return base;
        } catch (TooComplexToDeterminizeException | IllegalArgumentException e) {
            if (regexp) {
                return base;
            }
            throw e;
        }
        visitor.merge(additional);
        decision.accept(SequenceKeyFilterShardProfile.APPLIED);
        return filtered;
    }

    private void recordKeyFilterDecision(SearchExecutionContext context, String decision) {
        if (SequenceKeyFilterShardProfile.enabled(threadContext)) {
            SequenceKeyFilterShardProfile.record(threadContext, context, decision);
        }
    }

    /** Adds an exact compiler-owned envelope only after all ordinary stage and time predicates are assembled. */
    static QueryBuilder withKeyFilter(QueryBuilder base, QueryBuilder predicate) {
        return new BoolQueryBuilder().filter(base).filter(predicate).queryName(KEY_FILTER_ENVELOPE);
    }

    /** Part-5 shards recognize the outer optional slot and decline its unknown named payload. */
    static QueryBuilder withOptionalFilters(QueryBuilder base, @Nullable QueryBuilder keys, @Nullable RegexpQueryBuilder wildcard) {
        if (wildcard == null) {
            return keys == null ? base : withKeyFilter(base, keys);
        }
        var option = new BoolQueryBuilder().must(wildcard).queryName(WILDCARD_OPTION);
        if (keys != null) {
            option.filter(keys);
        }
        return withKeyFilter(base, option);
    }

    @Nullable
    private static BoolQueryBuilder wildcardOption(QueryBuilder predicate) {
        if (WILDCARD_OPTION.equals(predicate.queryName()) == false) {
            return null;
        }
        if (predicate instanceof BoolQueryBuilder bool
            && bool.must().size() == 1
            && bool.must().getFirst() instanceof RegexpQueryBuilder
            && bool.filter().size() <= 1
            && bool.should().isEmpty()
            && bool.mustNot().isEmpty()
            && bool.minimumShouldMatch() == null
            && bool.boost() == DEFAULT_BOOST
            && bool.adjustPureNegative()) {
            return bool;
        }
        throw new IllegalStateException("Invalid compiler-owned SEQUENCE wildcard option");
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

    private static String keyPredicateRejection(QueryBuilder predicate, QueryRewriteContext context, int depth) {
        if (depth > 2 || predicate.queryName() != null || predicate.boost() != DEFAULT_BOOST) {
            return "ineligible_predicate";
        }
        if (predicate instanceof TermQueryBuilder term) {
            return term.caseInsensitive() ? "ineligible_predicate" : mappedKeyRejection(term.fieldName(), term.value(), context);
        }
        if (predicate instanceof BoolQueryBuilder bool && bool.must().isEmpty() && bool.mustNot().isEmpty() && bool.adjustPureNegative()) {
            final List<QueryBuilder> children;
            if (bool.filter().isEmpty() && bool.should().isEmpty() == false && "1".equals(bool.minimumShouldMatch())) {
                children = bool.should();
            } else if (bool.should().isEmpty() && bool.filter().isEmpty() == false && bool.minimumShouldMatch() == null) {
                children = bool.filter();
            } else {
                return "ineligible_predicate";
            }
            for (QueryBuilder child : children) {
                String rejection = keyPredicateRejection(child, context, depth + 1);
                if (rejection != null) {
                    return rejection;
                }
            }
            return null;
        }
        return "ineligible_predicate";
    }

    private static String mappedKeyRejection(String field, Object value, QueryRewriteContext context) {
        var settings = context.getIndexSettings();
        IndexMode mode = settings == null ? null : settings.getMode();
        if (mode != IndexMode.STANDARD && mode != IndexMode.LOGSDB) {
            return "index_mode";
        }
        var effective = context.getFieldType(field);
        if (effective == null
            || effective.name().equals(field) == false
            || effective.hasDocValues() == false
            || effective.hasScriptValues()) {
            return "ineligible_field";
        }
        var indexed = context.getMappingLookup().getMapper(field);
        if (indexed instanceof FieldMapper == false || ((FieldMapper) indexed).fieldType() != effective) {
            return "ineligible_field";
        }
        if (effective.getClass() == KeywordFieldMapper.KeywordFieldType.class
            && effective instanceof KeywordFieldMapper.KeywordFieldType keyword) {
            return keywordTermRejection(keyword, value);
        }
        if (effective instanceof BooleanFieldMapper.BooleanFieldType && value instanceof Boolean && effective.indexType().hasTerms()) {
            return null;
        }
        if (effective.getClass() == NumberFieldMapper.NumberFieldType.class
            && effective instanceof NumberFieldMapper.NumberFieldType number
            && number.indexType().hasPoints()) {
            boolean supported = switch (number.numberType()) {
                case BYTE -> value instanceof Integer integer && integer >= Byte.MIN_VALUE && integer <= Byte.MAX_VALUE;
                case SHORT -> value instanceof Integer integer && integer >= Short.MIN_VALUE && integer <= Short.MAX_VALUE;
                case INTEGER -> value instanceof Integer;
                case LONG -> value instanceof Long;
                default -> false;
            };
            if (supported) {
                return null;
            }
        }
        return "ineligible_field";
    }

    static String keywordTermRejection(KeywordFieldMapper.KeywordFieldType keyword, Object value) {
        // Under NONE/DOC_VALUES this exact mapper loads indexed sorted-set bytes. Historical ignore_above
        // exclusions have no such bytes; current source-fetch settings are never reapplied to these keys.
        if (keyword.usesBinaryDocValues()) {
            return "binary_doc_values";
        }
        if (keyword.indexType().hasTerms() == false) {
            return "no_indexed_terms";
        }
        if (value instanceof String text
            && keyword.hasNormalizer() == false
            && keyword.getTextSearchInfo().searchAnalyzer() == Lucene.KEYWORD_ANALYZER
            && keyword.hasNullValue() == false
            && keyword.hasScriptValues() == false
            && (text.isEmpty() == false || keyword.emptyStringReadsAsNull() == false)) {
            return null;
        }
        return "ineligible_keyword";
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
