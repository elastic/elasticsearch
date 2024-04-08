/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.ql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeMap;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.ConstantInput;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.querydsl.container.Sort;
import org.elasticsearch.xpack.ql.querydsl.query.BoolQuery;
import org.elasticsearch.xpack.ql.querydsl.query.MatchAll;
import org.elasticsearch.xpack.ql.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.execution.search.SourceGenerator;
import org.elasticsearch.xpack.sql.expression.function.Score;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.ScorePipe;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByKey;
import org.elasticsearch.xpack.sql.querydsl.agg.LeafAgg;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.ql.util.CollectionUtils.combine;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.ALLOW_PARTIAL_SEARCH_RESULTS;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.INDEX_INCLUDE_FROZEN;

/**
 * Container for various references of the built ES query.
 * Useful to understanding how to interpret and navigate the
 * returned result.
 */
public class QueryContainer {

    private final Aggs aggs;
    private final Query query;

    public static class FieldInfo {
        private final FieldExtraction extraction;
        private final String id;
        private final Attribute attribute;

        public FieldInfo(FieldExtraction extraction, String id, Attribute attribute) {
            this.extraction = extraction;
            this.id = id;
            this.attribute = attribute;
        }

        public FieldExtraction extraction() {
            return extraction;
        }

        public String id() {
            return id;
        }

        public Attribute attribute() {
            return attribute;
        }
    }

    // fields extracted from the response - not necessarily what the client sees
    // for example in case of grouping or custom sorting, the response has extra columns
    // that is filtered before getting to the client
    private final List<FieldInfo> fields;

    // aliases found in the tree
    private final AttributeMap<Expression> aliases;

    // pseudo functions (like count) - that are 'extracted' from other aggs
    private final Map<String, GroupByKey> pseudoFunctions;

    // scalar function processors - recorded as functions get folded;
    // at scrolling, their inputs (leaves) get updated
    private AttributeMap<Pipe> scalarFunctions;

    private final Map<String, Sort> sort;
    private final int limit;
    private final boolean trackHits;
    private final boolean includeFrozen;
    // used when pivoting for retrieving at least one pivot row
    private final int minPageSize;
    private final boolean allowPartialSearchResults;

    // computed
    private Boolean aggsOnly;
    private Boolean customSort;
    // associate Attributes with aliased FieldAttributes (since they map directly to ES fields)
    private Map<Attribute, FieldAttribute> fieldAlias;

    public QueryContainer() {
        this(null, null, null, null, null, null, null, -1, false, INDEX_INCLUDE_FROZEN, -1, ALLOW_PARTIAL_SEARCH_RESULTS);
    }

    public QueryContainer(
        Query query,
        Aggs aggs,
        List<FieldInfo> fields,
        AttributeMap<Expression> aliases,
        Map<String, GroupByKey> pseudoFunctions,
        AttributeMap<Pipe> scalarFunctions,
        Map<String, Sort> sort,
        int limit,
        boolean trackHits,
        boolean includeFrozen,
        int minPageSize,
        boolean allowPartialSearchResults
    ) {
        this.query = query;
        this.aggs = aggs == null ? Aggs.EMPTY : aggs;
        this.fields = fields == null || fields.isEmpty() ? emptyList() : fields;
        this.aliases = aliases == null || aliases.isEmpty() ? AttributeMap.emptyAttributeMap() : aliases;
        this.pseudoFunctions = pseudoFunctions == null || pseudoFunctions.isEmpty() ? emptyMap() : pseudoFunctions;
        this.scalarFunctions = scalarFunctions == null || scalarFunctions.isEmpty() ? AttributeMap.emptyAttributeMap() : scalarFunctions;
        this.sort = sort == null || sort.isEmpty() ? emptyMap() : sort;
        this.limit = limit;
        this.trackHits = trackHits;
        this.includeFrozen = includeFrozen;
        this.minPageSize = minPageSize;
        this.allowPartialSearchResults = allowPartialSearchResults;
    }

    /**
     * If needed, create a comparator for each indicated column (which is indicated by an index pointing to the column number from the
     * result set).
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public List<Tuple<Integer, Comparator>> sortingColumns() {
        if (customSort == Boolean.FALSE) {
            return emptyList();
        }

        for (Sort s : sort.values()) {
            if (s instanceof AggregateSort) {
                customSort = Boolean.TRUE;
                break;
            }
        }

        // If no custom sort is used break early
        if (customSort == null) {
            customSort = Boolean.FALSE;
            return emptyList();
        }

        List<Tuple<Integer, Comparator>> sortingColumns = new ArrayList<>(sort.size());
        for (Map.Entry<String, Sort> entry : sort.entrySet()) {
            String expressionId = entry.getKey();
            Sort s = entry.getValue();

            int atIndex = -1;
            for (int i = 0; i < fields.size(); i++) {
                FieldInfo field = fields.get(i);
                if (field.id().equals(expressionId)) {
                    atIndex = i;
                    break;
                }
            }
            if (atIndex == -1) {
                throw new SqlIllegalArgumentException("Cannot find backing column for ordering aggregation [{}]", s);
            }

            // assemble a comparator for it, if it's not an AggregateSort
            // then it's pre-sorted by ES so use null
            Comparator comp = null;
            if (s instanceof AggregateSort) {
                comp = s.direction() == Sort.Direction.ASC ? Comparator.naturalOrder() : Comparator.reverseOrder();
                comp = s.missing() == Sort.Missing.FIRST ? Comparator.nullsFirst(comp) : Comparator.nullsLast(comp);
            }

            sortingColumns.add(new Tuple<>(Integer.valueOf(atIndex), comp));
        }

        return sortingColumns;
    }

    /**
     * Since the container contains both the field extractors and the visible columns,
     * compact the information in the listener through a bitset that acts as a mask
     * on what extractors are used for the visible columns.
     */
    public BitSet columnMask(List<Attribute> columns) {
        BitSet mask = new BitSet(fields.size());
        if (columns.size() > 0) {
            aliasName(columns.get(0));
        }

        for (Attribute column : columns) {
            Expression expression = aliases.resolve(column, column);

            // find the column index
            String id = Expressions.id(expression);
            int index = -1;

            for (int i = 0; i < fields.size(); i++) {
                FieldInfo field = fields.get(i);
                // if the index is already set there is a collision,
                // so continue searching for the other field with the same id
                if (mask.get(i) == false && field.id().equals(id)) {
                    index = i;
                    break;
                }
            }

            if (index > -1) {
                mask.set(index);
            } else {
                throw new SqlIllegalArgumentException("Cannot resolve field extractor index for column [{}]", column);
            }
        }
        return mask;
    }

    public Query query() {
        return query;
    }

    public Aggs aggs() {
        return aggs;
    }

    public List<FieldInfo> fields() {
        return fields;
    }

    public AttributeMap<Expression> aliases() {
        return aliases;
    }

    public Map<String, GroupByKey> pseudoFunctions() {
        return pseudoFunctions;
    }

    public Map<String, Sort> sort() {
        return sort;
    }

    public int limit() {
        return limit;
    }

    public boolean isAggsOnly() {
        if (aggsOnly == null) {
            aggsOnly = Boolean.valueOf(this.fields.stream().anyMatch(t -> t.extraction().supportedByAggsOnlyQuery()));
        }

        return aggsOnly.booleanValue();
    }

    public boolean hasColumns() {
        return fields.size() > 0;
    }

    public boolean shouldTrackHits() {
        return trackHits;
    }

    public boolean shouldIncludeFrozen() {
        return includeFrozen;
    }

    public boolean allowPartialSearchResults() {
        return allowPartialSearchResults;
    }

    public int minPageSize() {
        return minPageSize;
    }

    //
    // copy methods
    //

    public QueryContainer with(Query q) {
        return new QueryContainer(
            q,
            aggs,
            fields,
            aliases,
            pseudoFunctions,
            scalarFunctions,
            sort,
            limit,
            trackHits,
            includeFrozen,
            minPageSize,
            allowPartialSearchResults
        );
    }

    public QueryContainer withAliases(AttributeMap<Expression> a) {
        return new QueryContainer(
            query,
            aggs,
            fields,
            a,
            pseudoFunctions,
            scalarFunctions,
            sort,
            limit,
            trackHits,
            includeFrozen,
            minPageSize,
            allowPartialSearchResults
        );
    }

    public QueryContainer withPseudoFunctions(Map<String, GroupByKey> p) {
        return new QueryContainer(
            query,
            aggs,
            fields,
            aliases,
            p,
            scalarFunctions,
            sort,
            limit,
            trackHits,
            includeFrozen,
            minPageSize,
            allowPartialSearchResults
        );
    }

    public QueryContainer with(Aggs a) {
        return new QueryContainer(
            query,
            a,
            fields,
            aliases,
            pseudoFunctions,
            scalarFunctions,
            sort,
            limit,
            trackHits,
            includeFrozen,
            minPageSize,
            allowPartialSearchResults
        );
    }

    public QueryContainer withLimit(int l) {
        return l == limit
            ? this
            : new QueryContainer(
                query,
                aggs,
                fields,
                aliases,
                pseudoFunctions,
                scalarFunctions,
                sort,
                l,
                trackHits,
                includeFrozen,
                minPageSize,
                allowPartialSearchResults
            );
    }

    public QueryContainer withTrackHits() {
        return trackHits
            ? this
            : new QueryContainer(
                query,
                aggs,
                fields,
                aliases,
                pseudoFunctions,
                scalarFunctions,
                sort,
                limit,
                true,
                includeFrozen,
                minPageSize,
                allowPartialSearchResults
            );
    }

    public QueryContainer withFrozen() {
        return includeFrozen
            ? this
            : new QueryContainer(
                query,
                aggs,
                fields,
                aliases,
                pseudoFunctions,
                scalarFunctions,
                sort,
                limit,
                trackHits,
                true,
                minPageSize,
                allowPartialSearchResults
            );
    }

    public QueryContainer withScalarProcessors(AttributeMap<Pipe> procs) {
        return new QueryContainer(
            query,
            aggs,
            fields,
            aliases,
            pseudoFunctions,
            procs,
            sort,
            limit,
            trackHits,
            includeFrozen,
            minPageSize,
            allowPartialSearchResults
        );
    }

    /**
     * Adds a sort expression that takes precedence over all existing sort expressions. Expressions are prepended because the logical plan
     * is folded from bottom up. So the most significant sort order will be added last.
     */
    public QueryContainer prependSort(String expressionId, Sort sortable) {
        Map<String, Sort> newSort = Maps.newLinkedHashMapWithExpectedSize(this.sort.size() + 1);
        newSort.put(expressionId, sortable);
        for (Map.Entry<String, Sort> entry : this.sort.entrySet()) {
            newSort.putIfAbsent(entry.getKey(), entry.getValue());
        }
        return new QueryContainer(
            query,
            aggs,
            fields,
            aliases,
            pseudoFunctions,
            scalarFunctions,
            newSort,
            limit,
            trackHits,
            includeFrozen,
            minPageSize,
            allowPartialSearchResults
        );
    }

    private String aliasName(Attribute attr) {
        if (fieldAlias == null) {
            fieldAlias = new LinkedHashMap<>();
            for (Map.Entry<Attribute, Expression> entry : aliases.entrySet()) {
                if (entry.getValue() instanceof FieldAttribute) {
                    fieldAlias.put(entry.getKey(), (FieldAttribute) entry.getValue());
                }
            }
        }
        FieldAttribute fa = fieldAlias.get(attr);
        return fa != null ? fa.name() : attr.name();
    }

    //
    // reference methods
    //
    private FieldExtraction topHitFieldRef(FieldAttribute fieldAttr) {
        return new SearchHitFieldRef(aliasName(fieldAttr), fieldAttr.field().getDataType());
    }

    private Tuple<QueryContainer, FieldExtraction> nestedHitFieldRef(FieldAttribute attr) {
        String name = aliasName(attr);
        Query q = rewriteToContainNestedField(
            query,
            attr.source(),
            attr.nestedParent().name(),
            name,
            SqlDataTypes.format(attr.field().getDataType()),
            SqlDataTypes.isFromDocValuesOnly(attr.field().getDataType())
        );

        SearchHitFieldRef nestedFieldRef = new SearchHitFieldRef(name, attr.field().getDataType(), attr.nestedParent().name());

        return new Tuple<>(
            new QueryContainer(
                q,
                aggs,
                fields,
                aliases,
                pseudoFunctions,
                scalarFunctions,
                sort,
                limit,
                trackHits,
                includeFrozen,
                minPageSize,
                allowPartialSearchResults
            ),
            nestedFieldRef
        );
    }

    static Query rewriteToContainNestedField(
        @Nullable Query query,
        Source source,
        String path,
        String name,
        String format,
        boolean hasDocValues
    ) {
        if (query == null) {
            /* There is no query so we must add the nested query
             * ourselves to fetch the field. */
            return new NestedQuery(
                source,
                path,
                singletonMap(name, new AbstractMap.SimpleImmutableEntry<>(hasDocValues, format)),
                new MatchAll(source)
            );
        }
        if (query.containsNestedField(path, name)) {
            // The query already has the nested field. Nothing to do.
            return query;
        }
        /* The query doesn't have the nested field so we have to ask
         * it to add it. */
        Query rewritten = query.addNestedField(path, name, format, hasDocValues);
        if (rewritten != query) {
            /* It successfully added it so we can use the rewritten
             * query. */
            return rewritten;
        }
        /* There is no nested query with a matching path so we must
         * add the nested query ourselves just to fetch the field. */
        NestedQuery nested = new NestedQuery(
            source,
            path,
            singletonMap(name, new AbstractMap.SimpleImmutableEntry<>(hasDocValues, format)),
            new MatchAll(source)
        );
        return new BoolQuery(source, true, query, nested);
    }

    // replace function/operators's input with references
    private Tuple<QueryContainer, FieldExtraction> resolvedTreeComputingRef(ScalarFunction function, Attribute attr) {
        Pipe proc = null;
        if ((proc = scalarFunctions.resolve(attr)) == null) {
            proc = function.asPipe();
            scalarFunctions = AttributeMap.builder(scalarFunctions).put(attr, proc).build();
        }

        // find the processor inputs (Attributes) and convert them into references
        // no need to promote them to the top since the container doesn't have to be aware
        class QueryAttributeResolver implements Pipe.AttributeResolver {
            private QueryContainer container;

            private QueryAttributeResolver(QueryContainer container) {
                this.container = container;
            }

            @Override
            public FieldExtraction resolve(Attribute attribute) {
                Tuple<QueryContainer, FieldExtraction> ref = container.asFieldExtraction(attribute);
                container = ref.v1();
                return ref.v2();
            }
        }
        QueryAttributeResolver resolver = new QueryAttributeResolver(this);
        proc = proc.resolveAttributes(resolver);
        QueryContainer qContainer = resolver.container;

        // update proc (if needed)
        if (qContainer.scalarFunctions().size() != scalarFunctions.size()) {
            qContainer = qContainer.withScalarProcessors(AttributeMap.builder(qContainer.scalarFunctions).put(attr, proc).build());
        }

        return new Tuple<>(qContainer, new ComputedRef(proc));
    }

    public QueryContainer addColumn(Attribute attr) {
        Expression expression = aliases.resolve(attr, attr);
        Tuple<QueryContainer, FieldExtraction> tuple = asFieldExtraction(attr);
        return tuple.v1().addColumn(tuple.v2(), Expressions.id(expression), attr);
    }

    private Tuple<QueryContainer, FieldExtraction> asFieldExtraction(Attribute attr) {
        // resolve it Expression
        Expression expression = aliases.resolve(attr, attr);

        if (expression instanceof FieldAttribute fa) {
            if (fa.isNested()) {
                return nestedHitFieldRef(fa);
            } else {
                return new Tuple<>(this, topHitFieldRef(fa));
            }
        }

        if (expression == null) {
            throw new SqlIllegalArgumentException("Unknown output attribute {}", attr);
        }

        if (expression.foldable()) {
            return new Tuple<>(this, new ComputedRef(new ConstantInput(expression.source(), expression, expression.fold())));
        }

        if (expression instanceof Score) {
            return new Tuple<>(this, new ComputedRef(new ScorePipe(expression.source(), expression)));
        }

        if (expression instanceof ScalarFunction) {
            return resolvedTreeComputingRef((ScalarFunction) expression, attr);
        }

        throw new SqlIllegalArgumentException("Unknown output attribute {}", attr);
    }

    public QueryContainer addColumn(FieldExtraction ref, String id, Attribute attribute) {
        return new QueryContainer(
            query,
            aggs,
            combine(fields, new FieldInfo(ref, id, attribute)),
            aliases,
            pseudoFunctions,
            scalarFunctions,
            sort,
            limit,
            trackHits,
            includeFrozen,
            minPageSize,
            allowPartialSearchResults
        );
    }

    public AttributeMap<Pipe> scalarFunctions() {
        return scalarFunctions;
    }

    //
    // agg methods
    //

    public QueryContainer addAgg(String groupId, LeafAgg agg) {
        return with(aggs.addAgg(agg));
    }

    public QueryContainer addGroups(Collection<GroupByKey> values) {
        return with(aggs.addGroups(values));
    }

    public GroupByKey findGroupForAgg(String aggId) {
        return aggs.findGroupForAgg(aggId);
    }

    public QueryContainer updateGroup(GroupByKey group) {
        return with(aggs.updateGroup(group));
    }

    //
    // boiler plate
    //

    @Override
    public int hashCode() {
        return Objects.hash(query, aggs, fields, aliases, sort, limit, trackHits, includeFrozen, allowPartialSearchResults);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        QueryContainer other = (QueryContainer) obj;
        return Objects.equals(query, other.query)
            && Objects.equals(aggs, other.aggs)
            && Objects.equals(fields, other.fields)
            && Objects.equals(aliases, other.aliases)
            && Objects.equals(sort, other.sort)
            && Objects.equals(limit, other.limit)
            && Objects.equals(trackHits, other.trackHits)
            && Objects.equals(includeFrozen, other.includeFrozen)
            && Objects.equals(allowPartialSearchResults, other.allowPartialSearchResults);
    }

    @Override
    public String toString() {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.humanReadable(true).prettyPrint();
            SourceGenerator.sourceBuilder(this, null, null).toXContent(builder, ToXContent.EMPTY_PARAMS);
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new RuntimeException("error rendering", e);
        }
    }
}
