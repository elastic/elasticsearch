/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.sql.execution.search.SourceGenerator;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.LiteralAttribute;
import org.elasticsearch.xpack.sql.expression.function.ScoreAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByKey;
import org.elasticsearch.xpack.sql.querydsl.agg.LeafAgg;
import org.elasticsearch.xpack.sql.querydsl.container.GroupByRef.Property;
import org.elasticsearch.xpack.sql.querydsl.query.BoolQuery;
import org.elasticsearch.xpack.sql.querydsl.query.MatchAll;
import org.elasticsearch.xpack.sql.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.sql.querydsl.query.Query;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.sql.util.CollectionUtils.combine;

public class QueryContainer {

    private final Aggs aggs;
    private final Query query;

    // final output seen by the client (hence the list or ordering)
    // gets converted by the Scroller into Extractors for hits or actual results in case of aggregations
    private final List<FieldExtraction> columns;

    // aliases (maps an alias to its actual resolved attribute)
    private final Map<Attribute, Attribute> aliases;

    // pseudo functions (like count) - that are 'extracted' from other aggs
    private final Map<String, GroupByKey> pseudoFunctions;

    // scalar function processors - recorded as functions get folded;
    // at scrolling, their inputs (leaves) get updated
    private final Map<Attribute, Pipe> scalarFunctions;

    private final Set<Sort> sort;
    private final int limit;

    // computed
    private final boolean aggsOnly;

    public QueryContainer() {
        this(null, null, null, null, null, null, null, -1);
    }

    public QueryContainer(Query query, Aggs aggs, List<FieldExtraction> refs, Map<Attribute, Attribute> aliases,
            Map<String, GroupByKey> pseudoFunctions,
            Map<Attribute, Pipe> scalarFunctions,
            Set<Sort> sort, int limit) {
        this.query = query;
        this.aggs = aggs == null ? new Aggs() : aggs;
        this.aliases = aliases == null || aliases.isEmpty() ? emptyMap() : aliases;
        this.pseudoFunctions = pseudoFunctions == null || pseudoFunctions.isEmpty() ? emptyMap() : pseudoFunctions;
        this.scalarFunctions = scalarFunctions == null || scalarFunctions.isEmpty() ? emptyMap() : scalarFunctions;
        this.columns = refs == null || refs.isEmpty() ? emptyList() : refs;
        this.sort = sort == null || sort.isEmpty() ? emptySet() : sort;
        this.limit = limit;
        aggsOnly = columns.stream().allMatch(FieldExtraction::supportedByAggsOnlyQuery);
    }

    public Query query() {
        return query;
    }

    public Aggs aggs() {
        return aggs;
    }

    public List<FieldExtraction> columns() {
        return columns;
    }

    public Map<Attribute, Attribute> aliases() {
        return aliases;
    }

    public Map<String, GroupByKey> pseudoFunctions() {
        return pseudoFunctions;
    }

    public Set<Sort> sort() {
        return sort;
    }

    public int limit() {
        return limit;
    }

    public boolean isAggsOnly() {
        return aggsOnly;
    }

    public boolean hasColumns() {
        return !columns.isEmpty();
    }

    //
    // copy methods
    //

    public QueryContainer with(Query q) {
        return new QueryContainer(q, aggs, columns, aliases, pseudoFunctions, scalarFunctions, sort, limit);
    }

    public QueryContainer with(List<FieldExtraction> r) {
        return new QueryContainer(query, aggs, r, aliases, pseudoFunctions, scalarFunctions, sort, limit);
    }

    public QueryContainer withAliases(Map<Attribute, Attribute> a) {
        return new QueryContainer(query, aggs, columns, a, pseudoFunctions, scalarFunctions, sort, limit);
    }

    public QueryContainer withPseudoFunctions(Map<String, GroupByKey> p) {
        return new QueryContainer(query, aggs, columns, aliases, p, scalarFunctions, sort, limit);
    }

    public QueryContainer with(Aggs a) {
        return new QueryContainer(query, a, columns, aliases, pseudoFunctions, scalarFunctions, sort, limit);
    }

    public QueryContainer withLimit(int l) {
        return l == limit ? this : new QueryContainer(query, aggs, columns, aliases, pseudoFunctions, scalarFunctions, sort, l);
    }

    public QueryContainer withScalarProcessors(Map<Attribute, Pipe> procs) {
        return new QueryContainer(query, aggs, columns, aliases, pseudoFunctions, procs, sort, limit);
    }

    public QueryContainer sort(Sort sortable) {
        Set<Sort> sort = new LinkedHashSet<>(this.sort);
        sort.add(sortable);
        return new QueryContainer(query, aggs, columns, aliases, pseudoFunctions, scalarFunctions, sort, limit);
    }

    private String aliasName(Attribute attr) {
        return aliases.getOrDefault(attr, attr).name();
    }

    //
    // reference methods
    //
    private FieldExtraction topHitFieldRef(FieldAttribute fieldAttr) {
        return new SearchHitFieldRef(aliasName(fieldAttr), fieldAttr.field().getDataType(), fieldAttr.field().isAggregatable());
    }

    private Tuple<QueryContainer, FieldExtraction> nestedHitFieldRef(FieldAttribute attr) {
        // Find the nested query for this field. If there isn't one then create it
        List<FieldExtraction> nestedRefs = new ArrayList<>();

        String name = aliasName(attr);
        String format = attr.field().getDataType() == DataType.DATE ? "epoch_millis" : DocValueFieldsContext.USE_DEFAULT_FORMAT;
        Query q = rewriteToContainNestedField(query, attr.location(),
                attr.nestedParent().name(), name, format, attr.field().isAggregatable());

        SearchHitFieldRef nestedFieldRef = new SearchHitFieldRef(name, attr.field().getDataType(),
                attr.field().isAggregatable(), attr.parent().name());
        nestedRefs.add(nestedFieldRef);

        return new Tuple<>(new QueryContainer(q, aggs, columns, aliases, pseudoFunctions, scalarFunctions, sort, limit), nestedFieldRef);
    }

    static Query rewriteToContainNestedField(@Nullable Query query, Location location, String path, String name, String format,
            boolean hasDocValues) {
        if (query == null) {
            /* There is no query so we must add the nested query
             * ourselves to fetch the field. */
            return new NestedQuery(location, path, singletonMap(name, new AbstractMap.SimpleImmutableEntry<>(hasDocValues, format)),
                    new MatchAll(location));
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
        NestedQuery nested = new NestedQuery(location, path,
                singletonMap(name, new AbstractMap.SimpleImmutableEntry<>(hasDocValues, format)), new MatchAll(location));
        return new BoolQuery(location, true, query, nested);
    }

    // replace function/operators's input with references
    private Tuple<QueryContainer, FieldExtraction> resolvedTreeComputingRef(ScalarFunctionAttribute ta) {
        Attribute attribute = aliases.getOrDefault(ta, ta);
        Pipe proc = scalarFunctions.get(attribute);

        // check the attribute itself
        if (proc == null) {
            if (attribute instanceof ScalarFunctionAttribute) {
                ta = (ScalarFunctionAttribute) attribute;
            }
            proc = ta.asPipe();
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
                Attribute attr = aliases.getOrDefault(attribute, attribute);
                Tuple<QueryContainer, FieldExtraction> ref = container.toReference(attr);
                container = ref.v1();
                return ref.v2();
            }
        }
        QueryAttributeResolver resolver = new QueryAttributeResolver(this);
        proc = proc.resolveAttributes(resolver);
        QueryContainer qContainer = resolver.container;

        // update proc
        Map<Attribute, Pipe> procs = new LinkedHashMap<>(qContainer.scalarFunctions());
        procs.put(attribute, proc);
        qContainer = qContainer.withScalarProcessors(procs);
        return new Tuple<>(qContainer, new ComputedRef(proc));
    }

    public QueryContainer addColumn(Attribute attr) {
        Tuple<QueryContainer, FieldExtraction> tuple = toReference(attr);
        return tuple.v1().addColumn(tuple.v2());
    }

    private Tuple<QueryContainer, FieldExtraction> toReference(Attribute attr) {
        if (attr instanceof FieldAttribute) {
            FieldAttribute fa = (FieldAttribute) attr;
            if (fa.isNested()) {
                return nestedHitFieldRef(fa);
            } else {
                return new Tuple<>(this, topHitFieldRef(fa));
            }
        }
        if (attr instanceof ScalarFunctionAttribute) {
            return resolvedTreeComputingRef((ScalarFunctionAttribute) attr);
        }
        if (attr instanceof LiteralAttribute) {
            return new Tuple<>(this, new ComputedRef(((LiteralAttribute) attr).asPipe()));
        }
        if (attr instanceof ScoreAttribute) {
            return new Tuple<>(this, new ComputedRef(((ScoreAttribute) attr).asPipe()));
        }

        throw new SqlIllegalArgumentException("Unknown output attribute {}", attr);
    }

    public QueryContainer addColumn(FieldExtraction ref) {
        return with(combine(columns, ref));
    }

    public Map<Attribute, Pipe> scalarFunctions() {
        return scalarFunctions;
    }

    //
    // agg methods
    //

    public QueryContainer addAggCount(GroupByKey group, String functionId) {
        FieldExtraction ref = group == null ? GlobalCountRef.INSTANCE : new GroupByRef(group.id(), Property.COUNT, null);
        Map<String, GroupByKey> pseudoFunctions = new LinkedHashMap<>(this.pseudoFunctions);
        pseudoFunctions.put(functionId, group);
        return new QueryContainer(query, aggs, combine(columns, ref), aliases, pseudoFunctions, scalarFunctions, sort, limit);
    }

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
        return Objects.hash(query, aggs, columns, aliases);
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
                && Objects.equals(columns, other.columns)
                && Objects.equals(aliases, other.aliases)
                && Objects.equals(sort, other.sort)
                && Objects.equals(limit, other.limit);
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