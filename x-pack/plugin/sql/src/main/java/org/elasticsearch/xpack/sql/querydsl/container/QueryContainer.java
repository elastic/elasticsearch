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
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.sql.execution.search.SourceGenerator;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.AttributeMap;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.LiteralAttribute;
import org.elasticsearch.xpack.sql.expression.function.ScoreAttribute;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByKey;
import org.elasticsearch.xpack.sql.querydsl.agg.LeafAgg;
import org.elasticsearch.xpack.sql.querydsl.query.BoolQuery;
import org.elasticsearch.xpack.sql.querydsl.query.MatchAll;
import org.elasticsearch.xpack.sql.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.sql.querydsl.query.Query;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
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

/**
 * Container for various references of the built ES query.
 * Useful to understanding how to interpret and navigate the
 * returned result.
 */
public class QueryContainer {

    private final Aggs aggs;
    private final Query query;

    // fields extracted from the response - not necessarily what the client sees
    // for example in case of grouping or custom sorting, the response has extra columns
    // that is filtered before getting to the client

    // the list contains both the field extraction and the id of its associated attribute (for custom sorting)
    private final List<Tuple<FieldExtraction, ExpressionId>> fields;

    // aliases (maps an alias to its actual resolved attribute)
    private final AttributeMap<Attribute> aliases;

    // pseudo functions (like count) - that are 'extracted' from other aggs
    private final Map<String, GroupByKey> pseudoFunctions;

    // scalar function processors - recorded as functions get folded;
    // at scrolling, their inputs (leaves) get updated
    private final AttributeMap<Pipe> scalarFunctions;

    private final Set<Sort> sort;
    private final int limit;
    private final boolean trackHits;
    private final boolean includeFrozen;

    // computed
    private Boolean aggsOnly;
    private Boolean customSort;

    public QueryContainer() {
        this(null, null, null, null, null, null, null, -1, false, false);
    }

    public QueryContainer(Query query,
            Aggs aggs,
            List<Tuple<FieldExtraction,
            ExpressionId>> fields,
            AttributeMap<Attribute> aliases,
            Map<String, GroupByKey> pseudoFunctions,
            AttributeMap<Pipe> scalarFunctions,
            Set<Sort> sort,
            int limit,
            boolean trackHits,
            boolean includeFrozen) {
        this.query = query;
        this.aggs = aggs == null ? Aggs.EMPTY : aggs;
        this.fields = fields == null || fields.isEmpty() ? emptyList() : fields;
        this.aliases = aliases == null || aliases.isEmpty() ? AttributeMap.emptyAttributeMap() : aliases;
        this.pseudoFunctions = pseudoFunctions == null || pseudoFunctions.isEmpty() ? emptyMap() : pseudoFunctions;
        this.scalarFunctions = scalarFunctions == null || scalarFunctions.isEmpty() ? AttributeMap.emptyAttributeMap() : scalarFunctions;
        this.sort = sort == null || sort.isEmpty() ? emptySet() : sort;
        this.limit = limit;
        this.trackHits = trackHits;
        this.includeFrozen = includeFrozen;
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

        List<Tuple<Integer, Comparator>> sortingColumns = new ArrayList<>(sort.size());

        boolean aggSort = false;
        for (Sort s : sort) {
            Tuple<Integer, Comparator> tuple = new Tuple<>(Integer.valueOf(-1), null);
            
            if (s instanceof AttributeSort) {
                AttributeSort as = (AttributeSort) s;
                // find the relevant column of each aggregate function
                if (as.attribute() instanceof AggregateFunctionAttribute) {
                    aggSort = true;
                    AggregateFunctionAttribute afa = (AggregateFunctionAttribute) as.attribute();
                    afa = (AggregateFunctionAttribute) aliases.getOrDefault(afa, afa);
                    int atIndex = -1;
                    for (int i = 0; i < fields.size(); i++) {
                        Tuple<FieldExtraction, ExpressionId> field = fields.get(i);
                        if (field.v2().equals(afa.innerId())) {
                            atIndex = i;
                            break;
                        }
                    }

                    if (atIndex == -1) {
                        throw new SqlIllegalArgumentException("Cannot find backing column for ordering aggregation [{}]", afa.name());
                    }
                    // assemble a comparator for it
                    Comparator comp = s.direction() == Sort.Direction.ASC ? Comparator.naturalOrder() : Comparator.reverseOrder();
                    comp = s.missing() == Sort.Missing.FIRST ? Comparator.nullsFirst(comp) : Comparator.nullsLast(comp);

                    tuple = new Tuple<>(Integer.valueOf(atIndex), comp);
                }
            }
            sortingColumns.add(tuple);
        }
        
        if (customSort == null) {
            customSort = Boolean.valueOf(aggSort);
        }

        return aggSort ? sortingColumns : emptyList();
    }

    /**
     * Since the container contains both the field extractors and the visible columns,
     * compact the information in the listener through a bitset that acts as a mask
     * on what extractors are used for the visible columns.
     */
    public BitSet columnMask(List<Attribute> columns) {
        BitSet mask = new BitSet(fields.size());
        for (Attribute column : columns) {
            Attribute alias = aliases.get(column);
            // find the column index
            int index = -1;
            ExpressionId id = column instanceof AggregateFunctionAttribute ? ((AggregateFunctionAttribute) column).innerId() : column.id();
            ExpressionId aliasId = alias != null ? (alias instanceof AggregateFunctionAttribute ? ((AggregateFunctionAttribute) alias)
                    .innerId() : alias.id()) : null;
            for (int i = 0; i < fields.size(); i++) {
                Tuple<FieldExtraction, ExpressionId> tuple = fields.get(i);
                if (tuple.v2().equals(id) || (aliasId != null && tuple.v2().equals(aliasId))) {
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

    public List<Tuple<FieldExtraction, ExpressionId>> fields() {
        return fields;
    }

    public AttributeMap<Attribute> aliases() {
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
        if (aggsOnly == null) {
            aggsOnly = Boolean.valueOf(this.fields.stream().allMatch(t -> t.v1().supportedByAggsOnlyQuery()));
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

    //
    // copy methods
    //

    public QueryContainer with(Query q) {
        return new QueryContainer(q, aggs, fields, aliases, pseudoFunctions, scalarFunctions, sort, limit, trackHits, includeFrozen);
    }

    public QueryContainer withAliases(AttributeMap<Attribute> a) {
        return new QueryContainer(query, aggs, fields, a, pseudoFunctions, scalarFunctions, sort, limit, trackHits, includeFrozen);
    }

    public QueryContainer withPseudoFunctions(Map<String, GroupByKey> p) {
        return new QueryContainer(query, aggs, fields, aliases, p, scalarFunctions, sort, limit, trackHits, includeFrozen);
    }

    public QueryContainer with(Aggs a) {
        return new QueryContainer(query, a, fields, aliases, pseudoFunctions, scalarFunctions, sort, limit, trackHits, includeFrozen);
    }

    public QueryContainer withLimit(int l) {
        return l == limit ? this : new QueryContainer(query, aggs, fields, aliases, pseudoFunctions, scalarFunctions, sort, l, trackHits,
                includeFrozen);
    }

    public QueryContainer withTrackHits() {
        return trackHits ? this : new QueryContainer(query, aggs, fields, aliases, pseudoFunctions, scalarFunctions, sort, limit, true,
                includeFrozen);
    }

    public QueryContainer withFrozen() {
        return includeFrozen ? this : new QueryContainer(query, aggs, fields, aliases, pseudoFunctions, scalarFunctions, sort, limit,
                trackHits, true);
    }

    public QueryContainer withScalarProcessors(AttributeMap<Pipe> procs) {
        return new QueryContainer(query, aggs, fields, aliases, pseudoFunctions, procs, sort, limit, trackHits, includeFrozen);
    }

    public QueryContainer addSort(Sort sortable) {
        Set<Sort> sort = new LinkedHashSet<>(this.sort);
        sort.add(sortable);
        return new QueryContainer(query, aggs, fields, aliases, pseudoFunctions, scalarFunctions, sort, limit, trackHits, includeFrozen);
    }

    private String aliasName(Attribute attr) {
        return aliases.getOrDefault(attr, attr).name();
    }

    //
    // reference methods
    //
    private FieldExtraction topHitFieldRef(FieldAttribute fieldAttr) {
        FieldAttribute actualField = fieldAttr;
        FieldAttribute rootField = fieldAttr;
        StringBuilder fullFieldName = new StringBuilder(fieldAttr.field().getName());
        
        // Only if the field is not an alias (in which case it will be taken out from docvalue_fields if it's isAggregatable()),
        // go up the tree of parents until a non-object (and non-nested) type of field is found and use that specific parent
        // as the field to extract data from, from _source. We do it like this because sub-fields are not in the _source, only
        // the root field to which those sub-fields belong to, are. Instead of "text_field.keyword_subfield" for _source extraction,
        // we use "text_field", because there is no source for "keyword_subfield".
        /*
         *    "text_field": {
         *       "type": "text",
         *       "fields": {
         *         "keyword_subfield": {
         *           "type": "keyword"
         *         }
         *       }
         *     }
         */
        if (fieldAttr.field().isAlias() == false) {
            while (actualField.parent() != null
                    && actualField.parent().field().getDataType() != DataType.OBJECT
                    && actualField.parent().field().getDataType() != DataType.NESTED
                    && actualField.field().getDataType().isFromDocValuesOnly() == false) {
                actualField = actualField.parent();
            }
        }
        while (rootField.parent() != null) {
            fullFieldName.insert(0, ".").insert(0, rootField.parent().field().getName());
            rootField = rootField.parent();
        }
        return new SearchHitFieldRef(aliasName(actualField), fullFieldName.toString(), fieldAttr.field().getDataType(),
                fieldAttr.field().isAggregatable(), fieldAttr.field().isAlias());
    }

    private Tuple<QueryContainer, FieldExtraction> nestedHitFieldRef(FieldAttribute attr) {
        String name = aliasName(attr);
        Query q = rewriteToContainNestedField(query, attr.source(),
                attr.nestedParent().name(), name, attr.field().getDataType().format(), attr.field().getDataType().isFromDocValuesOnly());

        SearchHitFieldRef nestedFieldRef = new SearchHitFieldRef(name, null, attr.field().getDataType(), attr.field().isAggregatable(),
                false, attr.parent().name());

        return new Tuple<>(
                new QueryContainer(q, aggs, fields, aliases, pseudoFunctions, scalarFunctions, sort, limit, trackHits, includeFrozen),
                nestedFieldRef);
    }

    static Query rewriteToContainNestedField(@Nullable Query query, Source source, String path, String name, String format,
            boolean hasDocValues) {
        if (query == null) {
            /* There is no query so we must add the nested query
             * ourselves to fetch the field. */
            return new NestedQuery(source, path, singletonMap(name, new AbstractMap.SimpleImmutableEntry<>(hasDocValues, format)),
                    new MatchAll(source));
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
        NestedQuery nested = new NestedQuery(source, path,
                singletonMap(name, new AbstractMap.SimpleImmutableEntry<>(hasDocValues, format)), new MatchAll(source));
        return new BoolQuery(source, true, query, nested);
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
        qContainer = qContainer.withScalarProcessors(new AttributeMap<>(procs));
        return new Tuple<>(qContainer, new ComputedRef(proc));
    }

    public QueryContainer addColumn(Attribute attr) {
        Tuple<QueryContainer, FieldExtraction> tuple = toReference(attr);
        return tuple.v1().addColumn(tuple.v2(), attr);
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

    public QueryContainer addColumn(FieldExtraction ref, Attribute attr) {
        ExpressionId id = attr instanceof AggregateFunctionAttribute ? ((AggregateFunctionAttribute) attr).innerId() : attr.id();
        return new QueryContainer(query, aggs, combine(fields, new Tuple<>(ref, id)), aliases, pseudoFunctions,
                scalarFunctions,
                sort, limit, trackHits, includeFrozen);
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
        return Objects.hash(query, aggs, fields, aliases, sort, limit, trackHits, includeFrozen);
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
                && Objects.equals(includeFrozen, other.includeFrozen);
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