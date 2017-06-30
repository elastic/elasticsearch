/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.xpack.sql.execution.search.SourceGenerator;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.NestedFieldAttribute;
import org.elasticsearch.xpack.sql.expression.RootFieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.ColumnProcessor;
import org.elasticsearch.xpack.sql.querydsl.agg.AggPath;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupingAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.LeafAgg;
import org.elasticsearch.xpack.sql.querydsl.query.AndQuery;
import org.elasticsearch.xpack.sql.querydsl.query.MatchAll;
import org.elasticsearch.xpack.sql.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.sql.querydsl.query.Query;
import org.elasticsearch.xpack.sql.util.CollectionUtils;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;

import static org.elasticsearch.xpack.sql.util.CollectionUtils.combine;

public class QueryContainer {

    private final Aggs aggs;
    private final Query query;
    private final List<Reference> refs;
    // aliases (maps an alias to its actual resolved attribute)
    private final Map<Attribute, Attribute> aliases;
    // processors for a given attribute - wraps the processor over the resolved ref
    private final Map<Attribute, ColumnProcessor> processors;
    // pseudo functions (like count) - that are 'extracted' from other aggs
    private final Map<String, GroupingAgg> pseudoFunctions;

    private final Set<Sort> sort;
    private final int limit;

    // computed
    private final boolean aggsOnly;
    private final int aggDepth;

    public QueryContainer() {
        this(null, null, null, null, null, null, null, -1);
    }

    public QueryContainer(Query query, Aggs aggs, List<Reference> refs, Map<Attribute, Attribute> aliases, Map<Attribute, ColumnProcessor> processors, Map<String, GroupingAgg> pseudoFunctions, Set<Sort> sort, int limit) {
        this.query = query;
        this.aggs = aggs == null ? new Aggs() : aggs;
        this.aliases = aliases == null || aliases.isEmpty() ? emptyMap() : aliases;
        this.processors = processors == null || processors.isEmpty() ? emptyMap() : processors;
        this.pseudoFunctions = pseudoFunctions == null || pseudoFunctions.isEmpty() ? emptyMap() : pseudoFunctions;
        this.refs = refs == null || refs.isEmpty() ? emptyList() : refs;
        this.sort = sort == null || sort.isEmpty() ? emptySet() : sort;
        this.limit = limit;

        int aggLevel = 0;
        boolean onlyAggs = true;

        for (Reference ref : this.refs) {
            if (ref.depth() > aggLevel) {
                aggLevel = ref.depth();
            }
            while (ref instanceof ProcessingRef) {
                ProcessingRef r = (ProcessingRef) ref;
                ref = r.ref();
            }
            if (ref instanceof FieldReference) {
                onlyAggs = false;
            }
        }
        aggsOnly = onlyAggs;
        aggDepth = aggLevel;
    }

    public Query query() {
        return query;
    }

    public Aggs aggs() {
        return aggs;
    }

    public List<Reference> refs() {
        return refs;
    }

    public Map<Attribute, Attribute> aliases() {
        return aliases;
    }

    public Map<Attribute, ColumnProcessor> processors() {
        return processors;
    }
    
    public Map<String, GroupingAgg> pseudoFunctions() {
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

    public int aggDepth() {
        return aggDepth;
    }

    public boolean hasReferences() {
        return !refs.isEmpty();
    }

    //
    // copy methods
    //

    public QueryContainer with(Query q) {
        return new QueryContainer(q, aggs, refs, aliases, processors, pseudoFunctions, sort, limit);
    }

    public QueryContainer with(List<Reference> r) {
        return new QueryContainer(query, aggs, r, aliases, processors, pseudoFunctions, sort, limit);
    }

    public QueryContainer withAliases(Map<Attribute, Attribute> a) {
        return new QueryContainer(query, aggs, refs, a, processors, pseudoFunctions, sort, limit);
    }

    public QueryContainer withProcessors(Map<Attribute, ColumnProcessor> p) {
        return new QueryContainer(query, aggs, refs, aliases, p, pseudoFunctions, sort, limit);
    }

    public QueryContainer withPseudoFunctions(Map<String, GroupingAgg> p) {
        return new QueryContainer(query, aggs, refs, aliases, processors, p, sort, limit);
    }

    public QueryContainer with(Aggs a) {
        return new QueryContainer(query, a, refs, aliases, processors, pseudoFunctions, sort, limit);
    }

    public QueryContainer withLimit(int l) {
        return l == limit ? this : new QueryContainer(query, aggs, refs, aliases, processors, pseudoFunctions, sort, l);
    }


    public QueryContainer sort(Sort sortable) {
        Set<Sort> sort = new LinkedHashSet<>(this.sort);
        sort.add(sortable);
        return new QueryContainer(query, aggs, refs, aliases, processors, pseudoFunctions, sort, limit);
    }
    
    private String aliasName(Attribute attr) {
        return aliases.getOrDefault(attr, attr).name();
    }

    private Reference wrapProcessorIfNeeded(Attribute attr, Reference ref) {
        ColumnProcessor columnProcessor = processors.get(attr);
        return columnProcessor != null ? new ProcessingRef(columnProcessor, ref) : ref;
    }

    //
    // reference methods
    //
    public QueryContainer addFieldRef(RootFieldAttribute fieldAttr) {
        SearchHitFieldRef fieldRef = new SearchHitFieldRef(aliasName(fieldAttr), shouldUseDocValue(fieldAttr));
        return addRef(wrapProcessorIfNeeded(fieldAttr, fieldRef));
    }

    private boolean shouldUseDocValue(FieldAttribute fieldAttr) {
        // TODO: configurable retrieval format for dates
        // && !(fieldAttr.dataType() instanceof DateType)
        return fieldAttr.dataType().hasDocValues();
    }

    public QueryContainer addNestedFieldRef(NestedFieldAttribute attr) {
        // attach the field to the relevant nested query
        List<Reference> nestedRefs = new ArrayList<>();

        String parent = attr.parentPath();
        String name = aliasName(attr);
        
        Query q = query;
        Map<String, Boolean> field = singletonMap(name, Boolean.valueOf(shouldUseDocValue(attr)));
        if (q == null) {
            q = new NestedQuery(attr.location(), parent, field, new MatchAll(attr.location()));
        }
        else {
            AtomicBoolean foundMatch = new AtomicBoolean(false);
            q = q.transformDown(n -> {
                if (parent.equals(n.path())) {
                    if (!n.fields().keySet().contains(name)) {
                        foundMatch.set(true);
                        return new NestedQuery(n.location(), n.path(), combine(n.fields(), field), n.child());
                    }
                }
                return n;
            }, NestedQuery.class);

            // no nested query exists for the given field, add one to retrieve its content
            if (!foundMatch.get()) {
                NestedQuery nested = new NestedQuery(attr.location(), parent, field, new MatchAll(attr.location()));
                q = new AndQuery(attr.location(), q, nested);
            }
        }
        
        NestedFieldRef nestedFieldRef = new NestedFieldRef(attr.parentPath(), attr.name(), shouldUseDocValue(attr));
        
        nestedRefs.add(wrapProcessorIfNeeded(attr, nestedFieldRef));

        return new QueryContainer(q, aggs, combine(refs, nestedRefs), aliases, processors, pseudoFunctions, sort, limit);
    }

    private QueryContainer addRef(Reference ref) {
        return with(combine(refs, ref));
    }
    
    //
    // agg methods
    //
    public QueryContainer addAggRef(String aggPath) {
        return addAggRef(aggPath, null);
    }

    public QueryContainer addAggRef(String aggPath, ColumnProcessor processor) {
        return addAggRef(new AggRef(aggPath), processor);
    }

    public QueryContainer addAggRef(AggRef customRef, ColumnProcessor processor) {
        Reference ref = processor != null ? new ProcessingRef(processor, customRef) : customRef;
        return addRef(ref);
    }

    public QueryContainer addAggCount(GroupingAgg parentGroup, String functionId, ColumnProcessor processor) {
        Reference ref = parentGroup == null ? TotalCountRef.INSTANCE : new AggRef(AggPath.bucketCount(parentGroup.asParentPath()));
        ref = processor != null ? new ProcessingRef(processor, ref) : ref;
        return new QueryContainer(query, aggs, combine(refs, ref), aliases, processors, combine(pseudoFunctions, CollectionUtils.of(functionId, parentGroup)), sort, limit);
    }

    public QueryContainer addAgg(String groupId, LeafAgg agg, ColumnProcessor processor) {
        return addAgg(groupId, agg, agg.propertyPath(), processor);
    }

    public QueryContainer addAgg(String groupId, LeafAgg agg, String aggRefPath, ColumnProcessor processor) {
        AggRef aggRef = new AggRef(aggRefPath);
        Reference ref = processor != null ? new ProcessingRef(processor, aggRef) : aggRef;
        return new QueryContainer(query, aggs.addAgg(groupId, agg), combine(refs, ref), aliases, processors, pseudoFunctions, sort, limit);
    }

    public QueryContainer addGroups(Collection<GroupingAgg> values) {
        return with(aggs.addGroups(values));
    }

    public QueryContainer updateGroup(GroupingAgg group) {
        return with(aggs.updateGroup(group));
    }

    public GroupingAgg findGroupForAgg(String aggId) {
        return aggs.findGroupForAgg(aggId);
    }

    //
    // boiler plate
    //

    @Override
    public int hashCode() {
        return Objects.hash(query, aggs, refs, aliases);
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
                && Objects.equals(refs, other.refs)
                && Objects.equals(aliases, other.aliases)
                && Objects.equals(sort, other.sort)
                && Objects.equals(limit, other.limit);
    }

    @Override
    public String toString() {
        return SourceGenerator.sourceBuilder(this).toString();
    }
}