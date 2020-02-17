/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder.ScriptSortType;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.querydsl.container.AttributeSort;
import org.elasticsearch.xpack.sql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.sql.querydsl.container.ScoreSort;
import org.elasticsearch.xpack.sql.querydsl.container.ScriptSort;
import org.elasticsearch.xpack.sql.querydsl.container.Sort;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;
import static org.elasticsearch.search.sort.SortBuilders.scoreSort;
import static org.elasticsearch.search.sort.SortBuilders.scriptSort;

public abstract class SourceGenerator {

    private SourceGenerator() {}

    private static final List<String> NO_STORED_FIELD = singletonList(StoredFieldsContext._NONE_);

    public static SearchSourceBuilder sourceBuilder(QueryContainer container, QueryBuilder filter, Integer size) {
        QueryBuilder finalQuery = null;
        // add the source
        if (container.query() != null) {
            if (filter != null) {
                finalQuery = boolQuery().must(container.query().asBuilder()).filter(filter);
            } else {
                finalQuery = container.query().asBuilder();
            }
        } else {
            if (filter != null) {
                finalQuery = boolQuery().filter(filter);
            }
        }

        final SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(finalQuery);

        QlSourceBuilder sortBuilder = new QlSourceBuilder();
        // Iterate through all the columns requested, collecting the fields that
        // need to be retrieved from the result documents

        // NB: the sortBuilder takes care of eliminating duplicates
        container.fields().forEach(f -> f.v1().collectFields(sortBuilder));
        sortBuilder.build(source);
        optimize(sortBuilder, source);

        // add the aggs (if present)
        AggregationBuilder aggBuilder = container.aggs().asAggBuilder();

        if (aggBuilder != null) {
            source.aggregation(aggBuilder);
        }

        sorting(container, source);

        // set page size
        if (size != null) {
            int sz = container.limit() > 0 ? Math.min(container.limit(), size) : size;
            // now take into account the the minimum page (if set)
            // that is, return the multiple of the minimum page size closer to the set size
            int minSize = container.minPageSize();
            sz = minSize > 0 ? (Math.max(sz / minSize, 1) * minSize) : sz;

            if (source.size() == -1) {
                source.size(sz);
            }
            if (aggBuilder instanceof CompositeAggregationBuilder) {
                // limit the composite aggs only for non-local sorting
                if (container.sortingColumns().isEmpty()) {
                    ((CompositeAggregationBuilder) aggBuilder).size(sz);
                } else {
                    ((CompositeAggregationBuilder) aggBuilder).size(size);
                }
            }
        }

        optimize(container, source);

        return source;
    }

    private static void sorting(QueryContainer container, SearchSourceBuilder source) {
        if (source.aggregations() != null && source.aggregations().count() > 0) {
            // Aggs can't be sorted using search sorting. That sorting is handled elsewhere.
            return;
        }
        if (container.sort() == null || container.sort().isEmpty()) {
            // if no sorting is specified, use the _doc one
            source.sort("_doc");
            return;
        }
        for (Sort sortable : container.sort()) {
            SortBuilder<?> sortBuilder = null;

            if (sortable instanceof AttributeSort) {
                AttributeSort as = (AttributeSort) sortable;
                Attribute attr = as.attribute();

                // sorting only works on not-analyzed fields - look for a multi-field replacement
                if (attr instanceof FieldAttribute) {
                    FieldAttribute fa = ((FieldAttribute) attr).exactAttribute();

                    sortBuilder = fieldSort(fa.name())
                            .missing(as.missing().position())
                            .unmappedType(fa.dataType().esType());
                    
                    if (fa.isNested()) {
                        FieldSortBuilder fieldSort = fieldSort(fa.name())
                                .missing(as.missing().position())
                                .unmappedType(fa.dataType().esType());

                        NestedSortBuilder newSort = new NestedSortBuilder(fa.nestedParent().name());
                        NestedSortBuilder nestedSort = fieldSort.getNestedSort();

                        if (nestedSort == null) {
                            fieldSort.setNestedSort(newSort);
                        } else {
                            while (nestedSort.getNestedSort() != null) {
                                nestedSort = nestedSort.getNestedSort();
                            }
                            nestedSort.setNestedSort(newSort);
                        }

                        nestedSort = newSort;

                        if (container.query() != null) {
                            container.query().enrichNestedSort(nestedSort);
                        }
                        sortBuilder = fieldSort;
                    }
                }
            } else if (sortable instanceof ScriptSort) {
                ScriptSort ss = (ScriptSort) sortable;
                sortBuilder = scriptSort(ss.script().toPainless(),
                        ss.script().outputType().isNumeric() ? ScriptSortType.NUMBER : ScriptSortType.STRING);
            } else if (sortable instanceof ScoreSort) {
                sortBuilder = scoreSort();
            }

            if (sortBuilder != null) {
                sortBuilder.order(sortable.direction().asOrder());
                source.sort(sortBuilder);
            }
        }
    }

    private static void optimize(QlSourceBuilder sqlSource, SearchSourceBuilder builder) {
        if (sqlSource.noSource()) {
            disableSource(builder);
        }
    }

    private static void optimize(QueryContainer query, SearchSourceBuilder builder) {
        // if only aggs are needed, don't retrieve any docs and remove scoring
        if (query.isAggsOnly()) {
            builder.size(0);
            builder.trackScores(false);
            // disable source fetching (only doc values are used)
            disableSource(builder);
        }
        if (query.shouldTrackHits()) {
            builder.trackTotalHits(true);
        }
    }

    private static void disableSource(SearchSourceBuilder builder) {
        builder.fetchSource(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        if (builder.storedFields() == null) {
            builder.storedFields(NO_STORED_FIELD);
        }
    }
}
