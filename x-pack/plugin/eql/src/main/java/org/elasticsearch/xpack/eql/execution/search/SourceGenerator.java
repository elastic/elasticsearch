/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder.ScriptSortType;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xpack.eql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.querydsl.container.AttributeSort;
import org.elasticsearch.xpack.ql.querydsl.container.ScriptSort;
import org.elasticsearch.xpack.ql.querydsl.container.Sort;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;
import static org.elasticsearch.search.sort.SortBuilders.scriptSort;

public abstract class SourceGenerator {

    private SourceGenerator() {}

    public static SearchSourceBuilder sourceBuilder(
        QueryContainer container,
        QueryBuilder filter,
        List<FieldAndFormat> fetchFields,
        Map<String, Object> runtimeMappings
    ) {
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

        // extract fields
        QlSourceBuilder sourceBuilder = new QlSourceBuilder();
        // Iterate through all the columns requested, collecting the fields that
        // need to be retrieved from the result documents

        // NB: the sortBuilder takes care of eliminating duplicates
        container.fields().forEach(f -> f.v1().collectFields(sourceBuilder));
        sourceBuilder.build(source);

        sorting(container, source);

        // disable the source, as we rely on "fields" API
        source.fetchSource(false);

        // add the "fields" to be fetched
        if (fetchFields != null) {
            fetchFields.forEach(source::fetchField);
        }

        // add the runtime fields
        if (runtimeMappings != null) {
            source.runtimeMappings(runtimeMappings);
        }

        if (container.limit() != null) {
            // add size and from
            source.size(container.limit().absLimit());
            // this should be added only for event queries
            if (container.limit().offset() > 0) {
                source.from(container.limit().offset());
            }
        }

        optimize(container, source);

        return source;
    }

    private static void sorting(QueryContainer container, SearchSourceBuilder source) {
        for (Sort sortable : container.sort().values()) {
            SortBuilder<?> sortBuilder = null;

            if (sortable instanceof AttributeSort as) {
                Attribute attr = as.attribute();

                // sorting only works on not-analyzed fields - look for a multi-field replacement
                if (attr instanceof FieldAttribute fieldAttribute) {
                    FieldAttribute fa = fieldAttribute.exactAttribute();

                    sortBuilder = fieldSort(fa.name()).missing(as.missing().searchOrder(as.direction()))
                        .unmappedType(fa.dataType().esType());

                    if (fa.isNested()) {
                        FieldSortBuilder fieldSort = fieldSort(fa.name()).missing(as.missing().searchOrder(as.direction()))
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
            } else if (sortable instanceof ScriptSort ss) {
                sortBuilder = scriptSort(
                    ss.script().toPainless(),
                    ss.script().outputType().isNumeric() ? ScriptSortType.NUMBER : ScriptSortType.STRING
                );
            }

            if (sortBuilder != null) {
                sortBuilder.order(sortable.direction().asOrder());
                source.sort(sortBuilder);
            }
        }
    }

    private static void optimize(QueryContainer query, SearchSourceBuilder builder) {
        builder.trackTotalHits(query.shouldTrackHits());
    }
}
