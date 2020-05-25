/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.querydsl.container;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.execution.search.SourceGenerator;
import org.elasticsearch.xpack.ql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeMap;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.ConstantInput;
import org.elasticsearch.xpack.ql.querydsl.container.Sort;
import org.elasticsearch.xpack.ql.querydsl.query.Query;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.ql.util.CollectionUtils.combine;

public class QueryContainer {

    private final FieldExtractorRegistry extractorRegistry = new FieldExtractorRegistry();
    private final Query query;
    // attributes found in the tree
    private final AttributeMap<Expression> attributes;
    // list of fields available in the output
    private final List<Tuple<FieldExtraction, String>> fields;

    private final Map<String, Sort> sort;
    private final boolean trackHits;
    private final boolean includeFrozen;

    public QueryContainer() {
        this(null, emptyList(), AttributeMap.emptyAttributeMap(), emptyMap(), false, false);
    }

    private QueryContainer(Query query, List<Tuple<FieldExtraction, String>> fields, AttributeMap<Expression> attributes,
                           Map<String, Sort> sort, boolean trackHits, boolean includeFrozen) {
        this.query = query;
        this.fields = fields;
        this.sort = sort;
        this.attributes = attributes;
        this.trackHits = trackHits;
        this.includeFrozen = includeFrozen;
    }

    public QueryContainer withFrozen() {
        throw new UnsupportedOperationException();
    }

    public Query query() {
        return query;
    }

    public List<Tuple<FieldExtraction, String>> fields() {
        return fields;
    }

    public Map<String, Sort> sort() {
        return sort;
    }

    public boolean shouldTrackHits() {
        return trackHits;
    }

    public QueryContainer with(Query q) {
        return new QueryContainer(q, fields, attributes, sort, trackHits, includeFrozen);
    }

    public QueryContainer addColumn(Attribute attr) {
        Expression expression = attributes.getOrDefault(attr, attr);
        Tuple<QueryContainer, FieldExtraction> tuple = asFieldExtraction(attr);
        return tuple.v1().addColumn(tuple.v2(), Expressions.id(expression));
    }

    private Tuple<QueryContainer, FieldExtraction> asFieldExtraction(Attribute attr) {
        // resolve it Expression
        Expression expression = attributes.getOrDefault(attr, attr);

        if (expression instanceof FieldAttribute) {
            FieldAttribute fa = (FieldAttribute) expression;
            if (fa.isNested()) {
                throw new UnsupportedOperationException("Nested not yet supported");
            }
            return new Tuple<>(this, extractorRegistry.fieldExtraction(expression));
        }

        if (expression.foldable()) {
            return new Tuple<>(this, new ComputedRef(new ConstantInput(expression.source(), expression, expression.fold())));
        }

        throw new EqlIllegalArgumentException("Unknown output attribute {}", attr);
    }

    public QueryContainer addSort(String expressionId, Sort sortable) {
        Map<String, Sort> newSort = new LinkedHashMap<>(this.sort);
        newSort.put(expressionId, sortable);
        return new QueryContainer(query, fields, attributes, newSort, trackHits, includeFrozen);
    }

    //
    // reference methods
    //

    public QueryContainer addColumn(FieldExtraction ref, String id) {
        return new QueryContainer(query, combine(fields, new Tuple<>(ref, id)), attributes, sort, trackHits, includeFrozen);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, attributes, fields, trackHits, includeFrozen);
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
                && Objects.equals(attributes, other.attributes)
                && Objects.equals(fields, other.fields)
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
            throw new EqlIllegalArgumentException("error rendering", e);
        }
    }
}