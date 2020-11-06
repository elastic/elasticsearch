/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;

/**
 * A query to a nested document.
 */
public class NestedQuery extends Query {
    private static long COUNTER = 0;
    // TODO: make this configurable
    private static final int MAX_INNER_HITS = 99;
    private static final List<String> NO_STORED_FIELD = singletonList(StoredFieldsContext._NONE_);

    private final String path;
    private final Map<String, Map.Entry<Boolean, String>> fields; // field -> (useDocValues, format)
    private final Query child;

    public NestedQuery(Source source, String path, Query child) {
        this(source, path, emptyMap(), child);
    }

    public NestedQuery(Source source, String path, Map<String, Map.Entry<Boolean, String>> fields, Query child) {
        super(source);
        if (path == null) {
            throw new IllegalArgumentException("path is required");
        }
        if (fields == null) {
            throw new IllegalArgumentException("fields is required");
        }
        if (child == null) {
            throw new IllegalArgumentException("child is required");
        }
        this.path = path;
        this.fields = fields;
        this.child = child;
    }

    @Override
    public boolean containsNestedField(String path, String field) {
        boolean iContainThisField = this.path.equals(path) && fields.containsKey(field);
        boolean myChildContainsThisField = child.containsNestedField(path, field);
        return iContainThisField || myChildContainsThisField;
    }

    @Override
    public Query addNestedField(String path, String field, String format, boolean hasDocValues) {
        if (false == this.path.equals(path)) {
            // I'm not at the right path so let my child query have a crack at it
            Query rewrittenChild = child.addNestedField(path, field, format, hasDocValues);
            if (rewrittenChild == child) {
                return this;
            }
            return new NestedQuery(source(), path, fields, rewrittenChild);
        }
        if (fields.containsKey(field)) {
            // I already have the field, no rewriting needed
            return this;
        }
        Map<String, Map.Entry<Boolean, String>> newFields = new HashMap<>(fields.size() + 1);
        newFields.putAll(fields);
        newFields.put(field, new AbstractMap.SimpleImmutableEntry<>(hasDocValues, format));
        return new NestedQuery(source(), path, unmodifiableMap(newFields), child);
    }

    @Override
    public void enrichNestedSort(NestedSortBuilder sort) {
        child.enrichNestedSort(sort);
        if (false == sort.getPath().equals(path)) {
            return;
        }

        //TODO: Add all filters in nested sorting when https://github.com/elastic/elasticsearch/issues/33079 is implemented
        // Adding multiple filters to sort sections makes sense for nested queries where multiple conditions belong to the same
        // nested query. The current functionality creates one nested query for each condition involving a nested field.
        QueryBuilder childAsBuilder = child.asBuilder();
        if (sort.getFilter() != null && false == sort.getFilter().equals(childAsBuilder)) {
            // throw new SqlIllegalArgumentException("nested query should have been grouped in one place");
            return;
        }
        sort.setFilter(childAsBuilder);
    }

    @Override
    public QueryBuilder asBuilder() {
        // disable score
        NestedQueryBuilder query = nestedQuery(path, child.asBuilder(), ScoreMode.None);

        if (!fields.isEmpty()) {
            InnerHitBuilder ihb = new InnerHitBuilder();
            ihb.setSize(0);
            ihb.setSize(MAX_INNER_HITS);
            ihb.setName(path + "_" + COUNTER++);

            boolean noSourceNeeded = true;
            List<String> sourceFields = new ArrayList<>();

            for (Map.Entry<String, Map.Entry<Boolean, String>> entry : fields.entrySet()) {
                if (entry.getValue().getKey()) {
                    ihb.addDocValueField(entry.getKey(), entry.getValue().getValue());
                }
                else {
                    sourceFields.add(entry.getKey());
                    noSourceNeeded = false;
                }
            }

            if (noSourceNeeded) {
                ihb.setFetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
                ihb.setStoredFieldNames(NO_STORED_FIELD);
            }
            else {
                ihb.setFetchSourceContext(new FetchSourceContext(true, sourceFields.toArray(new String[sourceFields.size()]), null));
            }

            query.innerHit(ihb);
        }

        return query;
    }

    String path() {
        return path;
    }

    Map<String, Map.Entry<Boolean, String>> fields() {
        return fields;
    }

    Query child() {
        return child;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path, fields, child);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        NestedQuery other = (NestedQuery) obj;
        return path.equals(other.path)
                && fields.equals(other.fields)
                && child.equals(other.child);
    }

    @Override
    protected String innerToString() {
        return path + "." + fields + "[" + child + "]";
    }
}
