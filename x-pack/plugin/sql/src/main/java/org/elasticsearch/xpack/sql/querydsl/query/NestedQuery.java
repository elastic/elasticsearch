/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.query;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.tree.Source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;

/**
 * A query to a nested document.
 */
public class NestedQuery extends Query {
    // TODO: make this configurable
    private static final int MAX_INNER_HITS = 99;
    private static final List<String> NO_STORED_FIELD = singletonList(StoredFieldsContext._NONE_);

    private final String path;
    private final Map<String, Boolean> fields;
    private final Query child;

    public NestedQuery(Source source, String path, Query child) {
        this(source, path, emptyMap(), child);
    }

    public NestedQuery(Source source, String path, Map<String, Boolean> fields, Query child) {
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
    public Query addNestedField(String path, String field, boolean hasDocValues) {
        if (false == this.path.equals(path)) {
            // I'm not at the right path so let my child query have a crack at it
            Query rewrittenChild = child.addNestedField(path, field, hasDocValues);
            if (rewrittenChild == child) {
                return this;
            }
            return new NestedQuery(source(), path, fields, rewrittenChild);
        }
        if (fields.containsKey(field)) {
            // I already have the field, no rewriting needed
            return this;
        }
        Map<String, Boolean> newFields = new HashMap<>(fields.size() + 1);
        newFields.putAll(fields);
        newFields.put(field, hasDocValues);
        return new NestedQuery(source(), path, unmodifiableMap(newFields), child);
    }

    @Override
    public void enrichNestedSort(NestedSortBuilder sort) {
        child.enrichNestedSort(sort);
        if (false == sort.getPath().equals(path)) {
            return;
        }
        QueryBuilder childAsBuilder = child.asBuilder();
        if (sort.getFilter() != null && false == sort.getFilter().equals(childAsBuilder)) {
            throw new SqlIllegalArgumentException("nested query should have been grouped in one place");
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

            boolean noSourceNeeded = true;
            List<String> sourceFields = new ArrayList<>();

            for (Entry<String, Boolean> entry : fields.entrySet()) {
                if (entry.getValue()) {
                    ihb.addDocValueField(entry.getKey());
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

    Map<String, Boolean> fields() {
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
