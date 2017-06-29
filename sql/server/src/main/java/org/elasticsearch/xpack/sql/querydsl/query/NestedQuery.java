/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xpack.sql.tree.Location;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;

public class NestedQuery extends UnaryQuery {

    // TODO: make this configurable
    private static final int MAX_INNER_HITS = 99;
    private static final List<String> NO_STORED_FIELD = singletonList(StoredFieldsContext._NONE_);

    private final String path;
    private final Map<String, Boolean> fields;

    public NestedQuery(Location location, String path, Query child) {
        this(location, path, emptyMap(), child);
    }

    public NestedQuery(Location location, String path, Map<String, Boolean> fields, Query child) {
        super(location, child);
        this.path = path;
        this.fields = fields;
    }

    public String path() {
        return path;
    }

    public Map<String, Boolean> fields() {
        return fields;
    }

    @Override
    public QueryBuilder asBuilder() {
        // disable source

        NestedQueryBuilder query = nestedQuery(path, child().asBuilder(), ScoreMode.None);

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
    
    @Override
    public int hashCode() {
        return Objects.hash(path, fields, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        NestedQuery other = (NestedQuery) obj;
        return Objects.equals(path, other.path)
                && Objects.equals(fields, other.fields)
                && Objects.equals(child(), other.child());
    }
}