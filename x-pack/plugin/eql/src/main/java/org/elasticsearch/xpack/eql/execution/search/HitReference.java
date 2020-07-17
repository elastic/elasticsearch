/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.search.SearchHit;

import java.util.Objects;

public class HitReference {

    private final String index;
    private final String id;
    
    public HitReference(SearchHit hit) {
        this.index = hit.getIndex();
        this.id = hit.getId();
    }
    
    public String index() {
        return index;
    }

    public String id() {
        return id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        HitReference other = (HitReference) obj;
        return Objects.equals(index, other.index)
                && Objects.equals(id, other.id);
    }

    @Override
    public String toString() {
        return "doc[" + index + "][" + id + "]";
    }
}
