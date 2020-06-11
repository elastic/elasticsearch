/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.index;

public class QlFieldCapability {
    private final String name;
    private final String type;
    private final boolean isSearchable;
    private final boolean isAggregatable;

    private String[] indices;
    private String[] nonSearchableIndices;
    private String[] nonAggregatableIndices;
    
    public QlFieldCapability(String name, String type, boolean isSearchable, boolean isAggregatable) {
        this.name = name;
        this.type = type;
        this.isSearchable = isSearchable;
        this.isAggregatable = isAggregatable;
    }

    public String[] getIndices() {
        return indices;
    }

    public void setIndices(String[] indices) {
        this.indices = indices;
    }

    public String[] getNonSearchableIndices() {
        return nonSearchableIndices;
    }

    public void setNonSearchableIndices(String[] nonSearchableIndices) {
        this.nonSearchableIndices = nonSearchableIndices;
    }

    public String[] getNonAggregatableIndices() {
        return nonAggregatableIndices;
    }

    public void setNonAggregatableIndices(String[] nonAggregatableIndices) {
        this.nonAggregatableIndices = nonAggregatableIndices;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public boolean isSearchable() {
        return isSearchable;
    }

    public boolean isAggregatable() {
        return isAggregatable;
    }
}
