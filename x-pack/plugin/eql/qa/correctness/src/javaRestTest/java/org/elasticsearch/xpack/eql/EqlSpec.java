/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import java.util.Objects;

public class EqlSpec {

    private int queryNo;
    private String query;
    private long seqCount;
    private long[] expectedEventIds;
    private long[] filterCounts;
    private String[] filters;
    private double time;

    public int queryNo() {
        return queryNo;
    }

    public void queryNo(int queryNo) {
        this.queryNo = queryNo;
    }

    public String query() {
        return query;
    }

    public void query(String query) {
        this.query = query;
    }

    public long seqCount() {
        return seqCount;
    }

    public void seqCount(long seqCount) {
        this.seqCount = seqCount;
    }

    public long[] expectedEventIds() {
        return expectedEventIds;
    }

    public void expectedEventIds(long[] expectedEventIds) {
        this.expectedEventIds = expectedEventIds;
    }

    public long[] filterCounts() {
        return filterCounts;
    }

    public void filterCounts(long[] filterCounts) {
        this.filterCounts = filterCounts;
    }

    public String[] filters() {
        return filters;
    }

    public void filters(String[] filters) {
        this.filters = filters;
    }

    public double time() {
        return time;
    }

    public void time(double time) {
        this.time = time;
    }

    public EqlSpec(int queryNo) {
        this.queryNo = queryNo;
    }

    @Override
    public String toString() {
        return queryNo + "";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EqlSpec eqlSpec = (EqlSpec) o;
        return queryNo == eqlSpec.queryNo;
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryNo);
    }
}
