/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql;

import java.util.Objects;

public class EqlSpec {

    private boolean caseSensitive;
    private long seqCount;
    private long[] expectedEventIds;
    private long[] filterCounts;
    private String[] filters;
    private String query;
    private double time;
    private int queryNo;

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public void caseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
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

    public String query() {
        return query;
    }

    public void query(String query) {
        this.query = query;
    }

    public double time() {
        return time;
    }

    public void time(double time) {
        this.time = time;
    }

    public int queryNo() {
        return queryNo;
    }

    public void queryNo(int queryNo) {
        this.queryNo = queryNo;
    }

    @Override
    public String toString() {
        return queryNo + "";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        EqlSpec that = (EqlSpec) other;

        return Objects.equals(this.query, that.query) && Objects.equals(this.caseSensitive, that.caseSensitive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.query, this.caseSensitive);
    }
}
