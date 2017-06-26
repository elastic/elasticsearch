/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.ml.job.results.Result;

import java.util.Objects;

/**
 * One time query builder for buckets.
 * <ul>
 * <li>From- Skip the first N Buckets. This parameter is for paging if not
 * required set to 0. Default = 0</li>
 * <li>Size- Take only this number of Buckets. Default =
 * {@value DEFAULT_SIZE}</li>
 * <li>Expand- Include anomaly records. Default= false</li>
 * <li>IncludeInterim- Include interim results. Default = false</li>
 * <li>anomalyScoreThreshold- Return only buckets with an anomalyScore &gt;=
 * this value. Default = 0.0</li>
 * <li>start- The start bucket time. A bucket with this timestamp will be
 * included in the results. If 0 all buckets up to <code>endEpochMs</code> are
 * returned. Default = -1</li>
 * <li>end- The end bucket timestamp buckets up to but NOT including this
 * timestamp are returned. If 0 all buckets from <code>startEpochMs</code> are
 * returned. Default = -1</li>
 * <li>partitionValue Set the bucket's max normalized probability to this
 * partition field value's max normalized probability. Default = null</li>
 * </ul>
 */
public final class BucketsQueryBuilder {
    public static final int DEFAULT_SIZE = 100;

    private BucketsQuery bucketsQuery = new BucketsQuery();

    public BucketsQueryBuilder from(int from) {
        bucketsQuery.from = from;
        return this;
    }

    public BucketsQueryBuilder size(int size) {
        bucketsQuery.size = size;
        return this;
    }

    public BucketsQueryBuilder expand(boolean expand) {
        bucketsQuery.expand = expand;
        return this;
    }

    public BucketsQueryBuilder includeInterim(boolean include) {
        bucketsQuery.includeInterim = include;
        return this;
    }

    public BucketsQueryBuilder anomalyScoreThreshold(Double anomalyScoreFilter) {
        if (anomalyScoreFilter != null) {
            bucketsQuery.anomalyScoreFilter = anomalyScoreFilter;
        }
        return this;
    }

    /**
     * @param partitionValue Not set if null or empty
     */
    public BucketsQueryBuilder partitionValue(String partitionValue) {
        if (!Strings.isNullOrEmpty(partitionValue)) {
            bucketsQuery.partitionValue = partitionValue;
        }
        return this;
    }

    public BucketsQueryBuilder sortField(String sortField) {
        bucketsQuery.sortField = sortField;
        return this;
    }

    public BucketsQueryBuilder sortDescending(boolean sortDescending) {
        bucketsQuery.sortDescending = sortDescending;
        return this;
    }

    /**
     * If startTime &lt;= 0 the parameter is not set
     */
    public BucketsQueryBuilder start(String startTime) {
        bucketsQuery.start = startTime;
        return this;
    }

    /**
     * If endTime &lt;= 0 the parameter is not set
     */
    public BucketsQueryBuilder end(String endTime) {
        bucketsQuery.end = endTime;
        return this;
    }

    public BucketsQueryBuilder timestamp(String timestamp) {
        bucketsQuery.timestamp = timestamp;
        bucketsQuery.size = 1;
        return this;
    }

    public BucketsQueryBuilder.BucketsQuery build() {
        if (bucketsQuery.timestamp != null && (bucketsQuery.start != null || bucketsQuery.end != null)) {
            throw new IllegalStateException("Either specify timestamp or start/end");
        }

        return bucketsQuery;
    }

    public void clear() {
        bucketsQuery = new BucketsQueryBuilder.BucketsQuery();
    }


    public class BucketsQuery {
        private int from = 0;
        private int size = DEFAULT_SIZE;
        private boolean expand = false;
        private boolean includeInterim = false;
        private double anomalyScoreFilter = 0.0;
        private String start;
        private String end;
        private String timestamp;
        private String partitionValue = null;
        private String sortField = Result.TIMESTAMP.getPreferredName();
        private boolean sortDescending = false;

        public int getFrom() {
            return from;
        }

        public int getSize() {
            return size;
        }

        public boolean isExpand() {
            return expand;
        }

        public boolean isIncludeInterim() {
            return includeInterim;
        }

        public double getAnomalyScoreFilter() {
            return anomalyScoreFilter;
        }

        public String getStart() {
            return start;
        }

        public String getEnd() {
            return end;
        }

        public String getTimestamp() {
            return timestamp;
        }

        /**
         * @return Null if not set
         */
        public String getPartitionValue() {
            return partitionValue;
        }

        public String getSortField() {
            return sortField;
        }

        public boolean isSortDescending() {
            return sortDescending;
        }

        @Override
        public int hashCode() {
            return Objects.hash(from, size, expand, includeInterim, anomalyScoreFilter, start, end,
                    timestamp, partitionValue, sortField, sortDescending);
        }


        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }

            BucketsQuery other = (BucketsQuery) obj;
            return Objects.equals(from, other.from) &&
                    Objects.equals(size, other.size) &&
                    Objects.equals(expand, other.expand) &&
                    Objects.equals(includeInterim, other.includeInterim) &&
                    Objects.equals(start, other.start) &&
                    Objects.equals(end, other.end) &&
                    Objects.equals(timestamp, other.timestamp) &&
                    Objects.equals(anomalyScoreFilter, other.anomalyScoreFilter) &&
                    Objects.equals(partitionValue, other.partitionValue) &&
                    Objects.equals(sortField, other.sortField) &&
                    this.sortDescending == other.sortDescending;
        }

    }
}
