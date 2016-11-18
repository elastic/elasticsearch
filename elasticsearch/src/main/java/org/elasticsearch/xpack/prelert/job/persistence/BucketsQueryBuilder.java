/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.common.Strings;

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
 * <li>normalizedProbabilityThreshold- Return only buckets with a
 * maxNormalizedProbability &gt;= this value. Default = 0.0</li>
 * <li>epochStart- The start bucket time. A bucket with this timestamp will be
 * included in the results. If 0 all buckets up to <code>endEpochMs</code> are
 * returned. Default = -1</li>
 * <li>epochEnd- The end bucket timestamp buckets up to but NOT including this
 * timestamp are returned. If 0 all buckets from <code>startEpochMs</code> are
 * returned. Default = -1</li>
 * <li>partitionValue Set the bucket's max normalised probability to this
 * partition field value's max normalised probability. Default = null</li>
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
        bucketsQuery.anomalyScoreFilter = anomalyScoreFilter;
        return this;
    }

    public BucketsQueryBuilder normalizedProbabilityThreshold(Double normalizedProbability) {
        bucketsQuery.normalizedProbability = normalizedProbability;
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
    public BucketsQueryBuilder epochStart(String startTime) {
        bucketsQuery.epochStart = startTime;
        return this;
    }

    /**
     * If endTime &lt;= 0 the parameter is not set
     */
    public BucketsQueryBuilder epochEnd(String endTime) {
        bucketsQuery.epochEnd = endTime;
        return this;
    }

    public BucketsQueryBuilder.BucketsQuery build() {
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
        private double anomalyScoreFilter = 0.0d;
        private double normalizedProbability = 0.0d;
        private String epochStart;
        private String epochEnd;
        private String partitionValue = null;
        private String sortField = Bucket.TIMESTAMP.getPreferredName();
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

        public double getNormalizedProbability() {
            return normalizedProbability;
        }

        public String getEpochStart() {
            return epochStart;
        }

        public String getEpochEnd() {
            return epochEnd;
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
            return Objects.hash(from, size, expand, includeInterim, anomalyScoreFilter, normalizedProbability, epochStart, epochEnd,
                    partitionValue, sortField, sortDescending);
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
                    Objects.equals(epochStart, other.epochStart) &&
                    Objects.equals(epochStart, other.epochStart) &&
                    Objects.equals(anomalyScoreFilter, other.anomalyScoreFilter) &&
                    Objects.equals(normalizedProbability, other.normalizedProbability) &&
                    Objects.equals(partitionValue, other.partitionValue) &&
                    Objects.equals(sortField, other.sortField) &&
                    this.sortDescending == other.sortDescending;
        }

    }
}
