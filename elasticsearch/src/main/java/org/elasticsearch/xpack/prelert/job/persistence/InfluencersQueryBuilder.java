/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.xpack.prelert.job.results.Influencer;

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
public final class InfluencersQueryBuilder {
    public static final int DEFAULT_SIZE = 100;

    private InfluencersQuery influencersQuery = new InfluencersQuery();

    public InfluencersQueryBuilder from(int from) {
        influencersQuery.from = from;
        return this;
    }

    public InfluencersQueryBuilder size(int size) {
        influencersQuery.size = size;
        return this;
    }

    public InfluencersQueryBuilder includeInterim(boolean include) {
        influencersQuery.includeInterim = include;
        return this;
    }

    public InfluencersQueryBuilder anomalyScoreThreshold(Double anomalyScoreFilter) {
        influencersQuery.anomalyScoreFilter = anomalyScoreFilter;
        return this;
    }

    public InfluencersQueryBuilder sortField(String sortField) {
        influencersQuery.sortField = sortField;
        return this;
    }

    public InfluencersQueryBuilder sortDescending(boolean sortDescending) {
        influencersQuery.sortDescending = sortDescending;
        return this;
    }

    /**
     * If startTime &gt;= 0 the parameter is not set
     */
    public InfluencersQueryBuilder epochStart(String startTime) {
        influencersQuery.epochStart = startTime;
        return this;
    }

    /**
     * If endTime &gt;= 0 the parameter is not set
     */
    public InfluencersQueryBuilder epochEnd(String endTime) {
        influencersQuery.epochEnd = endTime;
        return this;
    }

    public InfluencersQueryBuilder.InfluencersQuery build() {
        return influencersQuery;
    }

    public void clear() {
        influencersQuery = new InfluencersQueryBuilder.InfluencersQuery();
    }


    public class InfluencersQuery {
        private int from = 0;
        private int size = DEFAULT_SIZE;
        private boolean includeInterim = false;
        private double anomalyScoreFilter = 0.0d;
        private String epochStart;
        private String epochEnd;
        private String sortField = Influencer.ANOMALY_SCORE.getPreferredName();
        private boolean sortDescending = false;

        public int getFrom() {
            return from;
        }

        public int getSize() {
            return size;
        }

        public boolean isIncludeInterim() {
            return includeInterim;
        }

        public double getAnomalyScoreFilter() {
            return anomalyScoreFilter;
        }

        public String getEpochStart() {
            return epochStart;
        }

        public String getEpochEnd() {
            return epochEnd;
        }

        public String getSortField() {
            return sortField;
        }

        public boolean isSortDescending() {
            return sortDescending;
        }

        @Override
        public int hashCode() {
            return Objects.hash(from, size, includeInterim, anomalyScoreFilter, epochStart, epochEnd,
                    sortField, sortDescending);
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

            InfluencersQuery other = (InfluencersQuery) obj;
            return Objects.equals(from, other.from) &&
                    Objects.equals(size, other.size) &&
                    Objects.equals(includeInterim, other.includeInterim) &&
                    Objects.equals(epochStart, other.epochStart) &&
                    Objects.equals(epochStart, other.epochStart) &&
                    Objects.equals(anomalyScoreFilter, other.anomalyScoreFilter) &&
                    Objects.equals(sortField, other.sortField) &&
                    this.sortDescending == other.sortDescending;
        }

    }
}
