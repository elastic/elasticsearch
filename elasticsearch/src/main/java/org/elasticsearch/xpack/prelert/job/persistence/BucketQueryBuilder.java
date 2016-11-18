/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.common.Strings;

import java.util.Objects;

/**
 * One time query builder for a single buckets.
 * <ul>
 * <li>Timestamp (Required) - Timestamp of the bucket</li>
 * <li>Expand- Include anomaly records. Default= false</li>
 * <li>IncludeInterim- Include interim results. Default = false</li>
 * <li>partitionValue Set the bucket's max normalised probabiltiy to this
 * partiton field value's max normalised probability. Default = null</li>
 * </ul>
 */
public final class BucketQueryBuilder {
    public static int DEFAULT_SIZE = 100;

    private BucketQuery bucketQuery;

    public BucketQueryBuilder(String timestamp) {
        bucketQuery = new BucketQuery(timestamp);
    }

    public BucketQueryBuilder expand(boolean expand) {
        bucketQuery.expand = expand;
        return this;
    }

    public BucketQueryBuilder includeInterim(boolean include) {
        bucketQuery.includeInterim = include;
        return this;
    }

    /**
     * partitionValue must be non null and not empty else it
     * is not set
     */
    public BucketQueryBuilder partitionValue(String partitionValue) {
        if (!Strings.isNullOrEmpty(partitionValue)) {
            bucketQuery.partitionValue = partitionValue;
        }
        return this;
    }

    public BucketQueryBuilder.BucketQuery build() {
        return bucketQuery;
    }

    public class BucketQuery {
        private String timestamp;
        private boolean expand = false;
        private boolean includeInterim = false;
        private String partitionValue = null;

        public BucketQuery(String timestamp) {
            this.timestamp = timestamp;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public boolean isIncludeInterim() {
            return includeInterim;
        }

        public boolean isExpand() {
            return expand;
        }

        /**
         * @return Null if not set
         */
        public String getPartitionValue() {
            return partitionValue;
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, expand, includeInterim,
                    partitionValue);
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

            BucketQuery other = (BucketQuery) obj;
            return Objects.equals(timestamp, other.timestamp) &&
                    Objects.equals(expand, other.expand) &&
                    Objects.equals(includeInterim, other.includeInterim) &&
                    Objects.equals(partitionValue, other.partitionValue);
        }

    }
}
