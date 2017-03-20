/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

/**
 * One time query builder for records. Sets default values for the following
 * parameters:
 * <ul>
 * <li>From- Skip the first N records. This parameter is for paging if not
 * required set to 0. Default = 0</li>
 * <li>Size- Take only this number of records. Default =
 * {@value DEFAULT_SIZE}</li>
 * <li>IncludeInterim- Include interim results. Default = false</li>
 * <li>SortField- The field to sort results by if <code>null</code> no sort is
 * applied. Default = null</li>
 * <li>SortDescending- Sort in descending order. Default = true</li>
 * <li>recordScoreThreshold- Return only records with a record_score &gt;=
 * this value. Default = 0.0</li>
 * <li>start- The start bucket time. A bucket with this timestamp will be
 * included in the results. If 0 all buckets up to <code>endEpochMs</code> are
 * returned. Default = -1</li>
 * <li>end- The end bucket timestamp buckets up to but NOT including this
 * timestamp are returned. If 0 all buckets from <code>startEpochMs</code> are
 * returned. Default = -1</li>
 * </ul>
 */
public final class RecordsQueryBuilder {

    public static final int DEFAULT_SIZE = 100;

    private RecordsQuery recordsQuery = new RecordsQuery();

    public RecordsQueryBuilder from(int from) {
        recordsQuery.from = from;
        return this;
    }

    public RecordsQueryBuilder size(int size) {
        recordsQuery.size = size;
        return this;
    }

    public RecordsQueryBuilder epochStart(String startTime) {
        recordsQuery.start = startTime;
        return this;
    }

    public RecordsQueryBuilder epochEnd(String endTime) {
        recordsQuery.end = endTime;
        return this;
    }

    public RecordsQueryBuilder includeInterim(boolean include) {
        recordsQuery.includeInterim = include;
        return this;
    }

    public RecordsQueryBuilder sortField(String fieldname) {
        recordsQuery.sortField = fieldname;
        return this;
    }

    public RecordsQueryBuilder sortDescending(boolean sortDescending) {
        recordsQuery.sortDescending = sortDescending;
        return this;
    }

    public RecordsQueryBuilder recordScore(double recordScore) {
        recordsQuery.recordScore = recordScore;
        return this;
    }

    public RecordsQueryBuilder partitionFieldValue(String partitionFieldValue) {
        recordsQuery.partitionFieldValue = partitionFieldValue;
        return this;
    }

    public RecordsQuery build() {
        return recordsQuery;
    }

    public void clear() {
        recordsQuery = new RecordsQuery();
    }

    public class RecordsQuery {

        private int from = 0;
        private int size = DEFAULT_SIZE;
        private boolean includeInterim = false;
        private String sortField;
        private boolean sortDescending = true;
        private double recordScore = 0.0;
        private String partitionFieldValue;
        private String start;
        private String end;


        public int getSize() {
            return size;
        }

        public boolean isIncludeInterim() {
            return includeInterim;
        }

        public String getSortField() {
            return sortField;
        }

        public boolean isSortDescending() {
            return sortDescending;
        }

        public double getRecordScoreThreshold() {
            return recordScore;
        }

        public String getPartitionFieldValue() {
            return partitionFieldValue;
        }

        public int getFrom() {
            return from;
        }

        public String getStart() {
            return start;
        }

        public String getEnd() {
            return end;
        }
    }
}


