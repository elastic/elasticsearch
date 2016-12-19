/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

    public RecordsQueryBuilder anomalyScoreThreshold(double anomalyScoreFilter) {
        recordsQuery.anomalyScoreFilter = anomalyScoreFilter;
        return this;
    }

    public RecordsQueryBuilder normalizedProbability(double normalizedProbability) {
        recordsQuery.normalizedProbability = normalizedProbability;
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
        private double anomalyScoreFilter = 0.0d;
        private double normalizedProbability = 0.0d;
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

        public double getAnomalyScoreThreshold() {
            return anomalyScoreFilter;
        }

        public double getNormalizedProbabilityThreshold() {
            return normalizedProbability;
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


