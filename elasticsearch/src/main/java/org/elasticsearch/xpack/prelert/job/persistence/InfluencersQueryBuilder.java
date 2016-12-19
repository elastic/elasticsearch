/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
    public InfluencersQueryBuilder start(String startTime) {
        influencersQuery.start = startTime;
        return this;
    }

    /**
     * If endTime &gt;= 0 the parameter is not set
     */
    public InfluencersQueryBuilder end(String endTime) {
        influencersQuery.end = endTime;
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
        private String start;
        private String end;
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

        public String getStart() {
            return start;
        }

        public String getEnd() {
            return end;
        }

        public String getSortField() {
            return sortField;
        }

        public boolean isSortDescending() {
            return sortDescending;
        }

        @Override
        public int hashCode() {
            return Objects.hash(from, size, includeInterim, anomalyScoreFilter, start, end, sortField, sortDescending);
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
                    Objects.equals(start, other.start) &&
                    Objects.equals(end, other.end) &&
                    Objects.equals(anomalyScoreFilter, other.anomalyScoreFilter) &&
                    Objects.equals(sortField, other.sortField) &&
                    this.sortDescending == other.sortDescending;
        }

    }
}
