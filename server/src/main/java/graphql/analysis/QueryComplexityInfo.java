package graphql.analysis;

import graphql.PublicApi;

/**
 * The query complexity info.
 */
@PublicApi
public class QueryComplexityInfo {

    private final int complexity;

    private QueryComplexityInfo(int complexity) {
        this.complexity = complexity;
    }

    /**
     * This returns the query complexity.
     *
     * @return the query complexity
     */
    public int getComplexity() {
        return complexity;
    }

    @Override
    public String toString() {
        return "QueryComplexityInfo{" +
                "complexity=" + complexity +
                '}';
    }

    /**
     * @return a new {@link QueryComplexityInfo} builder
     */
    public static Builder newQueryComplexityInfo() {
        return new Builder();
    }

    @PublicApi
    public static class Builder {

        private int complexity;

        private Builder() {
        }

        /**
         * The query complexity.
         *
         * @param complexity the query complexity
         * @return this builder
         */
        public Builder complexity(int complexity) {
            this.complexity = complexity;
            return this;
        }

        /**
         * @return a built {@link QueryComplexityInfo} object
         */
        public QueryComplexityInfo build() {
            return new QueryComplexityInfo(complexity);
        }
    }
}
