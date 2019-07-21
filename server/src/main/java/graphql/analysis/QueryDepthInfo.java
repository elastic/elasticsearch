package graphql.analysis;

import graphql.PublicApi;

/**
 * The query depth info.
 */
@PublicApi
public class QueryDepthInfo {

    private final int depth;

    private QueryDepthInfo(int depth) {
        this.depth = depth;
    }

    /**
     * This returns the query depth.
     *
     * @return the query depth
     */
    public int getDepth() {
        return depth;
    }

    @Override
    public String toString() {
        return "QueryDepthInfo{" +
                "depth=" + depth +
                '}';
    }

    /**
     * @return a new {@link QueryDepthInfo} builder
     */
    public static Builder newQueryDepthInfo() {
        return new Builder();
    }

    @PublicApi
    public static class Builder {

        private int depth;

        private Builder() {
        }

        /**
         * The query depth.
         *
         * @param depth the depth complexity
         * @return this builder
         */
        public Builder depth(int depth) {
            this.depth = depth;
            return this;
        }

        /**
         * @return a built {@link QueryDepthInfo} object
         */
        public QueryDepthInfo build() {
            return new QueryDepthInfo(depth);
        }
    }
}
