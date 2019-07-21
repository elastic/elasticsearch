package graphql.schema.diff;

import graphql.PublicApi;

/**
 * This is the level of difference between graphql APIs
 */
@PublicApi
public enum DiffLevel {
    /**
     * A simple info object coming out of the difference engine
     */
    INFO,
    /**
     * The new API has made a breaking change
     */
    BREAKING,
    /**
     * The new API has made a dangerous (but non breaking) change
     */
    DANGEROUS
}
