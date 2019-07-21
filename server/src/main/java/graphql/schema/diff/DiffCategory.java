package graphql.schema.diff;

import graphql.PublicApi;

/**
 * A classification of difference events.
 */
@PublicApi
public enum DiffCategory {
    /**
     * The new API is missing something compared to the old API
     */
    MISSING,
    /**
     * The new API has become stricter for existing clients than the old API
     */
    STRICTER,
    /**
     * The new API has an invalid structure
     */
    INVALID,
    /**
     * The new API has added something not present in the old API
     */
    ADDITION,
    /**
     * The new API has changed something compared to the old API
     */
    DIFFERENT
}
