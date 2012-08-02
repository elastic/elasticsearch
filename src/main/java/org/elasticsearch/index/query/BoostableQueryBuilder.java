package org.elasticsearch.index.query;

/**
 * Query builder which allow setting some boost
 */
public interface BoostableQueryBuilder<B extends BoostableQueryBuilder<B>> {

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    public B boost(float boost);

}
