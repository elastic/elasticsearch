package org.elasticsearch.common.geo;

import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.TermQueryPrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;

import java.lang.String;

/**
 *
 */
public enum SpatialStrategy {

    TERM("term"),
    RECURSIVE("recursive");

    private final String strategyName;

    private SpatialStrategy(String strategyName) {
        this.strategyName = strategyName;
    }

    public String getStrategyName() {
        return strategyName;
    }

    public PrefixTreeStrategy create(SpatialPrefixTree grid, String fieldName) {
        if (this == TERM) {
            return new TermQueryPrefixTreeStrategy(grid, fieldName);
        }
        return new RecursivePrefixTreeStrategy(grid, fieldName);
    }
}
