package org.elasticsearch.search.facet;

/**
 */
public class ExtendedFacetsTestsMultiShardMultiNodeTests extends ExtendedFacetsTests {

    @Override
    protected int numberOfShards() {
        return 8;
    }

    @Override
    protected int numDocs() {
        return 10000;
    }
}
