package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.xcontent.ToXContent;

/**
 *
 */
public abstract class AbstractAggregationBuilder implements ToXContent {

    protected final String name;
    protected final String type;

    protected AbstractAggregationBuilder(String name, String type) {
        this.name = name;
        this.type = type;
    }

}
