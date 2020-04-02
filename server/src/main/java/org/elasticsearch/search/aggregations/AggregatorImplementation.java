package org.elasticsearch.search.aggregations;

import org.elasticsearch.search.aggregations.support.AggregatorSupplier;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.util.function.Predicate;

/**
 * Configuration for how {@link Aggregator}s are built.
 */
public class AggregatorImplementation {
    private final Predicate<ValuesSourceType> appliesTo;
    private final AggregatorSupplier aggregatorSupplier;

    public AggregatorImplementation(Predicate<ValuesSourceType> appliesTo, AggregatorSupplier aggregatorSupplier) {
        super();
        this.appliesTo = appliesTo;
        this.aggregatorSupplier = aggregatorSupplier;
    }

    /**
     * Matches {@linkplain ValuesSourceType}s that this implementation supports.
     */
    public Predicate<ValuesSourceType> getAppliesTo() {
        return appliesTo;
    }

    /**
     * The factory for the {@link Aggregator} implementation.
     */
    public AggregatorSupplier getAggregatorSupplier() {
        return aggregatorSupplier;
    }
}