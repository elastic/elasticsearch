package graphql.execution.instrumentation.dataloader;

import graphql.execution.instrumentation.InstrumentationState;
import org.dataloader.DataLoaderRegistry;
import org.slf4j.Logger;

/**
 * A base class that keeps track of whether aggressive batching can be used
 */
public class DataLoaderDispatcherInstrumentationState implements InstrumentationState {

    private final FieldLevelTrackingApproach approach;
    private final DataLoaderRegistry dataLoaderRegistry;
    private final InstrumentationState state;
    private final boolean hasNoDataLoaders;
    private boolean aggressivelyBatching = true;

    public DataLoaderDispatcherInstrumentationState(Logger log, DataLoaderRegistry dataLoaderRegistry) {

        this.dataLoaderRegistry = dataLoaderRegistry;
        this.approach = new FieldLevelTrackingApproach(log, dataLoaderRegistry);
        this.state = approach.createState();
        hasNoDataLoaders = dataLoaderRegistry.getKeys().isEmpty();

    }

    boolean isAggressivelyBatching() {
        return aggressivelyBatching;
    }

    void setAggressivelyBatching(boolean aggressivelyBatching) {
        this.aggressivelyBatching = aggressivelyBatching;
    }

    FieldLevelTrackingApproach getApproach() {
        return approach;
    }

    DataLoaderRegistry getDataLoaderRegistry() {
        return dataLoaderRegistry;
    }

    boolean hasNoDataLoaders() {
        return hasNoDataLoaders;
    }

    InstrumentationState getState() {
        return state;
    }


}
