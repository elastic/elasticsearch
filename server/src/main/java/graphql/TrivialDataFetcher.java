package graphql;

import graphql.schema.DataFetcher;


/**
 * Mark a DataFetcher as trivial:
 * If a data fetcher is simply mapping data from an object to a field, it can be considered a trivial data fetcher for the purposes
 * of tracing and so on.
 */
@PublicSpi
public interface TrivialDataFetcher<T> extends DataFetcher<T> {
}
