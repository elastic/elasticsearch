package graphql.execution.nextgen;

import graphql.Internal;
import graphql.schema.DataFetcher;

@Internal
public interface BatchedDataFetcher<T> extends DataFetcher<T> {
}
