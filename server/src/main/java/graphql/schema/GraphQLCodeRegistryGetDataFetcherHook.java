package graphql.schema;

public interface GraphQLCodeRegistryGetDataFetcherHook {
    DataFetcher<?> getDataFetcherHook(FieldCoordinates coordinates);
}
