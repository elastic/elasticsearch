package graphql.schema;

import graphql.PublicApi;

/**
 * This is passed to a {@link graphql.schema.DataFetcherFactory} when it is invoked to
 * get a {@link graphql.schema.DataFetcher}
 */
@PublicApi
public class DataFetcherFactoryEnvironment {
    private final GraphQLFieldDefinition fieldDefinition;

    DataFetcherFactoryEnvironment(GraphQLFieldDefinition fieldDefinition) {
        this.fieldDefinition = fieldDefinition;
    }

    /**
     * @return the field that needs a {@link graphql.schema.DataFetcher}
     */
    public GraphQLFieldDefinition getFieldDefinition() {
        return fieldDefinition;
    }

    public static Builder newDataFetchingFactoryEnvironment() {
        return new Builder();
    }

    static class Builder {
        GraphQLFieldDefinition fieldDefinition;

        public Builder fieldDefinition(GraphQLFieldDefinition fieldDefinition) {
            this.fieldDefinition = fieldDefinition;
            return this;
        }

        public DataFetcherFactoryEnvironment build() {
            return new DataFetcherFactoryEnvironment(fieldDefinition);
        }
    }
}
