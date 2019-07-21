package graphql.schema;

import graphql.PublicApi;

import java.util.Map;

/**
 * A {@link graphql.schema.SelectedField} represents a field that occurred in a query selection set during
 * execution and they are returned from using the {@link graphql.schema.DataFetchingFieldSelectionSet}
 * interface returned via {@link DataFetchingEnvironment#getSelectionSet()}
 */
@PublicApi
public interface SelectedField {
    /**
     * @return the name of the field
     */
    String getName();

    /**
     * @return the fully name of the field
     */
    String getQualifiedName();

    /**
     * @return the field runtime definition
     */
    GraphQLFieldDefinition getFieldDefinition();

    /**
     * @return a map of the arguments to this selected field
     */
    Map<String, Object> getArguments();

    /**
     * @return a sub selection set (if it has any)
     */
    DataFetchingFieldSelectionSet getSelectionSet();
}
