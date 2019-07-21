package graphql.execution;

import graphql.GraphQLException;
import graphql.PublicApi;

/**
 * This is thrown if multiple operations are defined in the query and
 * the operation name is missing or there is no matching operation name
 * contained in the GraphQL query.
 */
@PublicApi
public class UnknownOperationException extends GraphQLException {

    public UnknownOperationException(String message) {
        super(message);
    }
}
