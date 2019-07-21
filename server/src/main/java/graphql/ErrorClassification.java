package graphql;

/**
 * Errors in graphql-java can have a classification to help with the processing
 * of errors.  Custom {@link graphql.GraphQLError} implementations could use
 * custom classifications.
 * <p>
 * graphql-java ships with a standard set of error classifications via {@link graphql.ErrorType}
 */
@PublicApi
public interface ErrorClassification {

    /**
     * This is called to create a representation of the error classification
     * that can be put into the `extensions` map of the graphql error under the key 'classification'
     * when {@link GraphQLError#toSpecification()} is called
     *
     * @param error the error associated with this classification
     *
     * @return an object representation of this error classification
     */
    @SuppressWarnings("unused")
    default Object toSpecification(GraphQLError error) {
        return String.valueOf(this);
    }
}
