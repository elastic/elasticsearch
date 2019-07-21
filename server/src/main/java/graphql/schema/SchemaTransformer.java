package graphql.schema;

/**
 * These are called by the {@link graphql.schema.idl.SchemaGenerator} after a valid schema has been built
 * and they can then adjust it accordingly with some sort of post processing.
 */
public interface SchemaTransformer {

    /**
     * Called to transform the schema from its built state into something else
     *
     * @param originalSchema the original built schema
     *
     * @return a non null schema
     */
    GraphQLSchema transform(GraphQLSchema originalSchema);
}
