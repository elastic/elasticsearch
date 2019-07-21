package graphql.schema.diff;

import graphql.Assert;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.PublicApi;
import graphql.introspection.IntrospectionQuery;
import graphql.schema.GraphQLSchema;

import java.util.Map;

/**
 * Represents 2 schemas that can be diffed.  The {@link SchemaDiff} code
 * assumes that that schemas to be diffed are the result of a
 * {@link graphql.introspection.IntrospectionQuery}.
 */
@PublicApi
public class DiffSet {

    private final Map<String, Object> introspectionOld;
    private final Map<String, Object> introspectionNew;

    public DiffSet(Map<String, Object> introspectionOld, Map<String, Object> introspectionNew) {
        this.introspectionOld = introspectionOld;
        this.introspectionNew = introspectionNew;
    }

    /**
     * @return the old API as an introspection result
     */
    public Map<String, Object> getOld() {
        return introspectionOld;
    }

    /**
     * @return the new API as an introspection result
     */
    public Map<String, Object> getNew() {
        return introspectionNew;
    }


    /**
     * Creates a diff set out of the result of 2 introspection queries.
     *
     * @param introspectionOld the older introspection query
     * @param introspectionNew the newer introspection query
     *
     * @return a diff set representing them
     */
    public static DiffSet diffSet(Map<String, Object> introspectionOld, Map<String, Object> introspectionNew) {
        return new DiffSet(introspectionOld, introspectionNew);
    }

    /**
     * Creates a diff set out of the result of 2 schemas.
     *
     * @param schemaOld the older schema
     * @param schemaNew the newer schema
     *
     * @return a diff set representing them
     */
    public static DiffSet diffSet(GraphQLSchema schemaOld, GraphQLSchema schemaNew) {
        Map<String, Object> introspectionOld = introspect(schemaOld);
        Map<String, Object> introspectionNew = introspect(schemaNew);
        return diffSet(introspectionOld, introspectionNew);
    }

    private static Map<String, Object> introspect(GraphQLSchema schema) {
        GraphQL gql = GraphQL.newGraphQL(schema).build();
        ExecutionResult result = gql.execute(IntrospectionQuery.INTROSPECTION_QUERY);
        Assert.assertTrue(result.getErrors().size() == 0, "The schema has errors during Introspection");
        return result.getData();
    }
}
