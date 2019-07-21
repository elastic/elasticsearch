package org.elasticsearch.graphql.gql;

import graphql.schema.*;
import static graphql.schema.GraphQLObjectType.newObject;
import static graphql.schema.FieldCoordinates.coordinates;

import java.util.*;

public class GqlBuilder {
    private GraphQLObjectType.Builder query;
    private GraphQLObjectType.Builder mutation;
    private GraphQLCodeRegistry.Builder code;
    private Set<GraphQLType> types = new LinkedHashSet<>();
    private Set<GraphQLDirective> directives = new LinkedHashSet<>();

    public GqlBuilder() {
        query = newObject()
            .name("Query")
            .description("Main getter entry type.");
        mutation = newObject()
            .name("Mutation")
            .description("State mutation main type.");
        code = GraphQLCodeRegistry.newCodeRegistry();
    }

    public GqlBuilder queryField(GraphQLFieldDefinition.Builder builder) {
        query.field(builder);
        return this;
    }

    public GqlBuilder mutationField(GraphQLFieldDefinition.Builder builder) {
        mutation.field(builder);
        return this;
    }

    public GqlBuilder fetcher(String parentType, String fieldName, DataFetcher<?> dataFetcher) {
        code.dataFetcher(coordinates(parentType, fieldName), dataFetcher);
        return this;
    }

    public GqlBuilder type(GraphQLType type) {
        this.types.add(type);
        return this;
    }

    public GraphQLSchema build() {
        GraphQLSchema.Builder schemaBuilder = new GraphQLSchema.Builder()
            .query(query)
            .mutation(mutation)
            .additionalTypes(types)
            .additionalDirectives(directives)
            .codeRegistry(code.build());
        GraphQLSchema schema = schemaBuilder.build();

        return schema;
    }
}
