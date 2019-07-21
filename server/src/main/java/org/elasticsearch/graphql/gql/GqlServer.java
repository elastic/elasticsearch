package org.elasticsearch.graphql.gql;

import graphql.*;
import graphql.schema.*;

import java.util.Map;

import static graphql.schema.GraphQLObjectType.newObject;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLTypeReference.typeRef;
import static graphql.schema.GraphQLNonNull.nonNull;
import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.Scalars.*;

public class GqlServer {
    GqlBuilder builder;
    GraphQLSchema schema;
    GraphQL graphql;

    public GqlServer() {
        builder = new GqlBuilder();

        addPingResolve(builder);

        schema = builder.build();
        graphql = GraphQL.newGraphQL(schema).build();
    }

    void addPingResolve(GqlBuilder builder) {
        builder
            .queryField(newFieldDefinition()
                .description("Ping server if it is available.")
                .name("ping")
                .type(nonNull(GraphQLString)))
            .fetcher("Query", "ping", new StaticDataFetcher("pong"));
    }

    Map<String, Object> executeToSpecification(String query, String operationName, Map<String, Object> variables, Object ctx) {
        ExecutionResult result = graphql.execute(
            ExecutionInput.newExecutionInput(query)
                .operationName(operationName)
                .variables(variables)
                .context(ctx)
                .build()
        );
        return result.toSpecification();
    }
}
