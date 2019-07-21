package graphql.schema;

import graphql.Internal;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import static graphql.Assert.assertTrue;
import static graphql.schema.FieldCoordinates.coordinates;
import static graphql.util.TraversalControl.CONTINUE;

/**
 * This ensure that all fields have data fetchers and that unions and interfaces have type resolvers
 */
@Internal
class CodeRegistryVisitor extends GraphQLTypeVisitorStub {
    private final GraphQLCodeRegistry.Builder codeRegistry;

    CodeRegistryVisitor(GraphQLCodeRegistry.Builder codeRegistry) {
        this.codeRegistry = codeRegistry;
    }

    @Override
    public TraversalControl visitGraphQLFieldDefinition(GraphQLFieldDefinition node, TraverserContext<GraphQLType> context) {
        GraphQLFieldsContainer parentContainerType = (GraphQLFieldsContainer) context.getParentContext().thisNode();
        DataFetcher dataFetcher = node.getDataFetcher();
        if (dataFetcher == null) {
            dataFetcher = new PropertyDataFetcher<>(node.getName());
        }
        FieldCoordinates coordinates = coordinates(parentContainerType, node);
        codeRegistry.dataFetcherIfAbsent(coordinates, dataFetcher);
        return CONTINUE;
    }

    @Override
    public TraversalControl visitGraphQLInterfaceType(GraphQLInterfaceType node, TraverserContext<GraphQLType> context) {
        TypeResolver typeResolver = node.getTypeResolver();
        if (typeResolver != null) {
            codeRegistry.typeResolverIfAbsent(node, typeResolver);
        }
        assertTrue(codeRegistry.getTypeResolver(node) != null, "You MUST provide a type resolver for the interface type '" + node.getName() + "'");
        return CONTINUE;
    }

    @Override
    public TraversalControl visitGraphQLUnionType(GraphQLUnionType node, TraverserContext<GraphQLType> context) {
        TypeResolver typeResolver = node.getTypeResolver();
        if (typeResolver != null) {
            codeRegistry.typeResolverIfAbsent(node, typeResolver);
        }
        assertTrue(codeRegistry.getTypeResolver(node) != null, "You MUST provide a type resolver for the union type '" + node.getName() + "'");
        return CONTINUE;
    }
}
