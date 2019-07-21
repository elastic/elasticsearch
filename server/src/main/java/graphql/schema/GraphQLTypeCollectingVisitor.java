package graphql.schema;

import graphql.AssertException;
import graphql.Internal;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.lang.String.format;

@Internal
public class GraphQLTypeCollectingVisitor extends GraphQLTypeVisitorStub {

    private final Map<String, GraphQLType> result = new LinkedHashMap<>();

    public GraphQLTypeCollectingVisitor() {
    }

    @Override
    public TraversalControl visitGraphQLEnumType(GraphQLEnumType node, TraverserContext<GraphQLType> context) {
        assertTypeUniqueness(node, result);
        save(node.getName(), node);
        return super.visitGraphQLEnumType(node, context);
    }

    @Override
    public TraversalControl visitGraphQLScalarType(GraphQLScalarType node, TraverserContext<GraphQLType> context) {
        assertTypeUniqueness(node, result);
        save(node.getName(), node);
        return super.visitGraphQLScalarType(node, context);
    }

    @Override
    public TraversalControl visitGraphQLObjectType(GraphQLObjectType node, TraverserContext<GraphQLType> context) {
        if (isTypeReference(node.getName())) {
            assertTypeUniqueness(node, result);
        } else {
            save(node.getName(), node);
        }
        return super.visitGraphQLObjectType(node, context);
    }

    @Override
    public TraversalControl visitGraphQLInputObjectType(GraphQLInputObjectType node, TraverserContext<GraphQLType> context) {
        if (isTypeReference(node.getName())) {
            assertTypeUniqueness(node, result);
        } else {
            save(node.getName(), node);
        }
        return super.visitGraphQLInputObjectType(node, context);
    }

    @Override
    public TraversalControl visitGraphQLInterfaceType(GraphQLInterfaceType node, TraverserContext<GraphQLType> context) {
        if (isTypeReference(node.getName())) {
            assertTypeUniqueness(node, result);
        } else {
            save(node.getName(), node);
        }

        return super.visitGraphQLInterfaceType(node, context);
    }

    @Override
    public TraversalControl visitGraphQLUnionType(GraphQLUnionType node, TraverserContext<GraphQLType> context) {
        assertTypeUniqueness(node, result);
        save(node.getName(), node);
        return super.visitGraphQLUnionType(node, context);
    }

    @Override
    public TraversalControl visitGraphQLFieldDefinition(GraphQLFieldDefinition node, TraverserContext<GraphQLType> context) {

        return super.visitGraphQLFieldDefinition(node, context);
    }

    // TODO: eh? Isn't it similar to assertTypeUniqueness?
    private boolean isTypeReference(String name) {
        return result.containsKey(name) && !(result.get(name) instanceof GraphQLTypeReference);
    }

    private void save(String name, GraphQLType type) {
        result.put(name, type);
    }


    /*
        From http://facebook.github.io/graphql/#sec-Type-System

           All types within a GraphQL schema must have unique names. No two provided types may have the same name.
           No provided type may have a name which conflicts with any built in types (including Scalar and Introspection types).

        Enforcing this helps avoid problems later down the track fo example https://github.com/graphql-java/graphql-java/issues/373
    */
    private void assertTypeUniqueness(GraphQLType type, Map<String, GraphQLType> result) {
        GraphQLType existingType = result.get(type.getName());
        // do we have an existing definition
        if (existingType != null) {
            // type references are ok
            if (!(existingType instanceof GraphQLTypeReference || type instanceof GraphQLTypeReference))
                // object comparison here is deliberate
                if (existingType != type) {
                    throw new AssertException(format("All types within a GraphQL schema must have unique names. No two provided types may have the same name.\n" +
                                    "No provided type may have a name which conflicts with any built in types (including Scalar and Introspection types).\n" +
                                    "You have redefined the type '%s' from being a '%s' to a '%s'",
                            type.getName(), existingType.getClass().getSimpleName(), type.getClass().getSimpleName()));
                }
        }
    }

    public Map<String, GraphQLType> getResult() {
        return result;
    }
}
