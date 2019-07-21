package graphql.schema;


import graphql.Internal;
import graphql.introspection.Introspection;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Internal
public class SchemaUtil {

    private static final TypeTraverser TRAVERSER = new TypeTraverser();


    Map<String, GraphQLType> allTypes(final GraphQLSchema schema, final Set<GraphQLType> additionalTypes) {
        List<GraphQLType> roots = new ArrayList<>();
        roots.add(schema.getQueryType());

        if (schema.isSupportingMutations()) {
            roots.add(schema.getMutationType());
        }

        if (schema.isSupportingSubscriptions()) {
            roots.add(schema.getSubscriptionType());
        }

        if (additionalTypes != null) {
            roots.addAll(additionalTypes);
        }

        if (schema.getDirectives() != null) {
            roots.addAll(schema.getDirectives());
        }

        roots.add(Introspection.__Schema);

        GraphQLTypeCollectingVisitor visitor = new GraphQLTypeCollectingVisitor();
        TRAVERSER.depthFirst(visitor, roots);
        return visitor.getResult();
    }


    /*
     * Indexes GraphQLObject types registered with the provided schema by implemented GraphQLInterface name
     *
     * This helps in accelerates/simplifies collecting types that implement a certain interface
     *
     * Provided to replace {@link #findImplementations(graphql.schema.GraphQLSchema, graphql.schema.GraphQLInterfaceType)}
     *
     */
    Map<String, List<GraphQLObjectType>> groupImplementations(GraphQLSchema schema) {
        Map<String, List<GraphQLObjectType>> result = new LinkedHashMap<>();
        for (GraphQLType type : schema.getAllTypesAsList()) {
            if (type instanceof GraphQLObjectType) {
                for (GraphQLOutputType interfaceType : ((GraphQLObjectType) type).getInterfaces()) {
                    List<GraphQLObjectType> myGroup = result.computeIfAbsent(interfaceType.getName(), k -> new ArrayList<>());
                    myGroup.add((GraphQLObjectType) type);
                }
            }
        }

        return result;
    }

    /**
     * This method is deprecated due to a performance concern.
     *
     * The Algorithm complexity: O(n^2), where n is number of registered GraphQLTypes
     *
     * That indexing operation is performed twice per input document:
     * 1. during validation
     * 2. during execution
     *
     * We now indexed all types at the schema creation, which has brought complexity down to O(1)
     *
     * @param schema        GraphQL schema
     * @param interfaceType an interface type to find implementations for
     *
     * @return List of object types implementing provided interface
     *
     * @deprecated use {@link graphql.schema.GraphQLSchema#getImplementations(GraphQLInterfaceType)} instead
     */
    @Deprecated
    public List<GraphQLObjectType> findImplementations(GraphQLSchema schema, GraphQLInterfaceType interfaceType) {
        List<GraphQLObjectType> result = new ArrayList<>();
        for (GraphQLType type : schema.getAllTypesAsList()) {
            if (!(type instanceof GraphQLObjectType)) {
                continue;
            }
            GraphQLObjectType objectType = (GraphQLObjectType) type;
            if ((objectType).getInterfaces().contains(interfaceType)) {
                result.add(objectType);
            }
        }
        return result;
    }

    void replaceTypeReferences(GraphQLSchema schema) {
        final Map<String, GraphQLType> typeMap = schema.getTypeMap();
        List<GraphQLType> roots = new ArrayList<>(typeMap.values());
        roots.addAll(schema.getDirectives());
        TRAVERSER.depthFirst(new GraphQLTypeResolvingVisitor(typeMap), roots);
    }

    void extractCodeFromTypes(GraphQLCodeRegistry.Builder codeRegistry, GraphQLSchema schema) {
        Introspection.addCodeForIntrospectionTypes(codeRegistry);

        TRAVERSER.depthFirst(new CodeRegistryVisitor(codeRegistry), schema.getAllTypesAsList());
    }
}
