package graphql.schema;

import graphql.Internal;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static graphql.util.FpKit.valuesToList;

@Internal
public abstract class GraphqlTypeBuilder {

    protected String name;
    protected String description;
    protected GraphqlTypeComparatorRegistry comparatorRegistry = GraphqlTypeComparatorRegistry.AS_IS_REGISTRY;

    GraphqlTypeBuilder name(String name) {
        this.name = name;
        return this;
    }

    GraphqlTypeBuilder description(String description) {
        this.description = description;
        return this;
    }

    GraphqlTypeBuilder comparatorRegistry(GraphqlTypeComparatorRegistry comparatorRegistry) {
        this.comparatorRegistry = comparatorRegistry;
        return this;
    }


    <T extends GraphQLType> List<T> sort(Map<String, T> types, Class<? extends GraphQLType> parentType, Class<? extends GraphQLType> elementType) {
        return sort(valuesToList(types), parentType, elementType);
    }

    <T extends GraphQLType> List<T> sort(List<T> types, Class<? extends GraphQLType> parentType, Class<? extends GraphQLType> elementType) {
        Comparator<? super GraphQLType> comparator = getComparatorImpl(comparatorRegistry, parentType, elementType);
        return GraphqlTypeComparators.sortTypes(comparator, types);
    }

    Comparator<? super GraphQLType> getComparator(Class<? extends GraphQLType> parentType, Class<? extends GraphQLType> elementType) {
        return getComparatorImpl(comparatorRegistry, parentType, elementType);
    }

    private static Comparator<? super GraphQLType> getComparatorImpl(GraphqlTypeComparatorRegistry comparatorRegistry, Class<? extends GraphQLType> parentType, Class<? extends GraphQLType> elementType) {
        GraphqlTypeComparatorEnvironment environment = GraphqlTypeComparatorEnvironment.newEnvironment()
                .parentType(parentType)
                .elementType(elementType)
                .build();
        return comparatorRegistry.getComparator(environment);
    }
}