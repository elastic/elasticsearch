package graphql.schema;

import graphql.PublicApi;

import java.util.Comparator;

@PublicApi
public interface GraphqlTypeComparatorRegistry {

    /**
     * A registry that leaves the elements as there currently are
     */
    GraphqlTypeComparatorRegistry AS_IS_REGISTRY = new GraphqlTypeComparatorRegistry() {
        @Override
        public <T extends GraphQLType> Comparator<? super T> getComparator(GraphqlTypeComparatorEnvironment environment) {
            return GraphqlTypeComparators.asIsOrder();
        }
    };


    /**
     * A registry that sorts the elements by their name ascending
     */
    GraphqlTypeComparatorRegistry BY_NAME_REGISTRY = new GraphqlTypeComparatorRegistry() {
        @Override
        public <T extends GraphQLType> Comparator<? super T> getComparator(GraphqlTypeComparatorEnvironment environment) {
            return GraphqlTypeComparators.byNameAsc();
        }
    };


    /**
     * @param environment Defines the scope to control where the {@code Comparator} can be applied.
     * @param <T>         the type of the comparator
     *
     * @return The registered {@code Comparator} or {@code null} if not found.
     */
    <T extends GraphQLType> Comparator<? super T> getComparator(GraphqlTypeComparatorEnvironment environment);
}
