package graphql.schema;

import graphql.PublicApi;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import static graphql.Assert.assertNotNull;
import static graphql.schema.GraphqlTypeComparatorEnvironment.newEnvironment;

/**
 * Associates a {@code Comparator} with a {@code GraphqlTypeComparatorEnvironment} to control the scope in which the {@code Comparator} can be applied.
 */
@PublicApi
public class DefaultGraphqlTypeComparatorRegistry implements GraphqlTypeComparatorRegistry {

    public static final Comparator<GraphQLType> DEFAULT_COMPARATOR = Comparator.comparing(GraphQLType::getName);

    private Map<GraphqlTypeComparatorEnvironment, Comparator<?>> registry = new HashMap<>();

    private DefaultGraphqlTypeComparatorRegistry() {
    }

    private DefaultGraphqlTypeComparatorRegistry(Map<GraphqlTypeComparatorEnvironment, Comparator<?>> registry) {
        this.registry = registry;
    }

    /**
     * Search for the most to least specific registered {@code Comparator} otherwise a default is returned.
     */
    @Override
    public <T extends GraphQLType> Comparator<? super T> getComparator(GraphqlTypeComparatorEnvironment environment) {
        Comparator<?> comparator = registry.get(environment);
        if (comparator != null) {
            //noinspection unchecked
            return (Comparator<? super T>) comparator;
        }
        comparator = registry.get(environment.transform(builder -> builder.parentType(null)));
        if (comparator != null) {
            //noinspection unchecked
            return (Comparator<? super T>) comparator;
        }
        return DEFAULT_COMPARATOR;
    }

    /**
     * @return A registry where all {@code GraphQLType}s receive a default {@code Comparator} by comparing {@code GraphQLType::getName}.
     */
    public static GraphqlTypeComparatorRegistry defaultComparators() {
        return new DefaultGraphqlTypeComparatorRegistry();
    }

    public static Builder newComparators() {
        return new Builder();
    }

    public static class Builder {

        private Map<GraphqlTypeComparatorEnvironment, Comparator<?>> registry = new HashMap<>();

        /**
         * Registers a {@code Comparator} with an environment to control its permitted scope of operation.
         *
         * @param environment     Defines the scope to control where the {@code Comparator} can be applied.
         * @param comparatorClass The {@code Comparator} class for added type safety. It should match {@code environment.elementType}.
         * @param comparator      The {@code Comparator} of type {@code comparatorClass}.
         * @param <T>             The specific {@code GraphQLType} the {@code Comparator} should operate on.
         *
         * @return The {@code Builder} instance to allow chaining.
         */
        public <T extends GraphQLType> Builder addComparator(GraphqlTypeComparatorEnvironment environment, Class<T> comparatorClass, Comparator<? super T> comparator) {
            assertNotNull(environment, "environment can't be null");
            assertNotNull(comparatorClass, "comparatorClass can't be null");
            assertNotNull(comparator, "comparator can't be null");
            registry.put(environment, comparator);
            return this;
        }

        /**
         * Convenience method which supplies an environment builder function.
         *
         * @param builderFunction the function which is given a builder
         * @param comparatorClass The {@code Comparator} class for added type safety. It should match {@code environment.elementType}.
         * @param comparator      The {@code Comparator} of type {@code comparatorClass}.
         * @param <T>             the graphql type
         *
         * @return this builder
         *
         * @see #addComparator
         */
        public <T extends GraphQLType> Builder addComparator(UnaryOperator<GraphqlTypeComparatorEnvironment.Builder> builderFunction,
                                                             Class<T> comparatorClass, Comparator<? super T> comparator) {
            assertNotNull(builderFunction, "builderFunction can't be null");

            GraphqlTypeComparatorEnvironment environment = builderFunction.apply(newEnvironment()).build();
            return addComparator(environment, comparatorClass, comparator);
        }

        public DefaultGraphqlTypeComparatorRegistry build() {
            return new DefaultGraphqlTypeComparatorRegistry(registry);
        }
    }
}
