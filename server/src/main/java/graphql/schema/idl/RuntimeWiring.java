package graphql.schema.idl;

import graphql.PublicApi;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.SchemaTransformer;
import graphql.schema.TypeResolver;
import graphql.schema.visibility.GraphqlFieldVisibility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static graphql.Assert.assertNotNull;
import static graphql.schema.visibility.DefaultGraphqlFieldVisibility.DEFAULT_FIELD_VISIBILITY;

/**
 * A runtime wiring is a specification of data fetchers, type resolvers and custom scalars that are needed
 * to wire together a functional {@link GraphQLSchema}
 */
@PublicApi
public class RuntimeWiring {

    private final Map<String, Map<String, DataFetcher>> dataFetchers;
    private final Map<String, DataFetcher> defaultDataFetchers;
    private final Map<String, GraphQLScalarType> scalars;
    private final Map<String, TypeResolver> typeResolvers;
    private final Map<String, SchemaDirectiveWiring> registeredDirectiveWiring;
    private final List<SchemaDirectiveWiring> directiveWiring;
    private final WiringFactory wiringFactory;
    private final Map<String, EnumValuesProvider> enumValuesProviders;
    private final Collection<SchemaTransformer> schemaTransformers;
    private final GraphqlFieldVisibility fieldVisibility;
    private final GraphQLCodeRegistry codeRegistry;
    private final GraphqlTypeComparatorRegistry comparatorRegistry;

    private RuntimeWiring(Builder builder) {
        this.dataFetchers = builder.dataFetchers;
        this.defaultDataFetchers = builder.defaultDataFetchers;
        this.scalars = builder.scalars;
        this.typeResolvers = builder.typeResolvers;
        this.registeredDirectiveWiring = builder.registeredDirectiveWiring;
        this.directiveWiring = builder.directiveWiring;
        this.wiringFactory = builder.wiringFactory;
        this.enumValuesProviders = builder.enumValuesProviders;
        this.schemaTransformers = builder.schemaTransformers;
        this.fieldVisibility = builder.fieldVisibility;
        this.codeRegistry = builder.codeRegistry;
        this.comparatorRegistry = builder.comparatorRegistry;
    }

    /**
     * @return a builder of Runtime Wiring
     */
    public static Builder newRuntimeWiring() {
        return new Builder();
    }

    public GraphQLCodeRegistry getCodeRegistry() {
        return codeRegistry;
    }

    public Map<String, GraphQLScalarType> getScalars() {
        return new LinkedHashMap<>(scalars);
    }

    public Map<String, Map<String, DataFetcher>> getDataFetchers() {
        return dataFetchers;
    }

    public Map<String, DataFetcher> getDataFetcherForType(String typeName) {
        return dataFetchers.computeIfAbsent(typeName, k -> new LinkedHashMap<>());
    }

    public DataFetcher getDefaultDataFetcherForType(String typeName) {
        return defaultDataFetchers.get(typeName);
    }

    public Map<String, TypeResolver> getTypeResolvers() {
        return typeResolvers;
    }

    public Map<String, EnumValuesProvider> getEnumValuesProviders() {
        return this.enumValuesProviders;
    }

    public WiringFactory getWiringFactory() {
        return wiringFactory;
    }

    public GraphqlFieldVisibility getFieldVisibility() {
        return fieldVisibility;
    }

    public Map<String, SchemaDirectiveWiring> getRegisteredDirectiveWiring() {
        return registeredDirectiveWiring;
    }

    public List<SchemaDirectiveWiring> getDirectiveWiring() {
        return directiveWiring;
    }

    public Collection<SchemaTransformer> getSchemaTransformers() {
        return schemaTransformers;
    }

    public GraphqlTypeComparatorRegistry getComparatorRegistry() {
        return comparatorRegistry;
    }

    @PublicApi
    public static class Builder {
        private final Map<String, Map<String, DataFetcher>> dataFetchers = new LinkedHashMap<>();
        private final Map<String, DataFetcher> defaultDataFetchers = new LinkedHashMap<>();
        private final Map<String, GraphQLScalarType> scalars = new LinkedHashMap<>();
        private final Map<String, TypeResolver> typeResolvers = new LinkedHashMap<>();
        private final Map<String, EnumValuesProvider> enumValuesProviders = new LinkedHashMap<>();
        private final Map<String, SchemaDirectiveWiring> registeredDirectiveWiring = new LinkedHashMap<>();
        private final List<SchemaDirectiveWiring> directiveWiring = new ArrayList<>();
        private final Collection<SchemaTransformer> schemaTransformers = new ArrayList<>();
        private WiringFactory wiringFactory = new NoopWiringFactory();
        private GraphqlFieldVisibility fieldVisibility = DEFAULT_FIELD_VISIBILITY;
        private GraphQLCodeRegistry codeRegistry = GraphQLCodeRegistry.newCodeRegistry().build();
        private GraphqlTypeComparatorRegistry comparatorRegistry = GraphqlTypeComparatorRegistry.AS_IS_REGISTRY;

        private Builder() {
            ScalarInfo.STANDARD_SCALARS.forEach(this::scalar);
            // we give this out by default
            registeredDirectiveWiring.put(FetchSchemaDirectiveWiring.FETCH, new FetchSchemaDirectiveWiring());
        }

        /**
         * Adds a wiring factory into the runtime wiring
         *
         * @param wiringFactory the wiring factory to add
         *
         * @return this outer builder
         */
        public Builder wiringFactory(WiringFactory wiringFactory) {
            assertNotNull(wiringFactory, "You must provide a wiring factory");
            this.wiringFactory = wiringFactory;
            return this;
        }

        /**
         * This allows you to seed in your own {@link graphql.schema.GraphQLCodeRegistry} instance
         *
         * @param codeRegistry the code registry to use
         *
         * @return this outer builder
         */
        public Builder codeRegistry(GraphQLCodeRegistry codeRegistry) {
            this.codeRegistry = assertNotNull(codeRegistry);
            return this;
        }

        /**
         * This allows you to seed in your own {@link graphql.schema.GraphQLCodeRegistry} instance
         *
         * @param codeRegistry the code registry to use
         *
         * @return this outer builder
         */
        public Builder codeRegistry(GraphQLCodeRegistry.Builder codeRegistry) {
            this.codeRegistry = assertNotNull(codeRegistry).build();
            return this;
        }

        /**
         * This allows you to add in new custom Scalar implementations beyond the standard set.
         *
         * @param scalarType the new scalar implementation
         *
         * @return the runtime wiring builder
         */
        public Builder scalar(GraphQLScalarType scalarType) {
            scalars.put(scalarType.getName(), scalarType);
            return this;
        }

        /**
         * This allows you to add a field visibility that will be associated with the schema
         *
         * @param fieldVisibility the new field visibility
         *
         * @return the runtime wiring builder
         */
        public Builder fieldVisibility(GraphqlFieldVisibility fieldVisibility) {
            this.fieldVisibility = assertNotNull(fieldVisibility);
            return this;
        }

        /**
         * This allows you to add a new type wiring via a builder
         *
         * @param builder the type wiring builder to use
         *
         * @return this outer builder
         */
        public Builder type(TypeRuntimeWiring.Builder builder) {
            return type(builder.build());
        }

        /**
         * This form allows a lambda to be used as the builder of a type wiring
         *
         * @param typeName        the name of the type to wire
         * @param builderFunction a function that will be given the builder to use
         *
         * @return the runtime wiring builder
         */
        public Builder type(String typeName, UnaryOperator<TypeRuntimeWiring.Builder> builderFunction) {
            TypeRuntimeWiring.Builder builder = builderFunction.apply(TypeRuntimeWiring.newTypeWiring(typeName));
            return type(builder.build());
        }

        /**
         * This adds a type wiring
         *
         * @param typeRuntimeWiring the new type wiring
         *
         * @return the runtime wiring builder
         */
        public Builder type(TypeRuntimeWiring typeRuntimeWiring) {
            String typeName = typeRuntimeWiring.getTypeName();
            Map<String, DataFetcher> typeDataFetchers = dataFetchers.computeIfAbsent(typeName, k -> new LinkedHashMap<>());
            typeRuntimeWiring.getFieldDataFetchers().forEach(typeDataFetchers::put);

            defaultDataFetchers.put(typeName, typeRuntimeWiring.getDefaultDataFetcher());

            TypeResolver typeResolver = typeRuntimeWiring.getTypeResolver();
            if (typeResolver != null) {
                this.typeResolvers.put(typeName, typeResolver);
            }

            EnumValuesProvider enumValuesProvider = typeRuntimeWiring.getEnumValuesProvider();
            if (enumValuesProvider != null) {
                this.enumValuesProviders.put(typeName, enumValuesProvider);
            }
            return this;
        }

        /**
         * This provides the wiring code for a named directive.
         * <p>
         * Note: The provided directive wiring will ONLY be called back if an element has a directive
         * with the specified name.
         * <p>
         * To be called back for every directive the use {@link #directiveWiring(SchemaDirectiveWiring)} or
         * use {@link graphql.schema.idl.WiringFactory#providesSchemaDirectiveWiring(SchemaDirectiveWiringEnvironment)}
         * instead.
         *
         * @param directiveName         the name of the directive to wire
         * @param schemaDirectiveWiring the runtime behaviour of this wiring
         *
         * @return the runtime wiring builder
         *
         * @see #directiveWiring(SchemaDirectiveWiring)
         * @see graphql.schema.idl.SchemaDirectiveWiring
         * @see graphql.schema.idl.WiringFactory#providesSchemaDirectiveWiring(SchemaDirectiveWiringEnvironment)
         */
        public Builder directive(String directiveName, SchemaDirectiveWiring schemaDirectiveWiring) {
            registeredDirectiveWiring.put(directiveName, schemaDirectiveWiring);
            return this;
        }

        /**
         * This adds a directive wiring that will be called for all directives.
         * <p>
         * Note : Unlike {@link #directive(String, SchemaDirectiveWiring)} which is only called back if a  named
         * directives is present, this directive wiring will be called back for every element
         * in the schema even if it has zero directives.
         *
         * @param schemaDirectiveWiring the runtime behaviour of this wiring
         *
         * @return the runtime wiring builder
         *
         * @see #directive(String, SchemaDirectiveWiring)
         * @see graphql.schema.idl.SchemaDirectiveWiring
         * @see graphql.schema.idl.WiringFactory#providesSchemaDirectiveWiring(SchemaDirectiveWiringEnvironment)
         */
        public Builder directiveWiring(SchemaDirectiveWiring schemaDirectiveWiring) {
            directiveWiring.add(schemaDirectiveWiring);
            return this;
        }

        /**
         * You can specify your own sort order of graphql types via {@link graphql.schema.GraphqlTypeComparatorRegistry}
         * which will tell you what type of objects you are to sort when
         * it asks for a comparator.
         *
         * @return the runtime wiring builder
         */
        public Builder comparatorRegistry(GraphqlTypeComparatorRegistry comparatorRegistry) {
            this.comparatorRegistry = comparatorRegistry;
            return this;
        }

        /**
         * Adds a schema transformer into the mix
         *
         * @param schemaTransformer the non null schema transformer to add
         *
         * @return the runtime wiring builder
         */
        public Builder transformer(SchemaTransformer schemaTransformer) {
            this.schemaTransformers.add(assertNotNull(schemaTransformer));
            return this;
        }

        /**
         * @return the built runtime wiring
         */
        public RuntimeWiring build() {
            return new RuntimeWiring(this);
        }

    }
}

