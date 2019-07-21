package graphql.schema;

import graphql.PublicApi;
import graphql.schema.visibility.GraphqlFieldVisibility;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import static graphql.Assert.assertNotNull;
import static graphql.Assert.assertTrue;
import static graphql.Assert.assertValidName;
import static graphql.schema.DataFetcherFactoryEnvironment.newDataFetchingFactoryEnvironment;
import static graphql.schema.FieldCoordinates.coordinates;
import static graphql.schema.visibility.DefaultGraphqlFieldVisibility.DEFAULT_FIELD_VISIBILITY;


/**
 * The {@link graphql.schema.GraphQLCodeRegistry} holds that execution code that is associated with graphql types, namely
 * the {@link graphql.schema.DataFetcher}s associated with fields, the {@link graphql.schema.TypeResolver}s associated with
 * abstract types and the {@link graphql.schema.visibility.GraphqlFieldVisibility}
 *
 * For legacy reasons these code functions can still exist on the original type objects but this will be removed in a future version.  Once
 * removed the type system objects will be able have proper hashCode/equals methods and be checked for proper equality.
 */
@PublicApi
public class GraphQLCodeRegistry {

    private final Map<FieldCoordinates, DataFetcherFactory> dataFetcherMap;
    private final Map<String, DataFetcherFactory> systemDataFetcherMap;
    private final Map<String, TypeResolver> typeResolverMap;
    private final GraphqlFieldVisibility fieldVisibility;

    private GraphQLCodeRegistry(Map<FieldCoordinates, DataFetcherFactory> dataFetcherMap, Map<String, DataFetcherFactory> systemDataFetcherMap, Map<String, TypeResolver> typeResolverMap, GraphqlFieldVisibility fieldVisibility) {
        this.dataFetcherMap = dataFetcherMap;
        this.systemDataFetcherMap = systemDataFetcherMap;
        this.typeResolverMap = typeResolverMap;
        this.fieldVisibility = fieldVisibility;
    }

    /**
     * @return the {@link graphql.schema.visibility.GraphqlFieldVisibility}
     */
    public GraphqlFieldVisibility getFieldVisibility() {
        return fieldVisibility;
    }

    public GraphQLCodeRegistryGetDataFetcherHook getDataFetcherHook;

    /**
     * Returns a data fetcher associated with a field within a container type
     *
     * @param parentType      the container type
     * @param fieldDefinition the field definition
     *
     * @return the DataFetcher associated with this field.  All fields have data fetchers
     */
    public DataFetcher getDataFetcher(GraphQLFieldsContainer parentType, GraphQLFieldDefinition fieldDefinition) {
        FieldCoordinates coords = FieldCoordinates.coordinates(parentType, fieldDefinition);
        if (getDataFetcherHook != null) {
            DataFetcher<?> fetcher = getDataFetcherHook.getDataFetcherHook(coords);
            if (fetcher != null) return fetcher;
        }
        return getDataFetcherImpl(coords, fieldDefinition, dataFetcherMap, systemDataFetcherMap);
    }

    /**
     * Returns a data fetcher associated with a field located at specified coordinates.
     *
     * @param coordinates     the field coordinates
     * @param fieldDefinition the field definition
     *
     * @return the DataFetcher associated with this field.  All fields have data fetchers
     */
    public DataFetcher getDataFetcher(FieldCoordinates coordinates, GraphQLFieldDefinition fieldDefinition) {
        if (getDataFetcherHook != null) {
            DataFetcher<?> fetcher = getDataFetcherHook.getDataFetcherHook(coordinates);
            if (fetcher != null) return fetcher;
        }
        return getDataFetcherImpl(coordinates, fieldDefinition, dataFetcherMap, systemDataFetcherMap);
    }

    private static DataFetcher getDataFetcherImpl(FieldCoordinates coordinates, GraphQLFieldDefinition fieldDefinition, Map<FieldCoordinates, DataFetcherFactory> dataFetcherMap, Map<String, DataFetcherFactory> systemDataFetcherMap) {
        assertNotNull(coordinates);
        assertNotNull(fieldDefinition);

        DataFetcherFactory dataFetcherFactory = systemDataFetcherMap.get(fieldDefinition.getName());
        if (dataFetcherFactory == null) {
            dataFetcherFactory = dataFetcherMap.get(coordinates);
            if (dataFetcherFactory == null) {
                dataFetcherFactory = DataFetcherFactories.useDataFetcher(new PropertyDataFetcher<>(fieldDefinition.getName()));
            }
        }
        return dataFetcherFactory.get(newDataFetchingFactoryEnvironment()
                .fieldDefinition(fieldDefinition)
                .build());
    }

    private static boolean hasDataFetcherImpl(FieldCoordinates coords, Map<FieldCoordinates, DataFetcherFactory> dataFetcherMap, Map<String, DataFetcherFactory> systemDataFetcherMap) {
        assertNotNull(coords);

        DataFetcherFactory dataFetcherFactory = systemDataFetcherMap.get(coords.getFieldName());
        if (dataFetcherFactory == null) {
            dataFetcherFactory = dataFetcherMap.get(coords);
        }
        return dataFetcherFactory != null;
    }


    /**
     * Returns the type resolver associated with this interface type
     *
     * @param interfaceType the interface type
     *
     * @return a non null {@link graphql.schema.TypeResolver}
     */
    public TypeResolver getTypeResolver(GraphQLInterfaceType interfaceType) {
        return getTypeResolverForInterface(interfaceType, typeResolverMap);
    }

    /**
     * Returns the type resolver associated with this union type
     *
     * @param unionType the union type
     *
     * @return a non null {@link graphql.schema.TypeResolver}
     */

    public TypeResolver getTypeResolver(GraphQLUnionType unionType) {
        return getTypeResolverForUnion(unionType, typeResolverMap);
    }

    private static TypeResolver getTypeResolverForInterface(GraphQLInterfaceType parentType, Map<String, TypeResolver> typeResolverMap) {
        assertNotNull(parentType);
        TypeResolver typeResolver = typeResolverMap.get(parentType.getName());
        if (typeResolver == null) {
            typeResolver = parentType.getTypeResolver();
        }
        return assertNotNull(typeResolver, "There must be a type resolver for interface " + parentType.getName());
    }

    private static TypeResolver getTypeResolverForUnion(GraphQLUnionType parentType, Map<String, TypeResolver> typeResolverMap) {
        assertNotNull(parentType);
        TypeResolver typeResolver = typeResolverMap.get(parentType.getName());
        if (typeResolver == null) {
            typeResolver = parentType.getTypeResolver();
        }
        return assertNotNull(typeResolver, "There must be a type resolver for union " + parentType.getName());
    }

    /**
     * This helps you transform the current {@link graphql.schema.GraphQLCodeRegistry} object into another one by starting a builder with all
     * the current values and allows you to transform it how you want.
     *
     * @param builderConsumer the consumer code that will be given a builder to transform
     *
     * @return a new GraphQLCodeRegistry object based on calling build on that builder
     */
    public GraphQLCodeRegistry transform(Consumer<Builder> builderConsumer) {
        Builder builder = newCodeRegistry(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    /**
     * @return a new builder of {@link graphql.schema.GraphQLCodeRegistry} objects
     */
    public static Builder newCodeRegistry() {
        return new Builder();
    }

    /**
     * Returns a new builder of {@link graphql.schema.GraphQLCodeRegistry} objects based on the existing one
     *
     * @param existingCodeRegistry the existing code registry to use
     *
     * @return a new builder of {@link graphql.schema.GraphQLCodeRegistry} objects
     */
    public static Builder newCodeRegistry(GraphQLCodeRegistry existingCodeRegistry) {
        return new Builder(existingCodeRegistry);
    }

    public static class Builder {
        private final Map<FieldCoordinates, DataFetcherFactory> dataFetcherMap = new LinkedHashMap<>();
        private final Map<String, DataFetcherFactory> systemDataFetcherMap = new LinkedHashMap<>();
        private final Map<String, TypeResolver> typeResolverMap = new HashMap<>();
        private GraphqlFieldVisibility fieldVisibility = DEFAULT_FIELD_VISIBILITY;


        private Builder() {
        }

        private Builder(GraphQLCodeRegistry codeRegistry) {
            this.dataFetcherMap.putAll(codeRegistry.dataFetcherMap);
            this.typeResolverMap.putAll(codeRegistry.typeResolverMap);
            this.fieldVisibility = codeRegistry.fieldVisibility;
        }

        /**
         * Returns a data fetcher associated with a field within a container type
         *
         * @param parentType      the container type
         * @param fieldDefinition the field definition
         *
         * @return the DataFetcher associated with this field.  All fields have data fetchers
         */
        public DataFetcher getDataFetcher(GraphQLFieldsContainer parentType, GraphQLFieldDefinition fieldDefinition) {
            return getDataFetcherImpl(FieldCoordinates.coordinates(parentType, fieldDefinition), fieldDefinition, dataFetcherMap, systemDataFetcherMap);
        }

        /**
         * Returns a data fetcher associated with a field located at specified coordinates.
         *
         * @param coordinates     the field coordinates
         * @param fieldDefinition the field definition
         *
         * @return the DataFetcher associated with this field.  All fields have data fetchers
         */
        public DataFetcher getDataFetcher(FieldCoordinates coordinates, GraphQLFieldDefinition fieldDefinition) {
            return getDataFetcherImpl(coordinates, fieldDefinition, dataFetcherMap, systemDataFetcherMap);
        }

        /**
         * Returns a data fetcher associated with a field within a container type
         *
         * @param coordinates the field coordinates
         *
         * @return the true if there is a data fetcher already for this field
         */
        public boolean hasDataFetcher(FieldCoordinates coordinates) {
            return hasDataFetcherImpl(coordinates, dataFetcherMap, systemDataFetcherMap);
        }

        /**
         * Returns the type resolver associated with this interface type
         *
         * @param interfaceType the interface type
         *
         * @return a non null {@link graphql.schema.TypeResolver}
         */
        public TypeResolver getTypeResolver(GraphQLInterfaceType interfaceType) {
            return getTypeResolverForInterface(interfaceType, typeResolverMap);
        }

        /**
         * Returns true of a type resolver has been registered for this type name
         *
         * @param typeName the name to check
         *
         * @return true if there is already a type resolver
         */
        public boolean hasTypeResolver(String typeName) {
            return typeResolverMap.containsKey(typeName);
        }

        /**
         * Returns the type resolver associated with this union type
         *
         * @param unionType the union type
         *
         * @return a non null {@link graphql.schema.TypeResolver}
         */
        public TypeResolver getTypeResolver(GraphQLUnionType unionType) {
            return getTypeResolverForUnion(unionType, typeResolverMap);
        }

        /**
         * Sets the data fetcher for a specific field inside a container type
         *
         * @param coordinates the field coordinates
         * @param dataFetcher the data fetcher code for that field
         *
         * @return this builder
         */
        public Builder dataFetcher(FieldCoordinates coordinates, DataFetcher<?> dataFetcher) {
            assertNotNull(dataFetcher);
            return dataFetcher(assertNotNull(coordinates), DataFetcherFactories.useDataFetcher(dataFetcher));
        }

        /**
         * Sets the data fetcher for a specific field inside a container type
         *
         * @param parentType      the container type
         * @param fieldDefinition the field definition
         * @param dataFetcher     the data fetcher code for that field
         *
         * @return this builder
         */
        public Builder dataFetcher(GraphQLFieldsContainer parentType, GraphQLFieldDefinition fieldDefinition, DataFetcher<?> dataFetcher) {
            return dataFetcher(FieldCoordinates.coordinates(parentType.getName(), fieldDefinition.getName()), dataFetcher);
        }

        /**
         * Called to place system data fetchers (eg Introspection fields) into the mix
         *
         * @param coordinates the field coordinates
         * @param dataFetcher the data fetcher code for that field
         *
         * @return this builder
         */
        public Builder systemDataFetcher(FieldCoordinates coordinates, DataFetcher<?> dataFetcher) {
            assertNotNull(dataFetcher);
            assertNotNull(coordinates);
            assertTrue(coordinates.getFieldName().startsWith("__"), "Only __ system fields can be used here");
            systemDataFetcherMap.put(coordinates.getFieldName(), DataFetcherFactories.useDataFetcher(dataFetcher));
            return this;
        }

        /**
         * Sets the data fetcher factory for a specific field inside a container type
         *
         * @param coordinates        the field coordinates
         * @param dataFetcherFactory the data fetcher factory code for that field
         *
         * @return this builder
         */
        public Builder dataFetcher(FieldCoordinates coordinates, DataFetcherFactory<?> dataFetcherFactory) {
            assertNotNull(dataFetcherFactory);
            dataFetcherMap.put(assertNotNull(coordinates), dataFetcherFactory);
            return this;
        }

        /**
         * Sets the data fetcher factory for a specific field inside a container type ONLY if not mapping has already been made
         *
         * @param coordinates the field coordinates
         * @param dataFetcher the data fetcher code for that field
         *
         * @return this builder
         */
        public Builder dataFetcherIfAbsent(FieldCoordinates coordinates, DataFetcher<?> dataFetcher) {
            dataFetcherMap.putIfAbsent(assertNotNull(coordinates), DataFetcherFactories.useDataFetcher(dataFetcher));
            return this;
        }

        /**
         * This allows you you to build all the data fetchers for the fields of a container type.
         *
         * @param parentTypeName    the parent container type
         * @param fieldDataFetchers the map of field names to data fetchers
         *
         * @return this builder
         */
        public Builder dataFetchers(String parentTypeName, Map<String, DataFetcher> fieldDataFetchers) {
            assertNotNull(fieldDataFetchers);
            fieldDataFetchers.forEach((fieldName, dataFetcher) -> {
                dataFetcher(coordinates(parentTypeName, fieldName), dataFetcher);
            });
            return this;
        }

        public Builder dataFetchers(GraphQLCodeRegistry codeRegistry) {
            this.dataFetcherMap.putAll(codeRegistry.dataFetcherMap);
            return this;
        }

        public Builder typeResolver(GraphQLInterfaceType interfaceType, TypeResolver typeResolver) {
            typeResolverMap.put(interfaceType.getName(), typeResolver);
            return this;
        }

        public Builder typeResolverIfAbsent(GraphQLInterfaceType interfaceType, TypeResolver typeResolver) {
            typeResolverMap.putIfAbsent(interfaceType.getName(), typeResolver);
            return this;
        }

        public Builder typeResolver(GraphQLUnionType unionType, TypeResolver typeResolver) {
            typeResolverMap.put(unionType.getName(), typeResolver);
            return this;
        }

        public Builder typeResolverIfAbsent(GraphQLUnionType unionType, TypeResolver typeResolver) {
            typeResolverMap.putIfAbsent(unionType.getName(), typeResolver);
            return this;
        }

        public Builder typeResolver(String typeName, TypeResolver typeResolver) {
            typeResolverMap.put(assertValidName(typeName), typeResolver);
            return this;
        }

        public Builder typeResolvers(GraphQLCodeRegistry codeRegistry) {
            this.typeResolverMap.putAll(codeRegistry.typeResolverMap);
            return this;
        }

        public Builder fieldVisibility(GraphqlFieldVisibility fieldVisibility) {
            this.fieldVisibility = assertNotNull(fieldVisibility);
            return this;
        }

        public Builder clearDataFetchers() {
            dataFetcherMap.clear();
            return this;
        }

        public Builder clearTypeResolvers() {
            typeResolverMap.clear();
            return this;
        }

        public GraphQLCodeRegistry build() {
            return new GraphQLCodeRegistry(dataFetcherMap, systemDataFetcherMap, typeResolverMap, fieldVisibility);
        }
    }
}
