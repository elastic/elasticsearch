package graphql.schema;

import graphql.AssertException;
import graphql.Internal;
import graphql.PublicApi;
import graphql.language.ObjectTypeDefinition;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static graphql.Assert.assertNotNull;
import static graphql.Assert.assertValidName;
import static graphql.schema.GraphqlTypeComparators.asIsOrder;
import static graphql.schema.GraphqlTypeComparators.sortTypes;
import static graphql.util.FpKit.getByName;
import static graphql.util.FpKit.valuesToList;
import static java.lang.String.format;
import static java.util.Collections.emptyList;

/**
 * This is the work horse type and represents an object with one or more field values that can be retrieved
 * by the graphql system.
 *
 * Those fields can themselves by object types and so on until you reach the leaf nodes of the type tree represented
 * by {@link graphql.schema.GraphQLScalarType}s.
 *
 * See http://graphql.org/learn/schema/#object-types-and-fields for more details on the concept.
 */
@PublicApi
public class GraphQLObjectType implements GraphQLType, GraphQLOutputType, GraphQLFieldsContainer, GraphQLCompositeType, GraphQLUnmodifiedType, GraphQLNullableType, GraphQLDirectiveContainer {


    private final String name;
    private final String description;
    private final Comparator<? super GraphQLType> interfaceComparator;
    private final Map<String, GraphQLFieldDefinition> fieldDefinitionsByName = new LinkedHashMap<>();
    private List<GraphQLOutputType> interfaces;
    private final List<GraphQLDirective> directives;
    private final ObjectTypeDefinition definition;

    /**
     * @param name             the name
     * @param description      the description
     * @param fieldDefinitions the fields
     * @param interfaces       the possible interfaces
     *
     * @deprecated use the {@link #newObject()} builder pattern instead, as this constructor will be made private in a future version.
     */
    @Internal
    @Deprecated
    public GraphQLObjectType(String name, String description, List<GraphQLFieldDefinition> fieldDefinitions,
                             List<GraphQLOutputType> interfaces) {
        this(name, description, fieldDefinitions, interfaces, emptyList(), null);
    }

    /**
     * @param name             the name
     * @param description      the description
     * @param fieldDefinitions the fields
     * @param interfaces       the possible interfaces
     * @param directives       the directives on this type element
     * @param definition       the AST definition
     *
     * @deprecated use the {@link #newObject()} builder pattern instead, as this constructor will be made private in a future version.
     */
    @Internal
    @Deprecated
    public GraphQLObjectType(String name, String description, List<GraphQLFieldDefinition> fieldDefinitions,
                             List<GraphQLOutputType> interfaces, List<GraphQLDirective> directives, ObjectTypeDefinition definition) {
        this(name, description, fieldDefinitions, interfaces, directives, definition, asIsOrder());
    }

    private GraphQLObjectType(String name, String description, List<GraphQLFieldDefinition> fieldDefinitions,
                              List<GraphQLOutputType> interfaces, List<GraphQLDirective> directives, ObjectTypeDefinition definition,
                              Comparator<? super GraphQLType> interfaceComparator) {
        assertValidName(name);
        assertNotNull(fieldDefinitions, "fieldDefinitions can't be null");
        assertNotNull(interfaces, "interfaces can't be null");
        this.name = name;
        this.description = description;
        this.interfaces = sortTypes(interfaceComparator, interfaces);
        this.definition = definition;
        this.directives = assertNotNull(directives);
        this.interfaceComparator = interfaceComparator;
        buildDefinitionMap(fieldDefinitions);
    }

    void replaceInterfaces(List<GraphQLOutputType> interfaces) {
        this.interfaces = sortTypes(interfaceComparator, interfaces);
    }

    private void buildDefinitionMap(List<GraphQLFieldDefinition> fieldDefinitions) {
        for (GraphQLFieldDefinition fieldDefinition : fieldDefinitions) {
            String name = fieldDefinition.getName();
            if (fieldDefinitionsByName.containsKey(name)) {
                throw new AssertException(format("Duplicated definition for field '%s' in type '%s'", name, this.name));
            }
            fieldDefinitionsByName.put(name, fieldDefinition);
        }
    }

    @Override
    public List<GraphQLDirective> getDirectives() {
        return new ArrayList<>(directives);
    }

    @Override
    public GraphQLFieldDefinition getFieldDefinition(String name) {
        return fieldDefinitionsByName.get(name);
    }

    @Override
    public List<GraphQLFieldDefinition> getFieldDefinitions() {
        return new ArrayList<>(fieldDefinitionsByName.values());
    }


    /**
     * @return This returns GraphQLInterface or GraphQLTypeReference instances, if the type
     * references are not resolved yet. After they are resolved it contains only GraphQLInterface.
     * Reference resolving happens when a full schema is built.
     */
    public List<GraphQLOutputType> getInterfaces() {
        return new ArrayList<>(interfaces);
    }

    public String getDescription() {
        return description;
    }


    @Override
    public String getName() {
        return name;
    }

    public ObjectTypeDefinition getDefinition() {
        return definition;
    }

    @Override
    public String toString() {
        return "GraphQLObjectType{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", fieldDefinitionsByName=" + fieldDefinitionsByName.keySet() +
                ", interfaces=" + interfaces +
                '}';
    }

    /**
     * This helps you transform the current GraphQLObjectType into another one by starting a builder with all
     * the current values and allows you to transform it how you want.
     *
     * @param builderConsumer the consumer code that will be given a builder to transform
     *
     * @return a new object based on calling build on that builder
     */
    public GraphQLObjectType transform(Consumer<Builder> builderConsumer) {
        Builder builder = newObject(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    @Override
    public TraversalControl accept(TraverserContext<GraphQLType> context, GraphQLTypeVisitor visitor) {
        return visitor.visitGraphQLObjectType(this, context);
    }

    @Override
    public List<GraphQLType> getChildren() {
        List<GraphQLType> children = new ArrayList<>(fieldDefinitionsByName.values());
        children.addAll(interfaces);
        children.addAll(directives);
        return children;
    }

    public static Builder newObject() {
        return new Builder();
    }

    public static Builder newObject(GraphQLObjectType existing) {
        return new Builder(existing);
    }

    @PublicApi
    public static class Builder extends GraphqlTypeBuilder {
        private ObjectTypeDefinition definition;
        private final Map<String, GraphQLFieldDefinition> fields = new LinkedHashMap<>();
        private final Map<String, GraphQLOutputType> interfaces = new LinkedHashMap<>();
        private final Map<String, GraphQLDirective> directives = new LinkedHashMap<>();

        public Builder() {
        }

        public Builder(GraphQLObjectType existing) {
            name = existing.getName();
            description = existing.getDescription();
            definition = existing.getDefinition();
            fields.putAll(getByName(existing.getFieldDefinitions(), GraphQLFieldDefinition::getName));
            interfaces.putAll(getByName(existing.getInterfaces(), GraphQLType::getName));
            directives.putAll(getByName(existing.getDirectives(), GraphQLDirective::getName));
        }

        @Override
        public Builder name(String name) {
            super.name(name);
            return this;
        }

        @Override
        public Builder description(String description) {
            super.description(description);
            return this;
        }

        @Override
        public Builder comparatorRegistry(GraphqlTypeComparatorRegistry comparatorRegistry) {
            super.comparatorRegistry(comparatorRegistry);
            return this;
        }

        public Builder definition(ObjectTypeDefinition definition) {
            this.definition = definition;
            return this;
        }

        public Builder field(GraphQLFieldDefinition fieldDefinition) {
            assertNotNull(fieldDefinition, "fieldDefinition can't be null");
            this.fields.put(fieldDefinition.getName(), fieldDefinition);
            return this;
        }

        /**
         * Take a field builder in a function definition and apply. Can be used in a jdk8 lambda
         * e.g.:
         * <pre>
         *     {@code
         *      field(f -> f.name("fieldName"))
         *     }
         * </pre>
         *
         * @param builderFunction a supplier for the builder impl
         *
         * @return this
         */
        public Builder field(UnaryOperator<GraphQLFieldDefinition.Builder> builderFunction) {
            assertNotNull(builderFunction, "builderFunction can't be null");
            GraphQLFieldDefinition.Builder builder = GraphQLFieldDefinition.newFieldDefinition();
            builder = builderFunction.apply(builder);
            return field(builder.build());
        }

        /**
         * Same effect as the field(GraphQLFieldDefinition). Builder.build() is called
         * from within
         *
         * @param builder an un-built/incomplete GraphQLFieldDefinition
         *
         * @return this
         */
        public Builder field(GraphQLFieldDefinition.Builder builder) {
            return field(builder.build());
        }

        public Builder fields(List<GraphQLFieldDefinition> fieldDefinitions) {
            assertNotNull(fieldDefinitions, "fieldDefinitions can't be null");
            fieldDefinitions.forEach(this::field);
            return this;
        }

        /**
         * This is used to clear all the fields in the builder so far.
         *
         * @return the builder
         */
        public Builder clearFields() {
            fields.clear();
            return this;
        }

        public boolean hasField(String fieldName) {
            return fields.containsKey(fieldName);
        }


        public Builder withInterface(GraphQLInterfaceType interfaceType) {
            assertNotNull(interfaceType, "interfaceType can't be null");
            this.interfaces.put(interfaceType.getName(), interfaceType);
            return this;
        }

        public Builder withInterface(GraphQLTypeReference reference) {
            assertNotNull(reference, "reference can't be null");
            this.interfaces.put(reference.getName(), reference);
            return this;
        }

        public Builder withInterfaces(GraphQLInterfaceType... interfaceType) {
            for (GraphQLInterfaceType type : interfaceType) {
                withInterface(type);
            }
            return this;
        }

        public Builder withInterfaces(GraphQLTypeReference... references) {
            for (GraphQLTypeReference reference : references) {
                withInterface(reference);
            }
            return this;
        }

        /**
         * This is used to clear all the interfaces in the builder so far.
         *
         * @return the builder
         */
        public Builder clearInterfaces() {
            interfaces.clear();
            return this;
        }


        public Builder withDirectives(GraphQLDirective... directives) {
            for (GraphQLDirective directive : directives) {
                withDirective(directive);
            }
            return this;
        }

        public Builder withDirective(GraphQLDirective directive) {
            assertNotNull(directive, "directive can't be null");
            directives.put(directive.getName(), directive);
            return this;
        }

        public Builder withDirective(GraphQLDirective.Builder builder) {
            return withDirective(builder.build());
        }

        /**
         * This is used to clear all the directives in the builder so far.
         *
         * @return the builder
         */
        public Builder clearDirectives() {
            directives.clear();
            return this;
        }

        public GraphQLObjectType build() {
            return new GraphQLObjectType(
                    name,
                    description,
                    sort(fields, GraphQLObjectType.class, GraphQLFieldDefinition.class),
                    valuesToList(interfaces),
                    sort(directives, GraphQLObjectType.class, GraphQLDirective.class),
                    definition,
                    getComparator(GraphQLObjectType.class, GraphQLInterfaceType.class)
            );
        }
    }
}
