package graphql.schema;


import graphql.Internal;
import graphql.PublicApi;
import graphql.language.InputValueDefinition;
import graphql.util.FpKit;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static graphql.Assert.assertNotNull;
import static graphql.Assert.assertValidName;

/**
 * This defines an argument that can be supplied to a graphql field (via {@link graphql.schema.GraphQLFieldDefinition}.
 *
 * Fields can be thought of as "functions" that take arguments and return a value.
 *
 * See http://graphql.org/learn/queries/#arguments for more details on the concept.
 *
 * {@link graphql.schema.GraphQLArgument} is used in two contexts, one context is graphql queries where it represents the arguments that can be
 * set on a field and the other is in Schema Definition Language (SDL) where it can be used to represent the argument value instances
 * that have been supplied on a {@link graphql.schema.GraphQLDirective}.
 *
 * The difference is the 'value' and 'defaultValue' properties.  In a query argument, the 'value' is never in the GraphQLArgument
 * object but rather in the AST direct or in the query variables map and the 'defaultValue' represents a value to use if both of these are
 * not present. You can think of them like a descriptor of what shape an argument might have.
 *
 * However with directives on SDL elements, the value is specified in AST only and transferred into the GraphQLArgument object and the
 * 'defaultValue' comes instead from the directive definition elsewhere in the SDL.  You can think of them as 'instances' of arguments, their shape and their
 * specific value on that directive.
 */
@PublicApi
public class GraphQLArgument implements GraphQLDirectiveContainer {

    private final String name;
    private final String description;
    private GraphQLInputType type;
    private final Object value;
    private final Object defaultValue;
    private final InputValueDefinition definition;
    private final List<GraphQLDirective> directives;

    /**
     * @param name         the arg name
     * @param description  the arg description
     * @param type         the arg type
     * @param defaultValue the default value
     *
     * @deprecated use the {@link #newArgument()} builder pattern instead, as this constructor will be made private in a future version.
     */
    @Internal
    @Deprecated
    public GraphQLArgument(String name, String description, GraphQLInputType type, Object defaultValue) {
        this(name, description, type, defaultValue, null);
    }

    /**
     * @param name the arg name
     * @param type the arg type
     *
     * @deprecated use the {@link #newArgument()} builder pattern instead, as this constructor will be made private in a future version.
     */
    @Internal
    @Deprecated
    public GraphQLArgument(String name, GraphQLInputType type) {
        this(name, null, type, null, null);
    }

    /**
     * @param name         the arg name
     * @param description  the arg description
     * @param type         the arg type
     * @param defaultValue the default value
     * @param definition   the AST definition
     *
     * @deprecated use the {@link #newArgument()} builder pattern instead, as this constructor will be made private in a future version.
     */
    public GraphQLArgument(String name, String description, GraphQLInputType type, Object defaultValue, InputValueDefinition definition) {
        this(name, description, type, defaultValue, null, definition, Collections.emptyList());
    }

    private GraphQLArgument(String name, String description, GraphQLInputType type, Object defaultValue, Object value, InputValueDefinition definition, List<GraphQLDirective> directives) {
        assertValidName(name);
        assertNotNull(type, "type can't be null");
        this.name = name;
        this.description = description;
        this.type = type;
        this.defaultValue = defaultValue;
        this.value = value;
        this.definition = definition;
        this.directives = directives;
    }


    void replaceType(GraphQLInputType type) {
        this.type = type;
    }

    @Override
    public String getName() {
        return name;
    }

    public GraphQLInputType getType() {
        return type;
    }

    /**
     * An argument has a default value when it represents the logical argument structure that a {@link graphql.schema.GraphQLFieldDefinition}
     * can have and it can also have a default value when used in a schema definition language (SDL) where the
     * default value comes via the directive definition.
     *
     * @return the default value of an argument
     */
    public Object getDefaultValue() {
        return defaultValue;
    }

    /**
     * An argument ONLY has a value when its used in a schema definition language (SDL) context as the arguments to SDL directives.  The method
     * should not be called in a query context, but rather the AST / variables map should be used to obtain an arguments value.
     *
     * @return the argument value
     */
    public Object getValue() {
        return value;
    }

    public String getDescription() {
        return description;
    }

    public InputValueDefinition getDefinition() {
        return definition;
    }

    @Override
    public List<GraphQLDirective> getDirectives() {
        return new ArrayList<>(directives);
    }

    /**
     * This helps you transform the current GraphQLArgument into another one by starting a builder with all
     * the current values and allows you to transform it how you want.
     *
     * @param builderConsumer the consumer code that will be given a builder to transform
     *
     * @return a new field based on calling build on that builder
     */
    public GraphQLArgument transform(Consumer<Builder> builderConsumer) {
        Builder builder = newArgument(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static Builder newArgument() {
        return new Builder();
    }

    public static Builder newArgument(GraphQLArgument existing) {
        return new Builder(existing);
    }

    @Override
    public TraversalControl accept(TraverserContext<GraphQLType> context, GraphQLTypeVisitor visitor) {
        return visitor.visitGraphQLArgument(this, context);
    }

    @Override
    public List<GraphQLType> getChildren() {
        List<GraphQLType> children = new ArrayList<>();
        children.add(type);
        children.addAll(directives);
        return children;
    }
    @Override
    public String toString() {
        return "GraphQLArgument{" +
                "name='" + name + '\'' +
                ", value=" + value +
                ", defaultValue=" + defaultValue +
                ", type=" + type +
                '}';
    }

    public static class Builder extends GraphqlTypeBuilder {

        private GraphQLInputType type;
        private Object defaultValue;
        private Object value;
        private InputValueDefinition definition;
        private final Map<String, GraphQLDirective> directives = new LinkedHashMap<>();

        public Builder() {
        }

        public Builder(GraphQLArgument existing) {
            this.name = existing.getName();
            this.type = existing.getType();
            this.value = existing.getValue();
            this.defaultValue = existing.getDefaultValue();
            this.description = existing.getDescription();
            this.definition = existing.getDefinition();
            this.directives.putAll(FpKit.getByName(existing.getDirectives(), GraphQLDirective::getName));
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

        public Builder definition(InputValueDefinition definition) {
            this.definition = definition;
            return this;
        }


        public Builder type(GraphQLInputType type) {
            this.type = type;
            return this;
        }

        public Builder defaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder value(Object value) {
            this.value = value;
            return this;
        }

        public Builder withDirectives(GraphQLDirective... directives) {
            assertNotNull(directives, "directives can't be null");
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


        public GraphQLArgument build() {
            return new GraphQLArgument(
                    name,
                    description,
                    type,
                    defaultValue,
                    value,
                    definition,
                    sort(directives, GraphQLArgument.class, GraphQLDirective.class)
            );
        }
    }
}
