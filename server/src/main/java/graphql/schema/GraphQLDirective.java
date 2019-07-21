package graphql.schema;


import graphql.Assert;
import graphql.PublicApi;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static graphql.Assert.assertNotNull;
import static graphql.Assert.assertValidName;
import static graphql.introspection.Introspection.DirectiveLocation;
import static graphql.util.FpKit.getByName;

/**
 * A directive can be used to modify the behavior of a graphql field or type.
 *
 * See http://graphql.org/learn/queries/#directives for more details on the concept.
 */
@SuppressWarnings("DeprecatedIsStillUsed") // because the graphql spec still has some of these deprecated fields
@PublicApi
public class GraphQLDirective implements GraphQLType {

    private final String name;
    private final String description;
    private final EnumSet<DirectiveLocation> locations;
    private final List<GraphQLArgument> arguments = new ArrayList<>();
    private final boolean onOperation;
    private final boolean onFragment;
    private final boolean onField;

    public GraphQLDirective(String name, String description, EnumSet<DirectiveLocation> locations,
                            List<GraphQLArgument> arguments, boolean onOperation, boolean onFragment, boolean onField) {
        assertValidName(name);
        assertNotNull(arguments, "arguments can't be null");
        this.name = name;
        this.description = description;
        this.locations = locations;
        this.arguments.addAll(arguments);
        this.onOperation = onOperation;
        this.onFragment = onFragment;
        this.onField = onField;
    }

    @Override
    public String getName() {
        return name;
    }

    public List<GraphQLArgument> getArguments() {
        return new ArrayList<>(arguments);
    }

    public GraphQLArgument getArgument(String name) {
        for (GraphQLArgument argument : arguments) {
            if (argument.getName().equals(name)) return argument;
        }
        return null;
    }

    public EnumSet<DirectiveLocation> validLocations() {
        return locations;
    }

    /**
     * @return onOperation
     *
     * @deprecated Use {@link #validLocations()}
     */
    @Deprecated
    public boolean isOnOperation() {
        return onOperation;
    }

    /**
     * @return onFragment
     *
     * @deprecated Use {@link #validLocations()}
     */
    @Deprecated
    public boolean isOnFragment() {
        return onFragment;
    }

    /**
     * @return onField
     *
     * @deprecated Use {@link #validLocations()}
     */
    @Deprecated
    public boolean isOnField() {
        return onField;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "GraphQLDirective{" +
                "name='" + name + '\'' +
                ", arguments=" + arguments +
                ", locations=" + locations +
                '}';
    }

    /**
     * This helps you transform the current GraphQLDirective into another one by starting a builder with all
     * the current values and allows you to transform it how you want.
     *
     * @param builderConsumer the consumer code that will be given a builder to transform
     *
     * @return a new field based on calling build on that builder
     */
    public GraphQLDirective transform(Consumer<Builder> builderConsumer) {
        Builder builder = newDirective(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    @Override
    public TraversalControl accept(TraverserContext<GraphQLType> context, GraphQLTypeVisitor visitor) {
        return visitor.visitGraphQLDirective(this, context);
    }

    @Override
    public List<GraphQLType> getChildren() {
        return new ArrayList<>(arguments);
    }

    public static Builder newDirective() {
        return new Builder();
    }

    public static Builder newDirective(GraphQLDirective existing) {
        return new Builder(existing);
    }

    public static class Builder extends GraphqlTypeBuilder {

        private boolean onOperation;
        private boolean onFragment;
        private boolean onField;
        private EnumSet<DirectiveLocation> locations = EnumSet.noneOf(DirectiveLocation.class);
        private final Map<String, GraphQLArgument> arguments = new LinkedHashMap<>();

        public Builder() {
        }

        @SuppressWarnings("deprecation")
        public Builder(GraphQLDirective existing) {
            this.name = existing.getName();
            this.description = existing.getDescription();
            this.onOperation = existing.isOnOperation();
            this.onFragment = existing.isOnFragment();
            this.onField = existing.isOnField();
            this.locations = existing.validLocations();
            this.arguments.putAll(getByName(existing.getArguments(), GraphQLArgument::getName));
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

        public Builder validLocations(DirectiveLocation... validLocations) {
            Collections.addAll(locations, validLocations);
            return this;
        }

        public Builder validLocation(DirectiveLocation validLocation) {
            locations.add(validLocation);
            return this;
        }

        public Builder clearValidLocations() {
            locations = EnumSet.noneOf(DirectiveLocation.class);
            return this;
        }

        public Builder argument(GraphQLArgument argument) {
            Assert.assertNotNull(argument, "argument must not be null");
            arguments.put(argument.getName(), argument);
            return this;
        }

        /**
         * Take an argument builder in a function definition and apply. Can be used in a jdk8 lambda
         * e.g.:
         * <pre>
         *     {@code
         *      argument(a -> a.name("argumentName"))
         *     }
         * </pre>
         *
         * @param builderFunction a supplier for the builder impl
         *
         * @return this
         */
        public Builder argument(UnaryOperator<GraphQLArgument.Builder> builderFunction) {
            GraphQLArgument.Builder builder = GraphQLArgument.newArgument();
            builder = builderFunction.apply(builder);
            return argument(builder);
        }

        /**
         * Same effect as the argument(GraphQLArgument). Builder.build() is called
         * from within
         *
         * @param builder an un-built/incomplete GraphQLArgument
         *
         * @return this
         */
        public Builder argument(GraphQLArgument.Builder builder) {
            return argument(builder.build());
        }

        /**
         * This is used to clear all the arguments in the builder so far.
         *
         * @return the builder
         */
        public Builder clearArguments() {
            arguments.clear();
            return this;
        }


        /**
         * @param onOperation onOperation
         *
         * @return this builder
         *
         * @deprecated Use {@code graphql.schema.GraphQLDirective.Builder#validLocations(DirectiveLocation...)}
         */
        @Deprecated
        public Builder onOperation(boolean onOperation) {
            this.onOperation = onOperation;
            return this;
        }

        /**
         * @param onFragment onFragment
         *
         * @return this builder
         *
         * @deprecated Use {@code graphql.schema.GraphQLDirective.Builder#validLocations(DirectiveLocation...)}
         */
        @Deprecated
        public Builder onFragment(boolean onFragment) {
            this.onFragment = onFragment;
            return this;
        }

        /**
         * @param onField onField
         *
         * @return this builder
         *
         * @deprecated Use {@code graphql.schema.GraphQLDirective.Builder#validLocations(DirectiveLocation...)}
         */
        @Deprecated
        public Builder onField(boolean onField) {
            this.onField = onField;
            return this;
        }

        public GraphQLDirective build() {
            return new GraphQLDirective(
                    name,
                    description,
                    locations,
                    sort(arguments, GraphQLDirective.class, GraphQLArgument.class),
                    onOperation,
                    onFragment,
                    onField);
        }


    }
}
