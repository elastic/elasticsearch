package graphql.schema;


import graphql.Internal;
import graphql.PublicApi;
import graphql.language.ScalarTypeDefinition;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static graphql.Assert.assertNotNull;
import static graphql.Assert.assertValidName;
import static graphql.util.FpKit.getByName;
import static java.util.Collections.emptyList;

/**
 * A scalar type is a leaf node in the graphql tree of types.  This class allows you to define new scalar types.
 *
 * <blockquote>
 * GraphQL provides a number of built‐in scalars, but type systems can add additional scalars with semantic meaning,
 * for example, a GraphQL system could define a scalar called Time which, while serialized as a string, promises to
 * conform to ISO‐8601. When querying a field of type Time, you can then rely on the ability to parse the result with an ISO‐8601 parser and use a client‐specific primitive for time.
 *
 * From the spec : http://facebook.github.io/graphql/#sec-Scalars
 * </blockquote>
 *
 * graphql-java ships with a set of predefined scalar types via {@link graphql.Scalars}
 *
 * @see graphql.Scalars
 */
public class GraphQLScalarType implements GraphQLType, GraphQLInputType, GraphQLOutputType, GraphQLUnmodifiedType, GraphQLNullableType, GraphQLDirectiveContainer {

    private final String name;
    private final String description;
    private final Coercing coercing;
    private final ScalarTypeDefinition definition;
    private final List<GraphQLDirective> directives;

    /**
     * @param name        the name
     * @param description the description
     * @param coercing    the coercing function
     *
     * @deprecated use the {@link #newScalar()} builder pattern instead, as this constructor will be made private in a future version.
     */
    @Internal
    @Deprecated
    public GraphQLScalarType(String name, String description, Coercing coercing) {
        this(name, description, coercing, emptyList(), null);
    }

    /**
     * @param name        the name
     * @param description the description
     * @param coercing    the coercing function
     * @param directives  the directives on this type element
     * @param definition  the AST definition
     *
     * @deprecated use the {@link #newScalar()} builder pattern instead, as this constructor will be made private in a future version.
     */
    @Internal
    @Deprecated
    public GraphQLScalarType(String name, String description, Coercing coercing, List<GraphQLDirective> directives, ScalarTypeDefinition definition) {
        assertValidName(name);
        assertNotNull(coercing, "coercing can't be null");
        assertNotNull(directives, "directives can't be null");

        this.name = name;
        this.description = description;
        this.coercing = coercing;
        this.definition = definition;
        this.directives = directives;
    }

    @Override
    public String getName() {
        return name;
    }


    public String getDescription() {
        return description;
    }


    public Coercing getCoercing() {
        return coercing;
    }

    public ScalarTypeDefinition getDefinition() {
        return definition;
    }

    @Override
    public List<GraphQLDirective> getDirectives() {
        return new ArrayList<>(directives);
    }

    @Override
    public String toString() {
        return "GraphQLScalarType{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", coercing=" + coercing +
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
    public GraphQLScalarType transform(Consumer<Builder> builderConsumer) {
        Builder builder = newScalar(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    @Override
    public TraversalControl accept(TraverserContext<GraphQLType> context, GraphQLTypeVisitor visitor) {
        return visitor.visitGraphQLScalarType(this, context);
    }

    @Override
    public List<GraphQLType> getChildren() {
        return new ArrayList<>(directives);
    }

    public static Builder newScalar() {
        return new Builder();
    }

    public static Builder newScalar(GraphQLScalarType existing) {
        return new Builder(existing);
    }


    @PublicApi
    public static class Builder extends GraphqlTypeBuilder {
        private Coercing coercing;
        private ScalarTypeDefinition definition;
        private final Map<String, GraphQLDirective> directives = new LinkedHashMap<>();

        public Builder() {
        }

        public Builder(GraphQLScalarType existing) {
            name = existing.getName();
            description = existing.getDescription();
            coercing = existing.getCoercing();
            definition = existing.getDefinition();
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

        public Builder definition(ScalarTypeDefinition definition) {
            this.definition = definition;
            return this;
        }

        public Builder coercing(Coercing coercing) {
            this.coercing = coercing;
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

        public GraphQLScalarType build() {
            return new GraphQLScalarType(name,
                    description,
                    coercing,
                    sort(directives, GraphQLScalarType.class, GraphQLDirective.class),
                    definition);
        }
    }
}