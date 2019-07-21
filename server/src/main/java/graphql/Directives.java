package graphql;


import graphql.schema.GraphQLDirective;

import static graphql.Scalars.GraphQLBoolean;
import static graphql.introspection.Introspection.DirectiveLocation.FIELD;
import static graphql.introspection.Introspection.DirectiveLocation.FRAGMENT_SPREAD;
import static graphql.introspection.Introspection.DirectiveLocation.INLINE_FRAGMENT;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLNonNull.nonNull;

/**
 * The directives that are understood by graphql-java
 */
public class Directives {

    public static final GraphQLDirective IncludeDirective = GraphQLDirective.newDirective()
            .name("include")
            .description("Directs the executor to include this field or fragment only when the `if` argument is true")
            .argument(newArgument()
                    .name("if")
                    .type(nonNull(GraphQLBoolean))
                    .description("Included when true."))
            .validLocations(FRAGMENT_SPREAD, INLINE_FRAGMENT, FIELD)
            .build();

    public static final GraphQLDirective SkipDirective = GraphQLDirective.newDirective()
            .name("skip")
            .description("Directs the executor to skip this field or fragment when the `if`'argument is true.")
            .argument(newArgument()
                    .name("if")
                    .type(nonNull(GraphQLBoolean))
                    .description("Skipped when true."))
            .validLocations(FRAGMENT_SPREAD, INLINE_FRAGMENT, FIELD)
            .build();

    /**
     * The @defer directive can be used to defer sending data for a field till later in the query.  This is an opt in
     * directive that is not available unless it is explicitly put into the schema.
     */
    public static final GraphQLDirective DeferDirective = GraphQLDirective.newDirective()
            .name("defer")
            .description("This directive allows results to be deferred during execution")
            .argument(newArgument()
                    .name("if")
                    .type(nonNull(GraphQLBoolean))
                    .description("Deferred behaviour is controlled by this argument")
                    .defaultValue(true)
            )
            .validLocations(FIELD)
            .build();

}
