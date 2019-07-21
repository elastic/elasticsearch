package graphql.schema;


import graphql.PublicApi;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import java.util.Collections;
import java.util.List;

/**
 * All types in graphql have a name
 */
@PublicApi
public interface GraphQLType {
    /**
     * @return the name of the type which MUST fit within the regular expression {@code [_A-Za-z][_0-9A-Za-z]*}
     */
    String getName();

    /**
     * @return returns all types directly associated with this node
     */
    default List<GraphQLType> getChildren() { return Collections.emptyList(); }

    /**
     * Double-dispatch entry point.
     *
     * It allows to travers a given non-trivial graphQL type and move from root to nested or enclosed types.
     *
     * This implements similar pattern as {@link graphql.language.Node}, see accept(...) for more details about the pattern.
     *
     * @param context TraverserContext bound to this graphQL type object
     * @param visitor Visitor instance that performs actual processing on the types(s)
     *
     * @return Result of Visitor's operation.
     * Note! Visitor's operation might return special results to control traversal process.
     */
    TraversalControl accept(TraverserContext<GraphQLType> context, GraphQLTypeVisitor visitor);
}
