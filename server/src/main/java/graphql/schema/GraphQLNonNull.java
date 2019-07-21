package graphql.schema;


import graphql.PublicApi;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import java.util.Collections;
import java.util.List;

import static graphql.Assert.assertNotNull;
import static graphql.Assert.assertTrue;

/**
 * A modified type that indicates there the underlying wrapped type will not be null.
 *
 * See http://graphql.org/learn/schema/#lists-and-non-null for more details on the concept
 */
@PublicApi

public class GraphQLNonNull implements GraphQLType, GraphQLInputType, GraphQLOutputType, GraphQLModifiedType {

    /**
     * A factory method for creating non null types so that when used with static imports allows
     * more readable code such as
     * {@code .type(nonNull(GraphQLString)) }
     *
     * @param wrappedType the type to wrap as being non null
     *
     * @return a GraphQLNonNull of that wrapped type
     */
    public static GraphQLNonNull nonNull(GraphQLType wrappedType) {
        return new GraphQLNonNull(wrappedType);
    }

    private GraphQLType wrappedType;

    public GraphQLNonNull(GraphQLType wrappedType) {
        assertNotNull(wrappedType, "wrappedType can't be null");
        assertNonNullWrapping(wrappedType);
        this.wrappedType = wrappedType;
    }

    private void assertNonNullWrapping(GraphQLType wrappedType) {
        assertTrue(!GraphQLTypeUtil.isNonNull(wrappedType), String.format("A non null type cannot wrap an existing non null type '%s'",
                GraphQLTypeUtil.simplePrint(wrappedType)));
    }

    @Override
    public GraphQLType getWrappedType() {
        return wrappedType;
    }


    void replaceType(GraphQLType type) {
        assertNonNullWrapping(type);
        wrappedType = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GraphQLNonNull that = (GraphQLNonNull) o;

        return !(wrappedType != null ? !wrappedType.equals(that.wrappedType) : that.wrappedType != null);

    }

    @Override
    public int hashCode() {
        return wrappedType != null ? wrappedType.hashCode() : 0;
    }

    @Override
    public String toString() {
        return GraphQLTypeUtil.simplePrint(this);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public TraversalControl accept(TraverserContext<GraphQLType> context, GraphQLTypeVisitor visitor) {
        return visitor.visitGraphQLNonNull(this, context);
    }

    @Override
    public List<GraphQLType> getChildren() {
        return Collections.singletonList(wrappedType);
    }
}
