package graphql.schema;

import graphql.Internal;
import graphql.PublicApi;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

import static graphql.Assert.assertNotNull;
import static graphql.Assert.assertTrue;

/**
 * This represents a hierarchy an graphql runtime element upwards to its
 * associated parent elements.  For example a GraphqlDirective can be on a GraphqlArgument
 * which can be on a GraphqlFieldDefinition, which can be on a GraphqlObjectType.
 */
@PublicApi
public class GraphqlElementParentTree {

    private final GraphQLType element;
    private final GraphqlElementParentTree parent;

    @Internal
    public GraphqlElementParentTree(Deque<GraphQLType> nodeStack) {
        assertNotNull(nodeStack, "You MUST have a non null stack of elements");
        assertTrue(!nodeStack.isEmpty(), "You MUST have a non empty stack of element");

        Deque<GraphQLType> copy = new ArrayDeque<>(nodeStack);
        element = copy.pop();
        if (!copy.isEmpty()) {
            parent = new GraphqlElementParentTree(copy);
        } else {
            parent = null;
        }
    }

    /**
     * Returns the element represented by this info
     *
     * @return the element in play
     */
    public GraphQLType getElement() {
        return element;
    }

    /**
     * @return an element MAY have an optional parent
     */
    public Optional<GraphqlElementParentTree> getParentInfo() {
        return Optional.ofNullable(parent);
    }

    /**
     * @return the tree as a list of types
     */
    public List<GraphQLType> toList() {
        List<GraphQLType> types = new ArrayList<>();
        types.add(element);
        Optional<GraphqlElementParentTree> parentInfo = this.getParentInfo();
        while (parentInfo.isPresent()) {
            types.add(parentInfo.get().getElement());
            parentInfo = parentInfo.get().getParentInfo();
        }
        return types;
    }

    @Override
    public String toString() {
        return String.valueOf(element) +
                " - parent : " +
                parent;
    }
}