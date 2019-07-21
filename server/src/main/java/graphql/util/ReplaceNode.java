package graphql.util;

import graphql.Internal;

/**
 * Special class to be set as var in {@link TraverserContext#setVar(Class, Object)} to indicate that the current node should be replaced.
 * This is used when the new Node actually has different children and you wanna iterator over the new State.
 */
@Internal
public class ReplaceNode {

    private final Object newNode;

    public ReplaceNode(Object newNode) {
        this.newNode = newNode;
    }

    public Object getNewNode() {
        return newNode;
    }
}
