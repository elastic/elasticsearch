package graphql.util;

import graphql.Internal;

@Internal
public interface TraverserVisitor<T> {

    /**
     * @param context the context in place
     *
     * @return Any allowed control value
     */
    TraversalControl enter(TraverserContext<T> context);

    /**
     * @param context the context in place
     *
     * @return Only Continue or Quit allowed
     */
    TraversalControl leave(TraverserContext<T> context);

    /**
     * @param context the context in place
     *
     * @return Only Continue or Quit allowed
     */
    default TraversalControl backRef(TraverserContext<T> context) {
        return TraversalControl.CONTINUE;
    }

}
