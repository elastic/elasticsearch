package graphql.util;

import graphql.Internal;

@Internal
public class TraverserVisitorStub<T> implements TraverserVisitor<T> {


    @Override
    public TraversalControl enter(TraverserContext<T> context) {
        return TraversalControl.CONTINUE;
    }

    @Override
    public TraversalControl leave(TraverserContext<T> context) {
        return TraversalControl.CONTINUE;
    }

}
