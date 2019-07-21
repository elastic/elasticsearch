package graphql.schema;


import graphql.PublicApi;
import graphql.util.TraversalControl;
import graphql.util.Traverser;
import graphql.util.TraverserContext;
import graphql.util.TraverserResult;
import graphql.util.TraverserVisitor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static graphql.util.TraversalControl.CONTINUE;

@PublicApi
public class TypeTraverser {


    private final Function<? super GraphQLType, ? extends List<GraphQLType>> getChildren;

    public TypeTraverser(Function<? super GraphQLType, ? extends List<GraphQLType>> getChildren) {
        this.getChildren = getChildren;
    }

    public TypeTraverser() {
        this(GraphQLType::getChildren);
    }

    public TraverserResult depthFirst(GraphQLTypeVisitor graphQLTypeVisitor, GraphQLType root) {
        return depthFirst(graphQLTypeVisitor, Collections.singletonList(root));
    }

    public TraverserResult depthFirst(final GraphQLTypeVisitor graphQLTypeVisitor, Collection<? extends GraphQLType> roots) {
        return depthFirst(initTraverser(), new TraverserDelegateVisitor(graphQLTypeVisitor), roots);
    }

    public TraverserResult depthFirst(final GraphQLTypeVisitor graphQLTypeVisitor,
                                      Collection<? extends GraphQLType> roots,
                                      Map<String, GraphQLType> types) {
        return depthFirst(initTraverser().rootVar(TypeTraverser.class, types), new TraverserDelegateVisitor(graphQLTypeVisitor), roots);
    }

    public TraverserResult depthFirst(final Traverser<GraphQLType> traverser,
                                      final TraverserDelegateVisitor traverserDelegateVisitor,
                                      Collection<? extends GraphQLType> roots) {
        return doTraverse(traverser, roots, traverserDelegateVisitor);
    }

    private Traverser<GraphQLType> initTraverser() {
        return Traverser.depthFirst(getChildren);
    }

    private TraverserResult doTraverse(Traverser<GraphQLType> traverser, Collection<? extends GraphQLType> roots, TraverserDelegateVisitor traverserDelegateVisitor) {
        return traverser.traverse(roots, traverserDelegateVisitor);
    }

    private static class TraverserDelegateVisitor implements TraverserVisitor<GraphQLType> {
        private final GraphQLTypeVisitor before;

        TraverserDelegateVisitor(GraphQLTypeVisitor delegate) {
            this.before = delegate;

        }

        @Override
        public TraversalControl enter(TraverserContext<GraphQLType> context) {
            return context.thisNode().accept(context, before);
        }

        @Override
        public TraversalControl leave(TraverserContext<GraphQLType> context) {
            return CONTINUE;
        }

        @Override
        public TraversalControl backRef(TraverserContext<GraphQLType> context) {
            return before.visitBackRef(context);
        }
    }

}
