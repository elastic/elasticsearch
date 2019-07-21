package graphql.execution.nextgen.result;

import graphql.Internal;
import graphql.util.Traverser;
import graphql.util.TraverserVisitor;

import java.util.Collection;

@Internal
public class ResultNodeTraverser {

    private final Traverser<ExecutionResultNode> traverser;

    private ResultNodeTraverser(Traverser<ExecutionResultNode> traverser) {
        this.traverser = traverser;
    }

    public static ResultNodeTraverser depthFirst() {
        return new ResultNodeTraverser(Traverser.depthFirst(ExecutionResultNode::getChildren, null, null));
    }

    public void traverse(TraverserVisitor<ExecutionResultNode> visitor, ExecutionResultNode root) {
        traverser.traverse(root, visitor);
    }

    public void traverse(TraverserVisitor<ExecutionResultNode> visitor, Collection<? extends ExecutionResultNode> roots) {
        traverser.traverse(roots, visitor);
    }

}
