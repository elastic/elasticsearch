package graphql.execution.nextgen;

import graphql.Internal;
import graphql.execution.ExecutionContext;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.nextgen.result.ExecutionResultNode;
import graphql.execution.nextgen.result.ObjectExecutionResultNode;
import graphql.execution.nextgen.result.ResolvedValue;
import graphql.execution.nextgen.result.ResultNodesUtil;
import graphql.execution.nextgen.result.RootExecutionResultNode;
import graphql.util.NodeMultiZipper;
import graphql.util.NodeZipper;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static graphql.execution.Async.each;
import static graphql.execution.Async.mapCompose;
import static graphql.util.FpKit.map;

@Internal
public class DefaultExecutionStrategy implements ExecutionStrategy {

    ExecutionStrategyUtil util = new ExecutionStrategyUtil();


    /*
     * the fundamental algorithm is:
     * - fetch sub selection and analyze it
     * - convert the fetched value analysis into result node
     * - get all unresolved result nodes and resolve the sub selection (start again recursively)
     */
    @Override
    public CompletableFuture<RootExecutionResultNode> execute(ExecutionContext context, FieldSubSelection fieldSubSelection) {
        return resolveSubSelection(context, fieldSubSelection)
                .thenApply(RootExecutionResultNode::new);
    }

    private CompletableFuture<List<ExecutionResultNode>> resolveSubSelection(ExecutionContext executionContext, FieldSubSelection fieldSubSelection) {
        List<CompletableFuture<ExecutionResultNode>> namedNodesCFList =
                mapCompose(util.fetchSubSelection(executionContext, fieldSubSelection), node -> resolveAllChildNodes(executionContext, node));
        return each(namedNodesCFList);
    }

    private CompletableFuture<ExecutionResultNode> resolveAllChildNodes(ExecutionContext context, ExecutionResultNode node) {
        NodeMultiZipper<ExecutionResultNode> unresolvedNodes = ResultNodesUtil.getUnresolvedNodes(node);
        List<CompletableFuture<NodeZipper<ExecutionResultNode>>> resolvedNodes = map(unresolvedNodes.getZippers(), unresolvedNode -> resolveNode(context, unresolvedNode));
        return resolvedNodesToResultNode(unresolvedNodes, resolvedNodes);
    }

    private CompletableFuture<NodeZipper<ExecutionResultNode>> resolveNode(ExecutionContext executionContext, NodeZipper<ExecutionResultNode> unresolvedNode) {
        ExecutionStepInfo executionStepInfo = unresolvedNode.getCurNode().getExecutionStepInfo();
        ResolvedValue resolvedValue = unresolvedNode.getCurNode().getResolvedValue();
        FieldSubSelection fieldSubSelection = util.createFieldSubSelection(executionContext, executionStepInfo, resolvedValue);
        return resolveSubSelection(executionContext, fieldSubSelection)
                .thenApply(resolvedChildMap -> unresolvedNode.withNewNode(new ObjectExecutionResultNode(executionStepInfo, resolvedValue, resolvedChildMap)));
    }

    private CompletableFuture<ExecutionResultNode> resolvedNodesToResultNode(
            NodeMultiZipper<ExecutionResultNode> unresolvedNodes,
            List<CompletableFuture<NodeZipper<ExecutionResultNode>>> resolvedNodes) {
        return each(resolvedNodes)
                .thenApply(unresolvedNodes::withReplacedZippers)
                .thenApply(NodeMultiZipper::toRootNode);
    }


}
