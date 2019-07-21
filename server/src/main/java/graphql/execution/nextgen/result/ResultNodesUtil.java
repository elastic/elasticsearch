package graphql.execution.nextgen.result;

import graphql.Assert;
import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.GraphQLError;
import graphql.Internal;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.NonNullableFieldWasNullError;
import graphql.execution.NonNullableFieldWasNullException;
import graphql.util.NodeLocation;
import graphql.util.NodeMultiZipper;
import graphql.util.NodeZipper;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import graphql.util.TraverserVisitorStub;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static graphql.execution.nextgen.result.ResultNodeAdapter.RESULT_NODE_ADAPTER;
import static graphql.util.FpKit.map;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;

@Internal
public class ResultNodesUtil {

    public static ExecutionResult toExecutionResult(RootExecutionResultNode root) {
        ExecutionResultData executionResultData = toDataImpl(root);
        return ExecutionResultImpl.newExecutionResult()
                .data(executionResultData.data)
                .errors(executionResultData.errors)
                .build();
    }

    private static class ExecutionResultData {
        Object data;
        List<GraphQLError> errors;


        public ExecutionResultData(Object data, List<GraphQLError> errors) {
            this.data = data;
            this.errors = errors;
        }
    }


    private static ExecutionResultData data(Object data, ExecutionResultNode executionResultNode) {
        List<GraphQLError> allErrors = new ArrayList<>();
        allErrors.addAll(executionResultNode.getResolvedValue().getErrors());
        allErrors.addAll(executionResultNode.getErrors());
        return new ExecutionResultData(data, allErrors);
    }

    private static ExecutionResultData data(Object data, List<GraphQLError> errors) {
        return new ExecutionResultData(data, errors);
    }

    private static ExecutionResultData data(Object data, NonNullableFieldWasNullException exception) {
        return new ExecutionResultData(data, Arrays.asList(new NonNullableFieldWasNullError(exception)));
    }

    private static ExecutionResultData toDataImpl(ExecutionResultNode root) {
        if (root instanceof LeafExecutionResultNode) {
            return root.getResolvedValue().isNullValue() ? data(null, root) : data(((LeafExecutionResultNode) root).getValue(), root);
        }
        if (root instanceof ListExecutionResultNode) {
            Optional<NonNullableFieldWasNullException> childNonNullableException = root.getChildNonNullableException();
            if (childNonNullableException.isPresent()) {
                return data(null, childNonNullableException.get());
            }
            List<ExecutionResultData> list = map(root.getChildren(), ResultNodesUtil::toDataImpl);
            List<Object> data = map(list, erd -> erd.data);
            List<GraphQLError> errors = new ArrayList<>();
            list.forEach(erd -> errors.addAll(erd.errors));
            errors.addAll(root.getErrors());
            return data(data, errors);
        }

        if (root instanceof UnresolvedObjectResultNode) {
            ExecutionStepInfo executionStepInfo = root.getExecutionStepInfo();
            return data("Not resolved : " + executionStepInfo.getPath() + " with field " + executionStepInfo.getField(), emptyList());
        }
        if (root instanceof ObjectExecutionResultNode) {
            Optional<NonNullableFieldWasNullException> childrenNonNullableException = root.getChildNonNullableException();
            if (childrenNonNullableException.isPresent()) {
                return data(null, childrenNonNullableException.get());
            }
            Map<String, Object> resultMap = new LinkedHashMap<>();
            List<GraphQLError> errors = new ArrayList<>();
            root.getChildren().forEach(child -> {
                ExecutionResultData executionResultData = toDataImpl(child);
                resultMap.put(child.getMergedField().getResultKey(), executionResultData.data);
                errors.addAll(executionResultData.errors);
            });
            errors.addAll(root.getErrors());
            return data(resultMap, errors);
        }
        throw new RuntimeException("Unexpected root " + root);
    }


    public static Optional<NonNullableFieldWasNullException> getFirstNonNullableException(Collection<ExecutionResultNode> collection) {
        return collection.stream()
                .filter(executionResultNode -> executionResultNode.getNonNullableFieldWasNullException() != null)
                .map(ExecutionResultNode::getNonNullableFieldWasNullException)
                .findFirst();
    }

    public static NonNullableFieldWasNullException newNullableException(ExecutionStepInfo executionStepInfo, List<NamedResultNode> children) {
        return newNullableException(executionStepInfo, children.stream().map(NamedResultNode::getNode).collect(Collectors.toList()));
    }

    public static Map<String, ExecutionResultNode> namedNodesToMap(List<NamedResultNode> namedResultNodes) {
        Map<String, ExecutionResultNode> result = new LinkedHashMap<>();
        for (NamedResultNode namedResultNode : namedResultNodes) {
            result.put(namedResultNode.getName(), namedResultNode.getNode());
        }
        return result;
    }

    public static NonNullableFieldWasNullException newNullableException(ExecutionStepInfo executionStepInfo, Collection<ExecutionResultNode> children) {
        // can only happen for the root node
        if (executionStepInfo == null) {
            return null;
        }
        Assert.assertNotNull(children);
        boolean listIsNonNull = executionStepInfo.isNonNullType();
        if (listIsNonNull) {
            Optional<NonNullableFieldWasNullException> firstNonNullableException = getFirstNonNullableException(children);
            if (firstNonNullableException.isPresent()) {
                return new NonNullableFieldWasNullException(firstNonNullableException.get());
            }
        }
        return null;
    }

    public static List<NodeZipper<ExecutionResultNode>> getUnresolvedNodes(Collection<ExecutionResultNode> roots) {
        List<NodeZipper<ExecutionResultNode>> result = new ArrayList<>();

        ResultNodeTraverser traverser = ResultNodeTraverser.depthFirst();
        traverser.traverse(new TraverserVisitorStub<ExecutionResultNode>() {
            @Override
            public TraversalControl enter(TraverserContext<ExecutionResultNode> context) {
                if (context.thisNode() instanceof UnresolvedObjectResultNode) {
                    result.add(new NodeZipper<>(context.thisNode(), context.getBreadcrumbs(), RESULT_NODE_ADAPTER));
                }
                return TraversalControl.CONTINUE;
            }

        }, roots);
        return result;
    }

    public static NodeMultiZipper<ExecutionResultNode> getUnresolvedNodes(ExecutionResultNode root) {
        List<NodeZipper<ExecutionResultNode>> unresolvedNodes = getUnresolvedNodes(singleton(root));
        return new NodeMultiZipper<>(root, unresolvedNodes, RESULT_NODE_ADAPTER);
    }


    public static NodeLocation key(String name) {
        return new NodeLocation(name, 0);
    }

    public static NodeLocation index(int index) {
        return new NodeLocation(null, index);
    }

}
