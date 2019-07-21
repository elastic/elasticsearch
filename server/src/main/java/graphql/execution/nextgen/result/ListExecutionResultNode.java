package graphql.execution.nextgen.result;

import graphql.GraphQLError;
import graphql.Internal;
import graphql.execution.ExecutionStepInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Internal
public class ListExecutionResultNode extends ExecutionResultNode {

    public ListExecutionResultNode(ExecutionStepInfo executionStepInfo,
                                   ResolvedValue resolvedValue,
                                   List<ExecutionResultNode> children) {
        this(executionStepInfo, resolvedValue, children, Collections.emptyList());

    }

    public ListExecutionResultNode(ExecutionStepInfo executionStepInfo,
                                   ResolvedValue resolvedValue,
                                   List<ExecutionResultNode> children,
                                   List<GraphQLError> errors) {
        super(executionStepInfo, resolvedValue, ResultNodesUtil.newNullableException(executionStepInfo, children), children, errors);
    }

    @Override
    public ExecutionResultNode withNewChildren(List<ExecutionResultNode> children) {
        return new ListExecutionResultNode(getExecutionStepInfo(), getResolvedValue(), children, getErrors());
    }

    @Override
    public ExecutionResultNode withNewResolvedValue(ResolvedValue resolvedValue) {
        return new ListExecutionResultNode(getExecutionStepInfo(), resolvedValue, getChildren(), getErrors());
    }

    @Override
    public ExecutionResultNode withNewExecutionStepInfo(ExecutionStepInfo executionStepInfo) {
        return new ListExecutionResultNode(executionStepInfo, getResolvedValue(), getChildren(), getErrors());
    }

    @Override
    public ExecutionResultNode withNewErrors(List<GraphQLError> errors) {
        return new ListExecutionResultNode(getExecutionStepInfo(), getResolvedValue(), getChildren(), new ArrayList<>(errors));
    }
}
