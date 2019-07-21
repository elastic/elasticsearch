package graphql.execution.nextgen.result;

import graphql.GraphQLError;
import graphql.Internal;
import graphql.execution.ExecutionStepInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Internal
public class ObjectExecutionResultNode extends ExecutionResultNode {


    public ObjectExecutionResultNode(ExecutionStepInfo executionStepInfo,
                                     ResolvedValue resolvedValue,
                                     List<ExecutionResultNode> children) {
        this(executionStepInfo, resolvedValue, children, Collections.emptyList());

    }

    public ObjectExecutionResultNode(ExecutionStepInfo executionStepInfo,
                                     ResolvedValue resolvedValue,
                                     List<ExecutionResultNode> children,
                                     List<GraphQLError> errors) {
        super(executionStepInfo, resolvedValue, ResultNodesUtil.newNullableException(executionStepInfo, children), children, errors);
    }


    @Override
    public ObjectExecutionResultNode withNewChildren(List<ExecutionResultNode> children) {
        return new ObjectExecutionResultNode(getExecutionStepInfo(), getResolvedValue(), children, getErrors());
    }

    @Override
    public ExecutionResultNode withNewResolvedValue(ResolvedValue resolvedValue) {
        return new ObjectExecutionResultNode(getExecutionStepInfo(), resolvedValue, getChildren(), getErrors());
    }

    @Override
    public ExecutionResultNode withNewExecutionStepInfo(ExecutionStepInfo executionStepInfo) {
        return new ObjectExecutionResultNode(executionStepInfo, getResolvedValue(), getChildren(), getErrors());
    }

    @Override
    public ExecutionResultNode withNewErrors(List<GraphQLError> errors) {
        return new ObjectExecutionResultNode(getExecutionStepInfo(), getResolvedValue(), getChildren(), new ArrayList<>(errors));
    }
}
