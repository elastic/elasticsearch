package graphql.execution.nextgen.result;

import graphql.GraphQLError;
import graphql.execution.ExecutionStepInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static graphql.Assert.assertShouldNeverHappen;

public class RootExecutionResultNode extends ObjectExecutionResultNode {


    public RootExecutionResultNode(List<ExecutionResultNode> children, List<GraphQLError> errors) {
        super(null, null, children, errors);
    }

    public RootExecutionResultNode(List<ExecutionResultNode> children) {
        super(null, null, children, Collections.emptyList());
    }

    @Override
    public ExecutionStepInfo getExecutionStepInfo() {
        return assertShouldNeverHappen("not supported at root node");
    }

    @Override
    public ResolvedValue getResolvedValue() {
        return assertShouldNeverHappen("not supported at root node");
    }

    @Override
    public RootExecutionResultNode withNewChildren(List<ExecutionResultNode> children) {
        return new RootExecutionResultNode(children, getErrors());
    }

    @Override
    public ExecutionResultNode withNewResolvedValue(ResolvedValue resolvedValue) {
        return assertShouldNeverHappen("not supported at root node");
    }

    @Override
    public ExecutionResultNode withNewExecutionStepInfo(ExecutionStepInfo executionStepInfo) {
        return assertShouldNeverHappen("not supported at root node");
    }

    @Override
    public ExecutionResultNode withNewErrors(List<GraphQLError> errors) {
        return new RootExecutionResultNode(getChildren(), new ArrayList<>(errors));
    }
}
