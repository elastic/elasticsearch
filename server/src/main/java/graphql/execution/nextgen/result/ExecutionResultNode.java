package graphql.execution.nextgen.result;

import graphql.Assert;
import graphql.GraphQLError;
import graphql.Internal;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.MergedField;
import graphql.execution.NonNullableFieldWasNullException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static graphql.Assert.assertNotNull;

@Internal
public abstract class ExecutionResultNode {

    private final ExecutionStepInfo executionStepInfo;
    private final ResolvedValue resolvedValue;
    private final NonNullableFieldWasNullException nonNullableFieldWasNullException;
    private final List<ExecutionResultNode> children;
    private final List<GraphQLError> errors;

    protected ExecutionResultNode(ExecutionStepInfo executionStepInfo,
                                  ResolvedValue resolvedValue,
                                  NonNullableFieldWasNullException nonNullableFieldWasNullException,
                                  List<ExecutionResultNode> children,
                                  List<GraphQLError> errors) {
        this.resolvedValue = resolvedValue;
        this.executionStepInfo = executionStepInfo;
        this.nonNullableFieldWasNullException = nonNullableFieldWasNullException;
        this.children = assertNotNull(children);
        children.forEach(Assert::assertNotNull);
        this.errors = new ArrayList<>(errors);
    }

    public List<GraphQLError> getErrors() {
        return new ArrayList<>(errors);
    }

    /*
     * can be null for the RootExecutionResultNode
     */
    public ResolvedValue getResolvedValue() {
        return resolvedValue;
    }

    public MergedField getMergedField() {
        return executionStepInfo.getField();
    }

    public ExecutionStepInfo getExecutionStepInfo() {
        return executionStepInfo;
    }

    public NonNullableFieldWasNullException getNonNullableFieldWasNullException() {
        return nonNullableFieldWasNullException;
    }

    public List<ExecutionResultNode> getChildren() {
        return new ArrayList<>(this.children);
    }

    public Optional<NonNullableFieldWasNullException> getChildNonNullableException() {
        return children.stream()
                .filter(executionResultNode -> executionResultNode.getNonNullableFieldWasNullException() != null)
                .map(ExecutionResultNode::getNonNullableFieldWasNullException)
                .findFirst();
    }

    /**
     * Creates a new ExecutionResultNode of the same specific type with the new set of result children
     *
     * @param children the new children for this result node
     *
     * @return a new ExecutionResultNode with the new result children
     */
    public abstract ExecutionResultNode withNewChildren(List<ExecutionResultNode> children);

    public abstract ExecutionResultNode withNewResolvedValue(ResolvedValue resolvedValue);

    public abstract ExecutionResultNode withNewExecutionStepInfo(ExecutionStepInfo executionStepInfo);


    /**
     * Creates a new ExecutionResultNode of the same specific type with the new error collection
     *
     * @param errors the new errors for this result node
     *
     * @return a new ExecutionResultNode with the new errors
     */
    public abstract ExecutionResultNode withNewErrors(List<GraphQLError> errors);


    @Override
    public String toString() {
        return "ExecutionResultNode{" +
                "executionStepInfo=" + executionStepInfo +
                ", resolvedValue=" + resolvedValue +
                ", nonNullableFieldWasNullException=" + nonNullableFieldWasNullException +
                ", children=" + children +
                ", errors=" + errors +
                '}';
    }
}
