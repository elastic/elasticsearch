package graphql.execution;

import graphql.schema.GraphQLType;

import static graphql.Assert.assertNotNull;

/**
 * See (http://facebook.github.io/graphql/#sec-Errors-and-Non-Nullability), but if a non nullable field
 * actually resolves to a null value and the parent type is nullable then the parent must in fact become null
 * so we use exceptions to indicate this special case
 */
public class NonNullableFieldWasNullException extends RuntimeException {

    private final ExecutionStepInfo executionStepInfo;
    private final ExecutionPath path;


    public NonNullableFieldWasNullException(ExecutionStepInfo executionStepInfo, ExecutionPath path) {
        super(
                mkMessage(assertNotNull(executionStepInfo),
                        assertNotNull(path))
        );
        this.executionStepInfo = executionStepInfo;
        this.path = path;
    }

    public NonNullableFieldWasNullException(NonNullableFieldWasNullException previousException) {
        super(
                mkMessage(
                        assertNotNull(previousException.executionStepInfo.getParent()),
                        assertNotNull(previousException.executionStepInfo.getParent().getPath())
                ),
                previousException
        );
        this.executionStepInfo = previousException.executionStepInfo.getParent();
        this.path = previousException.executionStepInfo.getParent().getPath();
    }


    private static String mkMessage(ExecutionStepInfo executionStepInfo, ExecutionPath path) {
        GraphQLType unwrappedTyped = executionStepInfo.getUnwrappedNonNullType();
        if (executionStepInfo.hasParent()) {
            GraphQLType unwrappedParentType = executionStepInfo.getParent().getUnwrappedNonNullType();
            return String.format("Cannot return null for non-nullable type: '%s' within parent '%s' (%s)", unwrappedTyped.getName(), unwrappedParentType.getName(), path);
        }
        return String.format("Cannot return null for non-nullable type: '%s' (%s)", unwrappedTyped.getName(), path);
    }

    public ExecutionStepInfo getExecutionStepInfo() {
        return executionStepInfo;
    }

    public ExecutionPath getPath() {
        return path;
    }

    @Override
    public String toString() {
        return "NonNullableFieldWasNullException{" +
                " path=" + path +
                " executionStepInfo=" + executionStepInfo +
                '}';
    }
}
