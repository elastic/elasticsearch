package graphql.execution.batched;

import graphql.execution.ExecutionPath;
import graphql.execution.ExecutionStepInfo;

import java.util.List;

@Deprecated
public class FetchedValues {

    private final List<FetchedValue> fetchedValues;
    private final ExecutionStepInfo executionStepInfo;
    private final ExecutionPath path;

    public FetchedValues(List<FetchedValue> fetchedValues, ExecutionStepInfo executionStepInfo, ExecutionPath path) {
        this.fetchedValues = fetchedValues;
        this.executionStepInfo = executionStepInfo;
        this.path = path;
    }

    public List<FetchedValue> getValues() {
        return fetchedValues;
    }

    public ExecutionStepInfo getExecutionStepInfo() {
        return executionStepInfo;
    }

    public ExecutionPath getPath() {
        return path;
    }
}
