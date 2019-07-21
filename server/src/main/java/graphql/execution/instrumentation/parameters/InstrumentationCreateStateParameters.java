package graphql.execution.instrumentation.parameters;

import graphql.ExecutionInput;
import graphql.schema.GraphQLSchema;

/**
 * Parameters sent to {@link graphql.execution.instrumentation.Instrumentation} methods
 */
public class InstrumentationCreateStateParameters {
    private final GraphQLSchema schema;
    private final ExecutionInput executionInput;

    public InstrumentationCreateStateParameters(GraphQLSchema schema, ExecutionInput executionInput) {
        this.schema = schema;
        this.executionInput = executionInput;
    }

    public GraphQLSchema getSchema() {
        return schema;
    }

    public ExecutionInput getExecutionInput() {
        return executionInput;
    }
}
