package graphql.execution.instrumentation.parameters;

import graphql.execution.ExecutionContext;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.instrumentation.Instrumentation;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.schema.GraphQLFieldDefinition;

/**
 * Parameters sent to {@link Instrumentation} methods
 */
public class InstrumentationFieldParameters {
    private final ExecutionContext executionContext;
    private final graphql.schema.GraphQLFieldDefinition fieldDef;
    private final ExecutionStepInfo executionStepInfo;
    private final InstrumentationState instrumentationState;

    public InstrumentationFieldParameters(ExecutionContext executionContext, GraphQLFieldDefinition fieldDef, ExecutionStepInfo executionStepInfo) {
        this(executionContext, fieldDef, executionStepInfo, executionContext.getInstrumentationState());
    }

    InstrumentationFieldParameters(ExecutionContext executionContext, GraphQLFieldDefinition fieldDef, ExecutionStepInfo executionStepInfo, InstrumentationState instrumentationState) {
        this.executionContext = executionContext;
        this.fieldDef = fieldDef;
        this.executionStepInfo = executionStepInfo;
        this.instrumentationState = instrumentationState;
    }

    /**
     * Returns a cloned parameters object with the new state
     *
     * @param instrumentationState the new state for this parameters object
     *
     * @return a new parameters object with the new state
     */
    public InstrumentationFieldParameters withNewState(InstrumentationState instrumentationState) {
        return new InstrumentationFieldParameters(
                this.executionContext, this.fieldDef, this.executionStepInfo, instrumentationState);
    }


    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    public GraphQLFieldDefinition getField() {
        return fieldDef;
    }

    public ExecutionStepInfo getExecutionStepInfo() {
        return executionStepInfo;
    }

    @SuppressWarnings("TypeParameterUnusedInFormals")
    public <T extends InstrumentationState> T getInstrumentationState() {
        //noinspection unchecked
        return (T) instrumentationState;
    }
}
