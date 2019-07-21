package graphql.execution.instrumentation.parameters;

import graphql.execution.ExecutionContext;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.ExecutionStrategyParameters;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.schema.GraphQLFieldDefinition;

/**
 * Parameters sent to {@link graphql.execution.instrumentation.Instrumentation} methods
 */
public class InstrumentationFieldCompleteParameters {
    private final ExecutionContext executionContext;
    private final GraphQLFieldDefinition fieldDef;
    private final ExecutionStepInfo typeInfo;
    private final Object fetchedValue;
    private final InstrumentationState instrumentationState;
    private final ExecutionStrategyParameters executionStrategyParameters;

    public InstrumentationFieldCompleteParameters(ExecutionContext executionContext, ExecutionStrategyParameters executionStrategyParameters, GraphQLFieldDefinition fieldDef, ExecutionStepInfo typeInfo, Object fetchedValue) {
        this(executionContext, executionStrategyParameters, fieldDef, typeInfo, fetchedValue, executionContext.getInstrumentationState());
    }

    InstrumentationFieldCompleteParameters(ExecutionContext executionContext, ExecutionStrategyParameters executionStrategyParameters, GraphQLFieldDefinition fieldDef, ExecutionStepInfo typeInfo, Object fetchedValue, InstrumentationState instrumentationState) {
        this.executionContext = executionContext;
        this.executionStrategyParameters = executionStrategyParameters;
        this.fieldDef = fieldDef;
        this.typeInfo = typeInfo;
        this.fetchedValue = fetchedValue;
        this.instrumentationState = instrumentationState;
    }

    /**
     * Returns a cloned parameters object with the new state
     *
     * @param instrumentationState the new state for this parameters object
     *
     * @return a new parameters object with the new state
     */
    public InstrumentationFieldCompleteParameters withNewState(InstrumentationState instrumentationState) {
        return new InstrumentationFieldCompleteParameters(
                this.executionContext, executionStrategyParameters, this.fieldDef, this.typeInfo, this.fetchedValue, instrumentationState);
    }


    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    public ExecutionStrategyParameters getExecutionStrategyParameters() {
        return executionStrategyParameters;
    }

    public GraphQLFieldDefinition getField() {
        return fieldDef;
    }

    public ExecutionStepInfo getTypeInfo() {
        return typeInfo;
    }

    public Object getFetchedValue() {
        return fetchedValue;
    }

    @SuppressWarnings("TypeParameterUnusedInFormals")
    public <T extends InstrumentationState> T getInstrumentationState() {
        //noinspection unchecked
        return (T) instrumentationState;
    }
}
