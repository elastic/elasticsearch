package graphql.execution.instrumentation.parameters;

import graphql.execution.ExecutionContext;
import graphql.execution.ExecutionStrategyParameters;
import graphql.execution.instrumentation.InstrumentationState;

/**
 * Parameters sent to {@link graphql.execution.instrumentation.Instrumentation} methods
 */
public class InstrumentationExecutionStrategyParameters {

    private final ExecutionContext executionContext;
    private final ExecutionStrategyParameters executionStrategyParameters;
    private final InstrumentationState instrumentationState;

    public InstrumentationExecutionStrategyParameters(ExecutionContext executionContext, ExecutionStrategyParameters executionStrategyParameters) {
        this(executionContext, executionStrategyParameters, executionContext.getInstrumentationState());
    }

    private InstrumentationExecutionStrategyParameters(ExecutionContext executionContext, ExecutionStrategyParameters executionStrategyParameters, InstrumentationState instrumentationState) {
        this.executionContext = executionContext;
        this.executionStrategyParameters = executionStrategyParameters;
        this.instrumentationState = instrumentationState;
    }

    /**
     * Returns a cloned parameters object with the new state
     *
     * @param instrumentationState the new state for this parameters object
     *
     * @return a new parameters object with the new state
     */
    public InstrumentationExecutionStrategyParameters withNewState(InstrumentationState instrumentationState) {
        return new InstrumentationExecutionStrategyParameters(executionContext, executionStrategyParameters, instrumentationState);
    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    public ExecutionStrategyParameters getExecutionStrategyParameters() {
        return executionStrategyParameters;
    }

    @SuppressWarnings("TypeParameterUnusedInFormals")
    public <T extends InstrumentationState> T getInstrumentationState() {
        //noinspection unchecked
        return (T) instrumentationState;
    }
}
