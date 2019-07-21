package graphql.execution.instrumentation;

/**
 * An {@link Instrumentation} implementation can create this as a stateful object that is then passed
 * to each instrumentation method, allowing state to be passed down with the request execution
 *
 * @see Instrumentation#createState(graphql.execution.instrumentation.parameters.InstrumentationCreateStateParameters)
 */
public interface InstrumentationState {
}
