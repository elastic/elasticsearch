package graphql.execution.instrumentation;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.PublicApi;
import graphql.execution.Async;
import graphql.execution.ExecutionContext;
import graphql.execution.FieldValueInfo;
import graphql.execution.MergedField;
import graphql.execution.instrumentation.parameters.InstrumentationCreateStateParameters;
import graphql.execution.instrumentation.parameters.InstrumentationDeferredFieldParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecuteOperationParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionStrategyParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldCompleteParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldParameters;
import graphql.execution.instrumentation.parameters.InstrumentationValidationParameters;
import graphql.language.Document;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLSchema;
import graphql.validation.ValidationError;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static graphql.Assert.assertNotNull;
import static java.util.stream.Collectors.toList;

/**
 * This allows you to chain together a number of {@link graphql.execution.instrumentation.Instrumentation} implementations
 * and run them in sequence.  The list order of instrumentation objects is always guaranteed to be followed and
 * the {@link graphql.execution.instrumentation.InstrumentationState} objects they create will be passed back to the originating
 * implementation.
 *
 * @see graphql.execution.instrumentation.Instrumentation
 */
@PublicApi
public class ChainedInstrumentation implements Instrumentation {

    // This class is inspired from https://github.com/leangen/graphql-spqr/blob/master/src/main/java/io/leangen/graphql/GraphQLRuntime.java#L80

    private final List<Instrumentation> instrumentations;

    public ChainedInstrumentation(List<Instrumentation> instrumentations) {
        this.instrumentations = Collections.unmodifiableList(assertNotNull(instrumentations));
    }

    /**
     * @return the list of instrumentations in play
     */
    public List<Instrumentation> getInstrumentations() {
        return instrumentations;
    }

    private InstrumentationState getState(Instrumentation instrumentation, InstrumentationState parametersInstrumentationState) {
        ChainedInstrumentationState chainedInstrumentationState = (ChainedInstrumentationState) parametersInstrumentationState;
        return chainedInstrumentationState.getState(instrumentation);
    }

    @Override
    public InstrumentationState createState(InstrumentationCreateStateParameters parameters) {
        return new ChainedInstrumentationState(instrumentations, parameters);
    }

    @Override
    public InstrumentationContext<ExecutionResult> beginExecution(final InstrumentationExecutionParameters parameters) {
        return new ChainedInstrumentationContext<>(instrumentations.stream()
                .map(instrumentation -> {
                    InstrumentationState state = getState(instrumentation, parameters.getInstrumentationState());
                    return instrumentation.beginExecution(parameters.withNewState(state));
                })
                .collect(toList()));
    }

    @Override
    public InstrumentationContext<Document> beginParse(InstrumentationExecutionParameters parameters) {
        return new ChainedInstrumentationContext<>(instrumentations.stream()
                .map(instrumentation -> {
                    InstrumentationState state = getState(instrumentation, parameters.getInstrumentationState());
                    return instrumentation.beginParse(parameters.withNewState(state));
                })
                .collect(toList()));
    }

    @Override
    public InstrumentationContext<List<ValidationError>> beginValidation(InstrumentationValidationParameters parameters) {
        return new ChainedInstrumentationContext<>(instrumentations.stream()
                .map(instrumentation -> {
                    InstrumentationState state = getState(instrumentation, parameters.getInstrumentationState());
                    return instrumentation.beginValidation(parameters.withNewState(state));
                })
                .collect(toList()));
    }

    @Override
    public InstrumentationContext<ExecutionResult> beginExecuteOperation(InstrumentationExecuteOperationParameters parameters) {
        return new ChainedInstrumentationContext<>(instrumentations.stream()
                .map(instrumentation -> {
                    InstrumentationState state = getState(instrumentation, parameters.getInstrumentationState());
                    return instrumentation.beginExecuteOperation(parameters.withNewState(state));
                })
                .collect(toList()));
    }

    @Override
    public ExecutionStrategyInstrumentationContext beginExecutionStrategy(InstrumentationExecutionStrategyParameters parameters) {
        return new ChainedExecutionStrategyInstrumentationContext(instrumentations.stream()
                .map(instrumentation -> {
                    InstrumentationState state = getState(instrumentation, parameters.getInstrumentationState());
                    return instrumentation.beginExecutionStrategy(parameters.withNewState(state));
                })
                .collect(toList()));
    }

    @Override
    public DeferredFieldInstrumentationContext beginDeferredField(InstrumentationDeferredFieldParameters parameters) {
        return new ChainedDeferredExecutionStrategyInstrumentationContext(instrumentations.stream()
                .map(instrumentation -> {
                    InstrumentationState state = getState(instrumentation, parameters.getInstrumentationState());
                    return instrumentation.beginDeferredField(parameters.withNewState(state));
                })
                .collect(toList()));
    }

    @Override
    public InstrumentationContext<ExecutionResult> beginField(InstrumentationFieldParameters parameters) {
        return new ChainedInstrumentationContext<>(instrumentations.stream()
                .map(instrumentation -> {
                    InstrumentationState state = getState(instrumentation, parameters.getInstrumentationState());
                    return instrumentation.beginField(parameters.withNewState(state));
                })
                .collect(toList()));
    }

    @Override
    public InstrumentationContext<Object> beginFieldFetch(InstrumentationFieldFetchParameters parameters) {
        return new ChainedInstrumentationContext<>(instrumentations.stream()
                .map(instrumentation -> {
                    InstrumentationState state = getState(instrumentation, parameters.getInstrumentationState());
                    return instrumentation.beginFieldFetch(parameters.withNewState(state));
                })
                .collect(toList()));
    }

    @Override
    public InstrumentationContext<ExecutionResult> beginFieldComplete(InstrumentationFieldCompleteParameters parameters) {
        return new ChainedInstrumentationContext<>(instrumentations.stream()
                .map(instrumentation -> {
                    InstrumentationState state = getState(instrumentation, parameters.getInstrumentationState());
                    return instrumentation.beginFieldComplete(parameters.withNewState(state));
                })
                .collect(toList()));
    }

    @Override
    public InstrumentationContext<ExecutionResult> beginFieldListComplete(InstrumentationFieldCompleteParameters parameters) {
        return new ChainedInstrumentationContext<>(instrumentations.stream()
                .map(instrumentation -> {
                    InstrumentationState state = getState(instrumentation, parameters.getInstrumentationState());
                    return instrumentation.beginFieldListComplete(parameters.withNewState(state));
                })
                .collect(toList()));
    }

    @Override
    public ExecutionInput instrumentExecutionInput(ExecutionInput executionInput, InstrumentationExecutionParameters parameters) {
        for (Instrumentation instrumentation : instrumentations) {
            InstrumentationState state = getState(instrumentation, parameters.getInstrumentationState());
            executionInput = instrumentation.instrumentExecutionInput(executionInput, parameters.withNewState(state));
        }
        return executionInput;
    }

    @Override
    public DocumentAndVariables instrumentDocumentAndVariables(DocumentAndVariables documentAndVariables, InstrumentationExecutionParameters parameters) {
        for (Instrumentation instrumentation : instrumentations) {
            InstrumentationState state = getState(instrumentation, parameters.getInstrumentationState());
            documentAndVariables = instrumentation.instrumentDocumentAndVariables(documentAndVariables, parameters.withNewState(state));
        }
        return documentAndVariables;
    }

    @Override
    public GraphQLSchema instrumentSchema(GraphQLSchema schema, InstrumentationExecutionParameters parameters) {
        for (Instrumentation instrumentation : instrumentations) {
            InstrumentationState state = getState(instrumentation, parameters.getInstrumentationState());
            schema = instrumentation.instrumentSchema(schema, parameters.withNewState(state));
        }
        return schema;
    }

    @Override
    public ExecutionContext instrumentExecutionContext(ExecutionContext executionContext, InstrumentationExecutionParameters parameters) {
        for (Instrumentation instrumentation : instrumentations) {
            InstrumentationState state = getState(instrumentation, parameters.getInstrumentationState());
            executionContext = instrumentation.instrumentExecutionContext(executionContext, parameters.withNewState(state));
        }
        return executionContext;
    }

    @Override
    public DataFetcher<?> instrumentDataFetcher(DataFetcher<?> dataFetcher, InstrumentationFieldFetchParameters parameters) {
        for (Instrumentation instrumentation : instrumentations) {
            InstrumentationState state = getState(instrumentation, parameters.getInstrumentationState());
            dataFetcher = instrumentation.instrumentDataFetcher(dataFetcher, parameters.withNewState(state));
        }
        return dataFetcher;
    }

    @Override
    public CompletableFuture<ExecutionResult> instrumentExecutionResult(ExecutionResult executionResult, InstrumentationExecutionParameters parameters) {
        CompletableFuture<List<ExecutionResult>> resultsFuture = Async.eachSequentially(instrumentations, (instrumentation, index, prevResults) -> {
            InstrumentationState state = getState(instrumentation, parameters.getInstrumentationState());
            ExecutionResult lastResult = prevResults.size() > 0 ? prevResults.get(prevResults.size() - 1) : executionResult;
            return instrumentation.instrumentExecutionResult(lastResult, parameters.withNewState(state));
        });
        return resultsFuture.thenApply((results) -> results.isEmpty() ? executionResult : results.get(results.size() - 1));
    }

    private static class ChainedInstrumentationState implements InstrumentationState {
        private final Map<Instrumentation, InstrumentationState> instrumentationStates;


        private ChainedInstrumentationState(List<Instrumentation> instrumentations, InstrumentationCreateStateParameters parameters) {
            instrumentationStates = new LinkedHashMap<>(instrumentations.size());
            instrumentations.forEach(i -> instrumentationStates.put(i, i.createState(parameters)));
        }

        private InstrumentationState getState(Instrumentation instrumentation) {
            return instrumentationStates.get(instrumentation);
        }

    }

    private static class ChainedInstrumentationContext<T> implements InstrumentationContext<T> {

        private final List<InstrumentationContext<T>> contexts;

        ChainedInstrumentationContext(List<InstrumentationContext<T>> contexts) {
            this.contexts = Collections.unmodifiableList(contexts);
        }

        @Override
        public void onDispatched(CompletableFuture<T> result) {
            contexts.forEach(context -> context.onDispatched(result));
        }

        @Override
        public void onCompleted(T result, Throwable t) {
            contexts.forEach(context -> context.onCompleted(result, t));
        }
    }

    private static class ChainedExecutionStrategyInstrumentationContext implements ExecutionStrategyInstrumentationContext {

        private final List<ExecutionStrategyInstrumentationContext> contexts;

        ChainedExecutionStrategyInstrumentationContext(List<ExecutionStrategyInstrumentationContext> contexts) {
            this.contexts = Collections.unmodifiableList(contexts);
        }

        @Override
        public void onDispatched(CompletableFuture<ExecutionResult> result) {
            contexts.forEach(context -> context.onDispatched(result));
        }

        @Override
        public void onCompleted(ExecutionResult result, Throwable t) {
            contexts.forEach(context -> context.onCompleted(result, t));
        }

        @Override
        public void onFieldValuesInfo(List<FieldValueInfo> fieldValueInfoList) {
            contexts.forEach(context -> context.onFieldValuesInfo(fieldValueInfoList));
        }

        @Override
        public void onDeferredField(MergedField field) {
            contexts.forEach(context -> context.onDeferredField(field));
        }
    }

    private static class ChainedDeferredExecutionStrategyInstrumentationContext implements DeferredFieldInstrumentationContext {

        private final List<DeferredFieldInstrumentationContext> contexts;

        ChainedDeferredExecutionStrategyInstrumentationContext(List<DeferredFieldInstrumentationContext> contexts) {
            this.contexts = Collections.unmodifiableList(contexts);
        }

        @Override
        public void onDispatched(CompletableFuture<ExecutionResult> result) {
            contexts.forEach(context -> context.onDispatched(result));
        }

        @Override
        public void onCompleted(ExecutionResult result, Throwable t) {
            contexts.forEach(context -> context.onCompleted(result, t));
        }

        @Override
        public void onFieldValueInfo(FieldValueInfo fieldValueInfo) {
            contexts.forEach(context -> context.onFieldValueInfo(fieldValueInfo));
        }
    }
}

