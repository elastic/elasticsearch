package graphql.execution;

import graphql.ExecutionResult;
import graphql.execution.defer.DeferSupport;
import graphql.execution.defer.DeferredCall;
import graphql.execution.defer.DeferredErrorSupport;
import graphql.execution.instrumentation.DeferredFieldInstrumentationContext;
import graphql.execution.instrumentation.ExecutionStrategyInstrumentationContext;
import graphql.execution.instrumentation.Instrumentation;
import graphql.execution.instrumentation.parameters.InstrumentationDeferredFieldParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionStrategyParameters;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static graphql.execution.MergedSelectionSet.newMergedSelectionSet;

/**
 * The standard graphql execution strategy that runs fields asynchronously non-blocking.
 */
public class AsyncExecutionStrategy extends AbstractAsyncExecutionStrategy {

    /**
     * The standard graphql execution strategy that runs fields asynchronously
     */
    public AsyncExecutionStrategy() {
        super(new SimpleDataFetcherExceptionHandler());
    }

    /**
     * Creates a execution strategy that uses the provided exception handler
     *
     * @param exceptionHandler the exception handler to use
     */
    public AsyncExecutionStrategy(DataFetcherExceptionHandler exceptionHandler) {
        super(exceptionHandler);
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public CompletableFuture<ExecutionResult> execute(ExecutionContext executionContext, ExecutionStrategyParameters parameters) throws NonNullableFieldWasNullException {
        Instrumentation instrumentation = executionContext.getInstrumentation();
        InstrumentationExecutionStrategyParameters instrumentationParameters = new InstrumentationExecutionStrategyParameters(executionContext, parameters);

        ExecutionStrategyInstrumentationContext executionStrategyCtx = instrumentation.beginExecutionStrategy(instrumentationParameters);

        MergedSelectionSet fields = parameters.getFields();
        List<String> fieldNames = new ArrayList<>(fields.keySet());
        List<CompletableFuture<FieldValueInfo>> futures = new ArrayList<>();
        List<String> resolvedFields = new ArrayList<>();
        for (String fieldName : fieldNames) {
            MergedField currentField = fields.getSubField(fieldName);

            ExecutionPath fieldPath = parameters.getPath().segment(mkNameForPath(currentField));
            ExecutionStrategyParameters newParameters = parameters
                    .transform(builder -> builder.field(currentField).path(fieldPath).parent(parameters));

            resolvedFields.add(fieldName);
            CompletableFuture<FieldValueInfo> future;

            if (isDeferred(executionContext, newParameters, currentField)) {
                executionStrategyCtx.onDeferredField(currentField);
                future = resolveFieldWithInfoToNull(executionContext, newParameters);
            } else {
                future = resolveFieldWithInfo(executionContext, newParameters);
            }
            futures.add(future);
        }
        CompletableFuture<ExecutionResult> overallResult = new CompletableFuture<>();
        executionStrategyCtx.onDispatched(overallResult);

        Async.each(futures).whenComplete((completeValueInfos, throwable) -> {
            BiConsumer<List<ExecutionResult>, Throwable> handleResultsConsumer = handleResults(executionContext, resolvedFields, overallResult);
            if (throwable != null) {
                handleResultsConsumer.accept(null, throwable.getCause());
                return;
            }
            List<CompletableFuture<ExecutionResult>> executionResultFuture = completeValueInfos.stream().map(FieldValueInfo::getFieldValue).collect(Collectors.toList());
            executionStrategyCtx.onFieldValuesInfo(completeValueInfos);
            Async.each(executionResultFuture).whenComplete(handleResultsConsumer);
        }).exceptionally((ex) -> {
            // if there are any issues with combining/handling the field results,
            // complete the future at all costs and bubble up any thrown exception so
            // the execution does not hang.
            overallResult.completeExceptionally(ex);
            return null;
        });

        overallResult.whenComplete(executionStrategyCtx::onCompleted);
        return overallResult;
    }

    private boolean isDeferred(ExecutionContext executionContext, ExecutionStrategyParameters parameters, MergedField currentField) {
        DeferSupport deferSupport = executionContext.getDeferSupport();
        if (deferSupport.checkForDeferDirective(currentField, executionContext.getVariables())) {
            DeferredErrorSupport errorSupport = new DeferredErrorSupport();

            // with a deferred field we are really resetting where we execute from, that is from this current field onwards
            Map<String, MergedField> fields = new LinkedHashMap<>();
            fields.put(currentField.getName(), currentField);

            ExecutionStrategyParameters callParameters = parameters.transform(builder ->
                    {
                        MergedSelectionSet mergedSelectionSet = newMergedSelectionSet().subFields(fields).build();
                        builder.deferredErrorSupport(errorSupport)
                                .field(currentField)
                                .fields(mergedSelectionSet)
                                .parent(null) // this is a break in the parent -> child chain - its a new start effectively
                                .listSize(0)
                                .currentListIndex(0);
                    }
            );

            DeferredCall call = new DeferredCall(parameters.getPath(), deferredExecutionResult(executionContext, callParameters), errorSupport);
            deferSupport.enqueue(call);
            return true;
        }
        return false;
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    private Supplier<CompletableFuture<ExecutionResult>> deferredExecutionResult(ExecutionContext executionContext, ExecutionStrategyParameters parameters) {
        return () -> {
            GraphQLFieldDefinition fieldDef = getFieldDef(executionContext, parameters, parameters.getField().getSingleField());
            GraphQLObjectType fieldContainer = (GraphQLObjectType) parameters.getExecutionStepInfo().getUnwrappedNonNullType();

            Instrumentation instrumentation = executionContext.getInstrumentation();
            DeferredFieldInstrumentationContext fieldCtx = instrumentation.beginDeferredField(
                    new InstrumentationDeferredFieldParameters(executionContext, parameters, fieldDef, createExecutionStepInfo(executionContext, parameters, fieldDef, fieldContainer))
            );
            CompletableFuture<ExecutionResult> result = new CompletableFuture<>();
            fieldCtx.onDispatched(result);
            CompletableFuture<FieldValueInfo> fieldValueInfoFuture = resolveFieldWithInfo(executionContext, parameters);

            fieldValueInfoFuture.whenComplete((fieldValueInfo, throwable) -> {
                fieldCtx.onFieldValueInfo(fieldValueInfo);

                CompletableFuture<ExecutionResult> execResultFuture = fieldValueInfo.getFieldValue();
                execResultFuture = execResultFuture.whenComplete(fieldCtx::onCompleted);
                Async.copyResults(execResultFuture, result);
            });
            return result;
        };
    }
}
