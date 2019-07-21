package graphql.execution.instrumentation.tracing;

import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.PublicApi;
import graphql.execution.instrumentation.Instrumentation;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.SimpleInstrumentation;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.execution.instrumentation.parameters.InstrumentationValidationParameters;
import graphql.language.Document;
import graphql.validation.ValidationError;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static graphql.execution.instrumentation.SimpleInstrumentationContext.whenCompleted;

/**
 * This {@link Instrumentation} implementation uses {@link TracingSupport} to
 * capture tracing information and puts it into the {@link ExecutionResult}
 */
@PublicApi
public class TracingInstrumentation extends SimpleInstrumentation {

    public static class Options {
        private final boolean includeTrivialDataFetchers;

        private Options(boolean includeTrivialDataFetchers) {
            this.includeTrivialDataFetchers = includeTrivialDataFetchers;
        }

        public boolean isIncludeTrivialDataFetchers() {
            return includeTrivialDataFetchers;
        }

        /**
         * By default trivial data fetchers (those that simple pull data from an object into field) are included
         * in tracing but you can control this behavior.
         *
         * @param flag the flag on whether to trace trivial data fetchers
         *
         * @return a new options object
         */
        public Options includeTrivialDataFetchers(boolean flag) {
            return new Options(flag);
        }

        public static Options newOptions() {
            return new Options(true);
        }

    }

    public TracingInstrumentation() {
        this(Options.newOptions());
    }

    public TracingInstrumentation(Options options) {
        this.options = options;
    }

    private final Options options;

    @Override
    public InstrumentationState createState() {
        return new TracingSupport(options.includeTrivialDataFetchers);
    }

    @Override
    public CompletableFuture<ExecutionResult> instrumentExecutionResult(ExecutionResult executionResult, InstrumentationExecutionParameters parameters) {
        Map<Object, Object> currentExt = executionResult.getExtensions();

        TracingSupport tracingSupport = parameters.getInstrumentationState();
        Map<Object, Object> tracingMap = new LinkedHashMap<>();
        tracingMap.putAll(currentExt == null ? Collections.emptyMap() : currentExt);
        tracingMap.put("tracing", tracingSupport.snapshotTracingData());

        return CompletableFuture.completedFuture(new ExecutionResultImpl(executionResult.getData(), executionResult.getErrors(), tracingMap));
    }

    @Override
    public InstrumentationContext<Object> beginFieldFetch(InstrumentationFieldFetchParameters parameters) {
        TracingSupport tracingSupport = parameters.getInstrumentationState();
        TracingSupport.TracingContext ctx = tracingSupport.beginField(parameters.getEnvironment(), parameters.isTrivialDataFetcher());
        return whenCompleted((result, t) -> ctx.onEnd());
    }

    @Override
    public InstrumentationContext<Document> beginParse(InstrumentationExecutionParameters parameters) {
        TracingSupport tracingSupport = parameters.getInstrumentationState();
        TracingSupport.TracingContext ctx = tracingSupport.beginParse();
        return whenCompleted((result, t) -> ctx.onEnd());
    }

    @Override
    public InstrumentationContext<List<ValidationError>> beginValidation(InstrumentationValidationParameters parameters) {
        TracingSupport tracingSupport = parameters.getInstrumentationState();
        TracingSupport.TracingContext ctx = tracingSupport.beginValidation();
        return whenCompleted((result, t) -> ctx.onEnd());
    }
}
