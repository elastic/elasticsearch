package graphql.execution.instrumentation.tracing;

import graphql.PublicApi;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.schema.DataFetchingEnvironment;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This creates a map of tracing information as outlined in https://github.com/apollographql/apollo-tracing
 * <p>
 * This is a stateful object that should be instantiated and called via {@link java.lang.instrument.Instrumentation}
 * calls.  It has been made a separate class so that you can compose this into existing
 * instrumentation code.
 */
@PublicApi
public class TracingSupport implements InstrumentationState {

    private final Instant startRequestTime;
    private final long startRequestNanos;
    private final ConcurrentLinkedQueue<Map<String, Object>> fieldData;
    private final Map<String, Object> parseMap = new LinkedHashMap<>();
    private final Map<String, Object> validationMap = new LinkedHashMap<>();
    private final boolean includeTrivialDataFetchers;

    /**
     * The timer starts as soon as you create this object
     *
     * @param includeTrivialDataFetchers whether the trace trivial data fetchers
     */
    public TracingSupport(boolean includeTrivialDataFetchers) {
        this.includeTrivialDataFetchers = includeTrivialDataFetchers;
        startRequestNanos = System.nanoTime();
        startRequestTime = Instant.now();
        fieldData = new ConcurrentLinkedQueue<>();
    }

    /**
     * A simple object that you need to call {@link #onEnd()} on
     */
    public interface TracingContext {
        /**
         * Call this to end the current trace context
         */
        void onEnd();
    }

    /**
     * This should be called to start the trace of a field, with {@link TracingContext#onEnd()} being called to
     * end the call.
     *
     * @param dataFetchingEnvironment the data fetching that is occurring
     * @param trivialDataFetcher      if the data fetcher is considered trivial
     *
     * @return a context to call end on
     */
    public TracingContext beginField(DataFetchingEnvironment dataFetchingEnvironment, boolean trivialDataFetcher) {
        if (!includeTrivialDataFetchers && trivialDataFetcher) {
            return () -> {
                // nothing to do
            };
        }
        long startFieldFetch = System.nanoTime();
        return () -> {
            long now = System.nanoTime();
            long duration = now - startFieldFetch;
            long startOffset = startFieldFetch - startRequestNanos;
            ExecutionStepInfo executionStepInfo = dataFetchingEnvironment.getExecutionStepInfo();

            Map<String, Object> fetchMap = new LinkedHashMap<>();
            fetchMap.put("path", executionStepInfo.getPath().toList());
            fetchMap.put("parentType", executionStepInfo.getParent().getUnwrappedNonNullType().getName());
            fetchMap.put("returnType", executionStepInfo.simplePrint());
            fetchMap.put("fieldName", executionStepInfo.getFieldDefinition().getName());
            fetchMap.put("startOffset", startOffset);
            fetchMap.put("duration", duration);

            fieldData.add(fetchMap);
        };
    }

    /**
     * This should be called to start the trace of query parsing, with {@link TracingContext#onEnd()} being called to
     * end the call.
     *
     * @return a context to call end on
     */
    public TracingContext beginParse() {
        return traceToMap(parseMap);
    }

    /**
     * This should be called to start the trace of query validation, with {@link TracingContext#onEnd()} being called to
     * end the call.
     *
     * @return a context to call end on
     */
    public TracingContext beginValidation() {
        return traceToMap(validationMap);
    }

    private TracingContext traceToMap(Map<String, Object> map) {
        long start = System.nanoTime();
        return () -> {
            long now = System.nanoTime();
            long duration = now - start;
            long startOffset = now - startRequestNanos;

            map.put("startOffset", startOffset);
            map.put("duration", duration);
        };
    }

    /**
     * This will snapshot this tracing and return a map of the results
     *
     * @return a snapshot of the tracing data
     */
    public Map<String, Object> snapshotTracingData() {

        Map<String, Object> traceMap = new LinkedHashMap<>();
        traceMap.put("version", 1L);
        traceMap.put("startTime", rfc3339(startRequestTime));
        traceMap.put("endTime", rfc3339(Instant.now()));
        traceMap.put("duration", System.nanoTime() - startRequestNanos);
        traceMap.put("parsing", copyMap(parseMap));
        traceMap.put("validation", copyMap(validationMap));
        traceMap.put("execution", executionData());

        return traceMap;
    }

    private Object copyMap(Map<String, Object> map) {
        Map<String, Object> mapCopy = new LinkedHashMap<>();
        mapCopy.putAll(map);
        return mapCopy;

    }

    private Map<String, Object> executionData() {
        Map<String, Object> map = new LinkedHashMap<>();
        List<Map<String, Object>> list = new ArrayList<>(fieldData);
        map.put("resolvers", list);
        return map;
    }

    private String rfc3339(Instant time) {
        return DateTimeFormatter.ISO_INSTANT.format(time);
    }

}
