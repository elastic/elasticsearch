package graphql.execution.instrumentation.dataloader;

import graphql.Assert;
import graphql.ExecutionResult;
import graphql.Internal;
import graphql.execution.ExecutionPath;
import graphql.execution.FieldValueInfo;
import graphql.execution.MergedField;
import graphql.execution.instrumentation.DeferredFieldInstrumentationContext;
import graphql.execution.instrumentation.ExecutionStrategyInstrumentationContext;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.parameters.InstrumentationDeferredFieldParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionStrategyParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import org.dataloader.DataLoaderRegistry;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * This approach uses field level tracking to achieve its aims of making the data loader more efficient
 */
@Internal
public class FieldLevelTrackingApproach {
    private final DataLoaderRegistry dataLoaderRegistry;
    private final Logger log;

    private static class CallStack implements InstrumentationState {

        private final Map<Integer, Integer> expectedFetchCountPerLevel = new LinkedHashMap<>();
        private final Map<Integer, Integer> fetchCountPerLevel = new LinkedHashMap<>();
        private final Map<Integer, Integer> expectedStrategyCallsPerLevel = new LinkedHashMap<>();
        private final Map<Integer, Integer> happenedStrategyCallsPerLevel = new LinkedHashMap<>();
        private final Map<Integer, Integer> happenedOnFieldValueCallsPerLevel = new LinkedHashMap<>();


        private final Set<Integer> dispatchedLevels = new LinkedHashSet<>();

        CallStack() {
            expectedStrategyCallsPerLevel.put(1, 1);
        }


        int increaseExpectedFetchCount(int level, int count) {
            expectedFetchCountPerLevel.put(level, expectedFetchCountPerLevel.getOrDefault(level, 0) + count);
            return expectedFetchCountPerLevel.get(level);
        }

        void increaseFetchCount(int level) {
            fetchCountPerLevel.put(level, fetchCountPerLevel.getOrDefault(level, 0) + 1);
        }

        void increaseExpectedStrategyCalls(int level, int count) {
            expectedStrategyCallsPerLevel.put(level, expectedStrategyCallsPerLevel.getOrDefault(level, 0) + count);
        }

        void increaseHappenedStrategyCalls(int level) {
            happenedStrategyCallsPerLevel.put(level, happenedStrategyCallsPerLevel.getOrDefault(level, 0) + 1);
        }

        void increaseHappenedOnFieldValueCalls(int level) {
            happenedOnFieldValueCallsPerLevel.put(level, happenedOnFieldValueCallsPerLevel.getOrDefault(level, 0) + 1);
        }

        boolean allStrategyCallsHappened(int level) {
            return Objects.equals(happenedStrategyCallsPerLevel.get(level), expectedStrategyCallsPerLevel.get(level));
        }

        boolean allOnFieldCallsHappened(int level) {
            return Objects.equals(happenedOnFieldValueCallsPerLevel.get(level), expectedStrategyCallsPerLevel.get(level));
        }

        boolean allFetchesHappened(int level) {
            return Objects.equals(fetchCountPerLevel.get(level), expectedFetchCountPerLevel.get(level));
        }

        @Override
        public String toString() {
            return "CallStack{" +
                    "expectedFetchCountPerLevel=" + expectedFetchCountPerLevel +
                    ", fetchCountPerLevel=" + fetchCountPerLevel +
                    ", expectedStrategyCallsPerLevel=" + expectedStrategyCallsPerLevel +
                    ", happenedStrategyCallsPerLevel=" + happenedStrategyCallsPerLevel +
                    ", happenedOnFieldValueCallsPerLevel=" + happenedOnFieldValueCallsPerLevel +
                    ", dispatchedLevels" + dispatchedLevels +
                    '}';
        }

        public boolean dispatchIfNotDispatchedBefore(int level) {
            if (dispatchedLevels.contains(level)) {
                Assert.assertShouldNeverHappen("level " + level + " already dispatched");
                return false;
            }
            dispatchedLevels.add(level);
            return true;
        }

        public void clearAndMarkCurrentLevelAsReady(int level) {
            expectedFetchCountPerLevel.clear();
            fetchCountPerLevel.clear();
            expectedStrategyCallsPerLevel.clear();
            happenedStrategyCallsPerLevel.clear();
            happenedOnFieldValueCallsPerLevel.clear();
            dispatchedLevels.clear();

            // make sure the level is ready
            expectedFetchCountPerLevel.put(level, 1);
            expectedStrategyCallsPerLevel.put(level, 1);
            happenedStrategyCallsPerLevel.put(level, 1);
        }
    }

    public FieldLevelTrackingApproach(Logger log, DataLoaderRegistry dataLoaderRegistry) {
        this.dataLoaderRegistry = dataLoaderRegistry;
        this.log = log;
    }

    public InstrumentationState createState() {
        return new CallStack();
    }

    ExecutionStrategyInstrumentationContext beginExecutionStrategy(InstrumentationExecutionStrategyParameters parameters) {
        CallStack callStack = parameters.getInstrumentationState();
        ExecutionPath path = parameters.getExecutionStrategyParameters().getPath();
        int parentLevel = path.getLevel();
        int curLevel = parentLevel + 1;
        int fieldCount = parameters.getExecutionStrategyParameters().getFields().size();
        synchronized (callStack) {
            callStack.increaseExpectedFetchCount(curLevel, fieldCount);
            callStack.increaseHappenedStrategyCalls(curLevel);
        }

        return new ExecutionStrategyInstrumentationContext() {
            @Override
            public void onDispatched(CompletableFuture<ExecutionResult> result) {

            }

            @Override
            public void onCompleted(ExecutionResult result, Throwable t) {

            }

            @Override
            public void onFieldValuesInfo(List<FieldValueInfo> fieldValueInfoList) {
                boolean dispatchNeeded;
                synchronized (callStack) {
                    dispatchNeeded = handleOnFieldValuesInfo(fieldValueInfoList, callStack, curLevel);
                }
                if (dispatchNeeded) {
                    dispatch();
                }
            }

            @Override
            public void onDeferredField(MergedField field) {
                boolean dispatchNeeded;
                // fake fetch count for this field
                synchronized (callStack) {
                    callStack.increaseFetchCount(curLevel);
                    dispatchNeeded = dispatchIfNeeded(callStack, curLevel);
                }
                if (dispatchNeeded) {
                    dispatch();
                }
            }
        };
    }

    //
    // thread safety : called with synchronised(callStack)
    //
    private boolean handleOnFieldValuesInfo(List<FieldValueInfo> fieldValueInfoList, CallStack callStack, int curLevel) {
        callStack.increaseHappenedOnFieldValueCalls(curLevel);
        int expectedStrategyCalls = 0;
        for (FieldValueInfo fieldValueInfo : fieldValueInfoList) {
            if (fieldValueInfo.getCompleteValueType() == FieldValueInfo.CompleteValueType.OBJECT) {
                expectedStrategyCalls++;
            } else if (fieldValueInfo.getCompleteValueType() == FieldValueInfo.CompleteValueType.LIST) {
                expectedStrategyCalls += getCountForList(fieldValueInfo);
            }
        }
        callStack.increaseExpectedStrategyCalls(curLevel + 1, expectedStrategyCalls);
        return dispatchIfNeeded(callStack, curLevel + 1);
    }

    private int getCountForList(FieldValueInfo fieldValueInfo) {
        int result = 0;
        for (FieldValueInfo cvi : fieldValueInfo.getFieldValueInfos()) {
            if (cvi.getCompleteValueType() == FieldValueInfo.CompleteValueType.OBJECT) {
                result++;
            } else if (cvi.getCompleteValueType() == FieldValueInfo.CompleteValueType.LIST) {
                result += getCountForList(cvi);
            }
        }
        return result;
    }

    DeferredFieldInstrumentationContext beginDeferredField(InstrumentationDeferredFieldParameters parameters) {
        CallStack callStack = parameters.getInstrumentationState();
        int level = parameters.getExecutionStrategyParameters().getPath().getLevel();
        synchronized (callStack) {
            callStack.clearAndMarkCurrentLevelAsReady(level);
        }

        return new DeferredFieldInstrumentationContext() {
            @Override
            public void onDispatched(CompletableFuture<ExecutionResult> result) {

            }

            @Override
            public void onCompleted(ExecutionResult result, Throwable t) {
            }

            @Override
            public void onFieldValueInfo(FieldValueInfo fieldValueInfo) {
                boolean dispatchNeeded;
                synchronized (callStack) {
                    dispatchNeeded = handleOnFieldValuesInfo(Collections.singletonList(fieldValueInfo), callStack, level);
                }
                if (dispatchNeeded) {
                    dispatch();
                }
            }
        };
    }

    public InstrumentationContext<Object> beginFieldFetch(InstrumentationFieldFetchParameters parameters) {
        CallStack callStack = parameters.getInstrumentationState();
        ExecutionPath path = parameters.getEnvironment().getExecutionStepInfo().getPath();
        int level = path.getLevel();
        return new InstrumentationContext<Object>() {

            @Override
            public void onDispatched(CompletableFuture result) {
                boolean dispatchNeeded;
                synchronized (callStack) {
                    callStack.increaseFetchCount(level);
                    dispatchNeeded = dispatchIfNeeded(callStack, level);
                }
                if (dispatchNeeded) {
                    dispatch();
                }

            }

            @Override
            public void onCompleted(Object result, Throwable t) {
            }
        };
    }


    //
    // thread safety : called with synchronised(callStack)
    //
    private boolean dispatchIfNeeded(CallStack callStack, int level) {
        if (levelReady(callStack, level)) {
            return callStack.dispatchIfNotDispatchedBefore(level);
        }
        return false;
    }

    //
    // thread safety : called with synchronised(callStack)
    //
    private boolean levelReady(CallStack callStack, int level) {
        if (level == 1) {
            // level 1 is special: there is only one strategy call and that's it
            return callStack.allFetchesHappened(1);
        }
        if (levelReady(callStack, level - 1) && callStack.allOnFieldCallsHappened(level - 1)
                && callStack.allStrategyCallsHappened(level) && callStack.allFetchesHappened(level)) {
            return true;
        }
        return false;
    }

    void dispatch() {
        log.debug("Dispatching data loaders ({})", dataLoaderRegistry.getKeys());
        dataLoaderRegistry.dispatchAll();
    }
}
