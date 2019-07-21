package graphql.execution.instrumentation;

import graphql.ExecutionResult;
import graphql.execution.FieldValueInfo;

public interface DeferredFieldInstrumentationContext extends InstrumentationContext<ExecutionResult> {

    default void onFieldValueInfo(FieldValueInfo fieldValueInfo) {

    }

}
