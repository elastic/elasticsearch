package graphql.execution.instrumentation;

import graphql.ExecutionResult;
import graphql.execution.FieldValueInfo;
import graphql.execution.MergedField;

import java.util.List;

public interface ExecutionStrategyInstrumentationContext extends InstrumentationContext<ExecutionResult> {

    default void onFieldValuesInfo(List<FieldValueInfo> fieldValueInfoList) {

    }

    default void onDeferredField(MergedField field) {

    }
}
