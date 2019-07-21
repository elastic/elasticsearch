package graphql.execution.nextgen;

import graphql.Internal;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.MergedField;
import graphql.execution.MergedSelectionSet;

import java.util.Map;


/**
 * A map from name to List of Field representing the actual sub selections (during execution) of a Field with Fragments
 * evaluated and conditional directives considered.
 */
@Internal
public class FieldSubSelection {

    private final Object source;
    private final Object localContext;
    // the type of this must be objectType and is the parent executionStepInfo for all mergedSelectionSet
    private final ExecutionStepInfo executionInfo;
    private final MergedSelectionSet mergedSelectionSet;

    private FieldSubSelection(Builder builder) {
        this.source = builder.source;
        this.localContext = builder.localContext;
        this.executionInfo = builder.executionInfo;
        this.mergedSelectionSet = builder.mergedSelectionSet;
    }

    public Object getSource() {
        return source;
    }

    public Object getLocalContext() {
        return localContext;
    }

    public Map<String, MergedField> getSubFields() {
        return mergedSelectionSet.getSubFields();
    }

    public MergedSelectionSet getMergedSelectionSet() {
        return mergedSelectionSet;
    }

    public ExecutionStepInfo getExecutionStepInfo() {
        return executionInfo;
    }

    @Override
    public String toString() {
        return "FieldSubSelection{" +
                "source=" + source +
                ", executionInfo=" + executionInfo +
                ", mergedSelectionSet" + mergedSelectionSet +
                '}';
    }

    public static Builder newFieldSubSelection() {
        return new Builder();
    }

    public static class Builder {
        private Object source;
        private Object localContext;
        private ExecutionStepInfo executionInfo;
        private MergedSelectionSet mergedSelectionSet;

        public Builder source(Object source) {
            this.source = source;
            return this;
        }

        public Builder localContext(Object localContext) {
            this.localContext = localContext;
            return this;
        }

        public Builder executionInfo(ExecutionStepInfo executionInfo) {
            this.executionInfo = executionInfo;
            return this;
        }

        public Builder mergedSelectionSet(MergedSelectionSet mergedSelectionSet) {
            this.mergedSelectionSet = mergedSelectionSet;
            return this;
        }

        public FieldSubSelection build() {
            return new FieldSubSelection(this);
        }


    }

}
