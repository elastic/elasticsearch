package graphql.execution;

import graphql.ExecutionResult;
import graphql.PublicApi;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static graphql.Assert.assertNotNull;

@PublicApi
public class FieldValueInfo {

    public enum CompleteValueType {
        OBJECT,
        LIST,
        NULL,
        SCALAR,
        ENUM

    }

    private final CompleteValueType completeValueType;
    private final CompletableFuture<ExecutionResult> fieldValue;
    private final List<FieldValueInfo> fieldValueInfos;

    private FieldValueInfo(CompleteValueType completeValueType, CompletableFuture<ExecutionResult> fieldValue, List<FieldValueInfo> fieldValueInfos) {
        assertNotNull(fieldValueInfos, "fieldValueInfos can't be null");
        this.completeValueType = completeValueType;
        this.fieldValue = fieldValue;
        this.fieldValueInfos = fieldValueInfos;
    }

    public CompleteValueType getCompleteValueType() {
        return completeValueType;
    }

    public CompletableFuture<ExecutionResult> getFieldValue() {
        return fieldValue;
    }

    public List<FieldValueInfo> getFieldValueInfos() {
        return fieldValueInfos;
    }

    public static Builder newFieldValueInfo(CompleteValueType completeValueType) {
        return new Builder(completeValueType);
    }

    @Override
    public String toString() {
        return "FieldValueInfo{" +
                "completeValueType=" + completeValueType +
                ", fieldValue=" + fieldValue +
                ", fieldValueInfos=" + fieldValueInfos +
                '}';
    }

    @SuppressWarnings("unused")
    public static class Builder {
        private CompleteValueType completeValueType;
        private CompletableFuture<ExecutionResult> executionResultFuture;
        private List<FieldValueInfo> listInfos = new ArrayList<>();

        public Builder(CompleteValueType completeValueType) {
            this.completeValueType = completeValueType;
        }

        public Builder completeValueType(CompleteValueType completeValueType) {
            this.completeValueType = completeValueType;
            return this;
        }

        public Builder fieldValue(CompletableFuture<ExecutionResult> executionResultFuture) {
            this.executionResultFuture = executionResultFuture;
            return this;
        }

        public Builder fieldValueInfos(List<FieldValueInfo> listInfos) {
            assertNotNull(listInfos, "fieldValueInfos can't be null");
            this.listInfos = listInfos;
            return this;
        }

        public FieldValueInfo build() {
            return new FieldValueInfo(completeValueType, executionResultFuture, listInfos);
        }
    }
}