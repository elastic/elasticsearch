package graphql.execution.preparsed;


import graphql.ExecutionInput;

import java.util.function.Function;

public class NoOpPreparsedDocumentProvider implements PreparsedDocumentProvider {
    public static final NoOpPreparsedDocumentProvider INSTANCE = new NoOpPreparsedDocumentProvider();

    @Override
    public PreparsedDocumentEntry getDocument(ExecutionInput executionInput, Function<ExecutionInput, PreparsedDocumentEntry> computeFunction) {
        return computeFunction.apply(executionInput);
    }
}
