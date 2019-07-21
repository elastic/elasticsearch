package graphql.execution.instrumentation.nextgen;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.Internal;
import graphql.execution.instrumentation.DocumentAndVariables;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.language.Document;
import graphql.schema.GraphQLSchema;
import graphql.validation.ValidationError;

import java.util.List;

import static graphql.execution.instrumentation.SimpleInstrumentationContext.noOp;

@Internal
public interface Instrumentation {

    default InstrumentationState createState(InstrumentationCreateStateParameters parameters) {
        return new InstrumentationState() {
        };
    }

    default ExecutionInput instrumentExecutionInput(ExecutionInput executionInput, InstrumentationExecutionParameters parameters) {
        return executionInput;
    }

    default DocumentAndVariables instrumentDocumentAndVariables(DocumentAndVariables documentAndVariables, InstrumentationExecutionParameters parameters) {
        return documentAndVariables;
    }

    default GraphQLSchema instrumentSchema(GraphQLSchema graphQLSchema, InstrumentationExecutionParameters parameters) {
        return graphQLSchema;
    }

    default ExecutionResult instrumentExecutionResult(ExecutionResult result, InstrumentationExecutionParameters parameters) {
        return result;
    }

    default InstrumentationContext<ExecutionResult> beginExecution(InstrumentationExecutionParameters parameters) {
        return noOp();
    }

    default InstrumentationContext<Document> beginParse(InstrumentationExecutionParameters parameters) {
        return noOp();
    }

    default InstrumentationContext<List<ValidationError>> beginValidation(InstrumentationValidationParameters parameters) {
        return noOp();
    }
}
