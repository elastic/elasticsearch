package graphql.execution.nextgen;

import graphql.ExecutionInput;
import graphql.Internal;
import graphql.execution.ExecutionContext;
import graphql.execution.ExecutionId;
import graphql.execution.ExecutionPath;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.FieldCollector;
import graphql.execution.FieldCollectorParameters;
import graphql.execution.MergedSelectionSet;
import graphql.execution.ValuesResolver;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.language.Document;
import graphql.language.FragmentDefinition;
import graphql.language.NodeUtil;
import graphql.language.OperationDefinition;
import graphql.language.VariableDefinition;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;

import java.util.List;
import java.util.Map;

import static graphql.execution.ExecutionContextBuilder.newExecutionContextBuilder;
import static graphql.execution.ExecutionStepInfo.newExecutionStepInfo;

@Internal
public class ExecutionHelper {

    private final FieldCollector fieldCollector = new FieldCollector();

    public static class ExecutionData {
        public ExecutionContext executionContext;
        public FieldSubSelection fieldSubSelection;
    }

    public ExecutionData createExecutionData(Document document,
                                             GraphQLSchema graphQLSchema,
                                             ExecutionId executionId,
                                             ExecutionInput executionInput,
                                             InstrumentationState instrumentationState) {

        NodeUtil.GetOperationResult getOperationResult = NodeUtil.getOperation(document, executionInput.getOperationName());
        Map<String, FragmentDefinition> fragmentsByName = getOperationResult.fragmentsByName;
        OperationDefinition operationDefinition = getOperationResult.operationDefinition;

        ValuesResolver valuesResolver = new ValuesResolver();
        Map<String, Object> inputVariables = executionInput.getVariables();
        List<VariableDefinition> variableDefinitions = operationDefinition.getVariableDefinitions();

        Map<String, Object> coercedVariables;
        coercedVariables = valuesResolver.coerceArgumentValues(graphQLSchema, variableDefinitions, inputVariables);

        ExecutionContext executionContext = newExecutionContextBuilder()
                .executionId(executionId)
                .instrumentationState(instrumentationState)
                .graphQLSchema(graphQLSchema)
                .context(executionInput.getContext())
                .root(executionInput.getRoot())
                .fragmentsByName(fragmentsByName)
                .variables(coercedVariables)
                .document(document)
                .operationDefinition(operationDefinition)
                .build();

        GraphQLObjectType operationRootType;
        operationRootType = Common.getOperationRootType(executionContext.getGraphQLSchema(), operationDefinition);
        FieldCollectorParameters collectorParameters = FieldCollectorParameters.newParameters()
                .schema(executionContext.getGraphQLSchema())
                .objectType(operationRootType)
                .fragments(executionContext.getFragmentsByName())
                .variables(executionContext.getVariables())
                .build();
        MergedSelectionSet mergedSelectionSet = fieldCollector.collectFields(collectorParameters, operationDefinition.getSelectionSet());
        ExecutionStepInfo executionInfo = newExecutionStepInfo().type(operationRootType).path(ExecutionPath.rootPath()).build();

        FieldSubSelection fieldSubSelection = FieldSubSelection.newFieldSubSelection()
                .source(executionInput.getRoot())
                .localContext(executionInput.getContext())
                .mergedSelectionSet(mergedSelectionSet)
                .executionInfo(executionInfo)
                .build();

        ExecutionData executionData = new ExecutionData();
        executionData.executionContext = executionContext;
        executionData.fieldSubSelection = fieldSubSelection;
        return executionData;

    }
}
