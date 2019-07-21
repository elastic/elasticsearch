package graphql.execution.nextgen;

import graphql.Internal;
import graphql.execution.Async;
import graphql.execution.ExecutionContext;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.ExecutionStepInfoFactory;
import graphql.execution.FetchedValue;
import graphql.execution.FieldCollector;
import graphql.execution.FieldCollectorParameters;
import graphql.execution.MergedField;
import graphql.execution.MergedSelectionSet;
import graphql.execution.ResolveType;
import graphql.execution.nextgen.result.ExecutionResultNode;
import graphql.execution.nextgen.result.ResolvedValue;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.util.FpKit;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static graphql.execution.FieldCollectorParameters.newParameters;

@Internal
public class ExecutionStrategyUtil {

    ExecutionStepInfoFactory executionStepInfoFactory = new ExecutionStepInfoFactory();
    FetchedValueAnalyzer fetchedValueAnalyzer = new FetchedValueAnalyzer();
    ValueFetcher valueFetcher = new ValueFetcher();
    ResultNodesCreator resultNodesCreator = new ResultNodesCreator();
    ResolveType resolveType = new ResolveType();
    FieldCollector fieldCollector = new FieldCollector();

    public List<CompletableFuture<ExecutionResultNode>> fetchSubSelection(ExecutionContext executionContext, FieldSubSelection fieldSubSelection) {
        List<CompletableFuture<FetchedValueAnalysis>> fetchedValueAnalysisList = fetchAndAnalyze(executionContext, fieldSubSelection);
        return fetchedValueAnalysisToNodesAsync(fetchedValueAnalysisList);
    }

    private List<CompletableFuture<FetchedValueAnalysis>> fetchAndAnalyze(ExecutionContext context, FieldSubSelection fieldSubSelection) {

        return FpKit.map(fieldSubSelection.getMergedSelectionSet().getSubFieldsList(),
                mergedField -> fetchAndAnalyzeField(context, fieldSubSelection.getSource(), fieldSubSelection.getLocalContext(), mergedField, fieldSubSelection.getExecutionStepInfo()));

    }

    private CompletableFuture<FetchedValueAnalysis> fetchAndAnalyzeField(ExecutionContext context, Object source, Object localContext, MergedField mergedField,
                                                                         ExecutionStepInfo executionStepInfo) {

        ExecutionStepInfo newExecutionStepInfo = executionStepInfoFactory.newExecutionStepInfoForSubField(context, mergedField, executionStepInfo);
        return valueFetcher
                .fetchValue(context, source, localContext, mergedField, newExecutionStepInfo)
                .thenApply(fetchValue -> analyseValue(context, fetchValue, newExecutionStepInfo));
    }

    private List<CompletableFuture<ExecutionResultNode>> fetchedValueAnalysisToNodesAsync(List<CompletableFuture<FetchedValueAnalysis>> list) {
        return Async.map(list, fetchedValueAnalysis -> resultNodesCreator.createResultNode(fetchedValueAnalysis));
    }

    public List<ExecutionResultNode> fetchedValueAnalysisToNodes(List<FetchedValueAnalysis> fetchedValueAnalysisList) {
        return FpKit.map(fetchedValueAnalysisList, fetchedValueAnalysis -> resultNodesCreator.createResultNode(fetchedValueAnalysis));
    }


    private FetchedValueAnalysis analyseValue(ExecutionContext executionContext, FetchedValue fetchedValue, ExecutionStepInfo executionInfo) {
        FetchedValueAnalysis fetchedValueAnalysis = fetchedValueAnalyzer.analyzeFetchedValue(executionContext, fetchedValue, executionInfo);
        return fetchedValueAnalysis;
    }

    public FieldSubSelection createFieldSubSelection(ExecutionContext executionContext, ExecutionStepInfo executionInfo, ResolvedValue resolvedValue) {
        MergedField field = executionInfo.getField();
        Object source = resolvedValue.getCompletedValue();
        Object localContext = resolvedValue.getLocalContext();

        GraphQLOutputType sourceType = executionInfo.getUnwrappedNonNullType();
        GraphQLObjectType resolvedObjectType = resolveType.resolveType(executionContext, field, source, executionInfo.getArguments(), sourceType);
        FieldCollectorParameters collectorParameters = newParameters()
                .schema(executionContext.getGraphQLSchema())
                .objectType(resolvedObjectType)
                .fragments(executionContext.getFragmentsByName())
                .variables(executionContext.getVariables())
                .build();
        MergedSelectionSet subFields = fieldCollector.collectFields(collectorParameters,
                executionInfo.getField());

        // it is not really a new step but rather a refinement
        ExecutionStepInfo newExecutionStepInfoWithResolvedType = executionInfo.changeTypeWithPreservedNonNull(resolvedObjectType);

        return FieldSubSelection.newFieldSubSelection()
                .source(source)
                .localContext(localContext)
                .mergedSelectionSet(subFields)
                .executionInfo(newExecutionStepInfoWithResolvedType)
                .build();
    }


}
