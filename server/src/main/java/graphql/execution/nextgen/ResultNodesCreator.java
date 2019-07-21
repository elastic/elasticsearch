package graphql.execution.nextgen;

import graphql.Internal;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.NonNullableFieldWasNullException;
import graphql.execution.nextgen.result.ExecutionResultNode;
import graphql.execution.nextgen.result.LeafExecutionResultNode;
import graphql.execution.nextgen.result.ListExecutionResultNode;
import graphql.execution.nextgen.result.ResolvedValue;
import graphql.execution.nextgen.result.UnresolvedObjectResultNode;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

@Internal
public class ResultNodesCreator {

    public ExecutionResultNode createResultNode(FetchedValueAnalysis fetchedValueAnalysis) {
        ResolvedValue resolvedValue = createResolvedValue(fetchedValueAnalysis);
        ExecutionStepInfo executionStepInfo = fetchedValueAnalysis.getExecutionStepInfo();

        if (fetchedValueAnalysis.isNullValue() && executionStepInfo.isNonNullType()) {
            NonNullableFieldWasNullException nonNullableFieldWasNullException = new NonNullableFieldWasNullException(executionStepInfo, executionStepInfo.getPath());

            return new LeafExecutionResultNode(executionStepInfo, resolvedValue, nonNullableFieldWasNullException);
        }
        if (fetchedValueAnalysis.isNullValue()) {
            return new LeafExecutionResultNode(executionStepInfo, resolvedValue, null);
        }
        if (fetchedValueAnalysis.getValueType() == FetchedValueAnalysis.FetchedValueType.OBJECT) {
            return createUnresolvedNode(fetchedValueAnalysis);
        }
        if (fetchedValueAnalysis.getValueType() == FetchedValueAnalysis.FetchedValueType.LIST) {
            return createListResultNode(fetchedValueAnalysis);
        }
        return new LeafExecutionResultNode(executionStepInfo, resolvedValue, null);
    }

    private ExecutionResultNode createUnresolvedNode(FetchedValueAnalysis fetchedValueAnalysis) {
        return new UnresolvedObjectResultNode(fetchedValueAnalysis.getExecutionStepInfo(), createResolvedValue(fetchedValueAnalysis));
    }

    private ResolvedValue createResolvedValue(FetchedValueAnalysis fetchedValueAnalysis) {
        return ResolvedValue.newResolvedValue()
                .completedValue(fetchedValueAnalysis.getCompletedValue())
                .localContext(fetchedValueAnalysis.getFetchedValue().getLocalContext())
                .nullValue(fetchedValueAnalysis.isNullValue())
                .errors(fetchedValueAnalysis.getErrors())
                .build();
    }

    private Optional<NonNullableFieldWasNullException> getFirstNonNullableException(Collection<ExecutionResultNode> collection) {
        return collection.stream()
                .filter(executionResultNode -> executionResultNode.getNonNullableFieldWasNullException() != null)
                .map(ExecutionResultNode::getNonNullableFieldWasNullException)
                .findFirst();
    }

    private ExecutionResultNode createListResultNode(FetchedValueAnalysis fetchedValueAnalysis) {
        List<ExecutionResultNode> executionResultNodes = fetchedValueAnalysis
                .getChildren()
                .stream()
                .map(this::createResultNode)
                .collect(toList());
        return new ListExecutionResultNode(fetchedValueAnalysis.getExecutionStepInfo(), createResolvedValue(fetchedValueAnalysis), executionResultNodes);
    }
}
