package graphql.execution.nextgen;

import graphql.Internal;
import graphql.execution.ExecutionContext;
import graphql.execution.nextgen.result.RootExecutionResultNode;

import java.util.concurrent.CompletableFuture;

@Internal
public interface ExecutionStrategy {

    CompletableFuture<RootExecutionResultNode> execute(ExecutionContext context, FieldSubSelection fieldSubSelection);

}
