package graphql.execution;


import graphql.DeferredExecutionResult;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.Internal;
import graphql.execution.defer.DeferSupport;
import graphql.execution.instrumentation.Instrumentation;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.parameters.InstrumentationExecuteOperationParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.language.Document;
import graphql.language.FragmentDefinition;
import graphql.language.NodeUtil;
import graphql.language.OperationDefinition;
import graphql.language.VariableDefinition;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static graphql.Assert.assertShouldNeverHappen;
import static graphql.execution.ExecutionContextBuilder.newExecutionContextBuilder;
import static graphql.execution.ExecutionStepInfo.newExecutionStepInfo;
import static graphql.execution.ExecutionStrategyParameters.newParameters;
import static graphql.language.OperationDefinition.Operation.MUTATION;
import static graphql.language.OperationDefinition.Operation.QUERY;
import static graphql.language.OperationDefinition.Operation.SUBSCRIPTION;
import static java.util.concurrent.CompletableFuture.completedFuture;

@Internal
public class Execution {
    private static final Logger log = LoggerFactory.getLogger(Execution.class);

    private final FieldCollector fieldCollector = new FieldCollector();
    private final ValuesResolver valuesResolver = new ValuesResolver();
    private final ExecutionStrategy queryStrategy;
    private final ExecutionStrategy mutationStrategy;
    private final ExecutionStrategy subscriptionStrategy;
    private final Instrumentation instrumentation;

    public Execution(ExecutionStrategy queryStrategy, ExecutionStrategy mutationStrategy, ExecutionStrategy subscriptionStrategy, Instrumentation instrumentation) {
        this.queryStrategy = queryStrategy != null ? queryStrategy : new AsyncExecutionStrategy();
        this.mutationStrategy = mutationStrategy != null ? mutationStrategy : new AsyncSerialExecutionStrategy();
        this.subscriptionStrategy = subscriptionStrategy != null ? subscriptionStrategy : new AsyncExecutionStrategy();
        this.instrumentation = instrumentation;
    }

    public CompletableFuture<ExecutionResult> execute(Document document, GraphQLSchema graphQLSchema, ExecutionId executionId, ExecutionInput executionInput, InstrumentationState instrumentationState) {

        NodeUtil.GetOperationResult getOperationResult = NodeUtil.getOperation(document, executionInput.getOperationName());
        Map<String, FragmentDefinition> fragmentsByName = getOperationResult.fragmentsByName;
        OperationDefinition operationDefinition = getOperationResult.operationDefinition;

        Map<String, Object> inputVariables = executionInput.getVariables();
        List<VariableDefinition> variableDefinitions = operationDefinition.getVariableDefinitions();

        Map<String, Object> coercedVariables;
        try {
            coercedVariables = valuesResolver.coerceArgumentValues(graphQLSchema, variableDefinitions, inputVariables);
        } catch (RuntimeException rte) {
            if (rte instanceof GraphQLError) {
                return completedFuture(new ExecutionResultImpl((GraphQLError) rte));
            }
            throw rte;
        }

        ExecutionContext executionContext = newExecutionContextBuilder()
                .instrumentation(instrumentation)
                .instrumentationState(instrumentationState)
                .executionId(executionId)
                .graphQLSchema(graphQLSchema)
                .queryStrategy(queryStrategy)
                .mutationStrategy(mutationStrategy)
                .subscriptionStrategy(subscriptionStrategy)
                .context(executionInput.getContext())
                .root(executionInput.getRoot())
                .fragmentsByName(fragmentsByName)
                .variables(coercedVariables)
                .document(document)
                .operationDefinition(operationDefinition)
                .dataLoaderRegistry(executionInput.getDataLoaderRegistry())
                .cacheControl(executionInput.getCacheControl())
                .build();


        InstrumentationExecutionParameters parameters = new InstrumentationExecutionParameters(
                executionInput, graphQLSchema, instrumentationState
        );
        executionContext = instrumentation.instrumentExecutionContext(executionContext, parameters);
        return executeOperation(executionContext, parameters, executionInput.getRoot(), executionContext.getOperationDefinition());
    }


    private CompletableFuture<ExecutionResult> executeOperation(ExecutionContext executionContext, InstrumentationExecutionParameters instrumentationExecutionParameters, Object root, OperationDefinition operationDefinition) {

        InstrumentationExecuteOperationParameters instrumentationParams = new InstrumentationExecuteOperationParameters(executionContext);
        InstrumentationContext<ExecutionResult> executeOperationCtx = instrumentation.beginExecuteOperation(instrumentationParams);

        OperationDefinition.Operation operation = operationDefinition.getOperation();
        GraphQLObjectType operationRootType;

        try {
            operationRootType = getOperationRootType(executionContext.getGraphQLSchema(), operationDefinition);
        } catch (RuntimeException rte) {
            if (rte instanceof GraphQLError) {
                ExecutionResult executionResult = new ExecutionResultImpl(Collections.singletonList((GraphQLError) rte));
                CompletableFuture<ExecutionResult> resultCompletableFuture = completedFuture(executionResult);

                executeOperationCtx.onDispatched(resultCompletableFuture);
                executeOperationCtx.onCompleted(executionResult, rte);
                return resultCompletableFuture;
            }
            throw rte;
        }

        FieldCollectorParameters collectorParameters = FieldCollectorParameters.newParameters()
                .schema(executionContext.getGraphQLSchema())
                .objectType(operationRootType)
                .fragments(executionContext.getFragmentsByName())
                .variables(executionContext.getVariables())
                .build();

        MergedSelectionSet fields = fieldCollector.collectFields(collectorParameters, operationDefinition.getSelectionSet());

        ExecutionPath path = ExecutionPath.rootPath();
        ExecutionStepInfo executionStepInfo = newExecutionStepInfo().type(operationRootType).path(path).build();
        NonNullableFieldValidator nonNullableFieldValidator = new NonNullableFieldValidator(executionContext, executionStepInfo);

        ExecutionStrategyParameters parameters = newParameters()
                .executionStepInfo(executionStepInfo)
                .source(root)
                .localContext(executionContext.getContext())
                .fields(fields)
                .nonNullFieldValidator(nonNullableFieldValidator)
                .path(path)
                .build();

        CompletableFuture<ExecutionResult> result;
        try {
            ExecutionStrategy executionStrategy;
            if (operation == OperationDefinition.Operation.MUTATION) {
                executionStrategy = mutationStrategy;
            } else if (operation == SUBSCRIPTION) {
                executionStrategy = subscriptionStrategy;
            } else {
                executionStrategy = queryStrategy;
            }
            log.debug("Executing '{}' query operation: '{}' using '{}' execution strategy", executionContext.getExecutionId(), operation, executionStrategy.getClass().getName());
            result = executionStrategy.execute(executionContext, parameters);
        } catch (NonNullableFieldWasNullException e) {
            // this means it was non null types all the way from an offending non null type
            // up to the root object type and there was a a null value some where.
            //
            // The spec says we should return null for the data in this case
            //
            // http://facebook.github.io/graphql/#sec-Errors-and-Non-Nullability
            //
            result = completedFuture(new ExecutionResultImpl(null, executionContext.getErrors()));
        }

        // note this happens NOW - not when the result completes
        executeOperationCtx.onDispatched(result);

        result = result.whenComplete(executeOperationCtx::onCompleted);

        return deferSupport(executionContext, result);
    }

    /*
     * Adds the deferred publisher if its needed at the end of the query.  This is also a good time for the deferred code to start running
     */
    private CompletableFuture<ExecutionResult> deferSupport(ExecutionContext executionContext, CompletableFuture<ExecutionResult> result) {
        return result.thenApply(er -> {
            DeferSupport deferSupport = executionContext.getDeferSupport();
            if (deferSupport.isDeferDetected()) {
                // we start the rest of the query now to maximize throughput.  We have the initial important results
                // and now we can start the rest of the calls as early as possible (even before some one subscribes)
                Publisher<DeferredExecutionResult> publisher = deferSupport.startDeferredCalls();
                return ExecutionResultImpl.newExecutionResult().from(er)
                        .addExtension(GraphQL.DEFERRED_RESULTS, publisher)
                        .build();
            }
            return er;
        });

    }

    private GraphQLObjectType getOperationRootType(GraphQLSchema graphQLSchema, OperationDefinition operationDefinition) {
        OperationDefinition.Operation operation = operationDefinition.getOperation();
        if (operation == MUTATION) {
            GraphQLObjectType mutationType = graphQLSchema.getMutationType();
            if (mutationType == null) {
                throw new MissingRootTypeException("Schema is not configured for mutations.", operationDefinition.getSourceLocation());
            }
            return mutationType;
        } else if (operation == QUERY) {
            GraphQLObjectType queryType = graphQLSchema.getQueryType();
            if (queryType == null) {
                throw new MissingRootTypeException("Schema does not define the required query root type.", operationDefinition.getSourceLocation());
            }
            return queryType;
        } else if (operation == SUBSCRIPTION) {
            GraphQLObjectType subscriptionType = graphQLSchema.getSubscriptionType();
            if (subscriptionType == null) {
                throw new MissingRootTypeException("Schema is not configured for subscriptions.", operationDefinition.getSourceLocation());
            }
            return subscriptionType;
        } else {
            return assertShouldNeverHappen("Unhandled case.  An extra operation enum has been added without code support");
        }
    }
}
