package graphql.execution;

import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.GraphQLError;
import graphql.Internal;
import graphql.PublicSpi;
import graphql.SerializationError;
import graphql.TrivialDataFetcher;
import graphql.TypeMismatchError;
import graphql.UnresolvedTypeError;
import graphql.execution.directives.QueryDirectivesImpl;
import graphql.execution.instrumentation.Instrumentation;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.parameters.InstrumentationFieldCompleteParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldParameters;
import graphql.introspection.Introspection;
import graphql.language.Argument;
import graphql.language.Field;
import graphql.schema.CoercingSerializeException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import graphql.schema.DataFetchingFieldSelectionSetImpl;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.util.FpKit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static graphql.execution.Async.exceptionallyCompletedFuture;
import static graphql.execution.ExecutionStepInfo.newExecutionStepInfo;
import static graphql.execution.FieldCollectorParameters.newParameters;
import static graphql.execution.FieldValueInfo.CompleteValueType.ENUM;
import static graphql.execution.FieldValueInfo.CompleteValueType.LIST;
import static graphql.execution.FieldValueInfo.CompleteValueType.NULL;
import static graphql.execution.FieldValueInfo.CompleteValueType.OBJECT;
import static graphql.execution.FieldValueInfo.CompleteValueType.SCALAR;
import static graphql.schema.DataFetchingEnvironmentImpl.newDataFetchingEnvironment;
import static graphql.schema.GraphQLTypeUtil.isList;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * An execution strategy is give a list of fields from the graphql query to execute and find values for using a recursive strategy.
 * <pre>
 *     query {
 *          friends {
 *              id
 *              name
 *              friends {
 *                  id
 *                  name
 *              }
 *          }
 *          enemies {
 *              id
 *              name
 *              allies {
 *                  id
 *                  name
 *              }
 *          }
 *     }
 *
 * </pre>
 * <p>
 * Given the graphql query above, an execution strategy will be called for the top level fields 'friends' and 'enemies' and it will be asked to find an object
 * to describe them.  Because they are both complex object types, it needs to descend down that query and start fetching and completing
 * fields such as 'id','name' and other complex fields such as 'friends' and 'allies', by recursively calling to itself to execute these lower
 * field layers
 * <p>
 * The execution of a field has two phases, first a raw object must be fetched for a field via a {@link DataFetcher} which
 * is defined on the {@link GraphQLFieldDefinition}.  This object must then be 'completed' into a suitable value, either as a scalar/enum type via
 * coercion or if its a complex object type by recursively calling the execution strategy for the lower level fields.
 * <p>
 * The first phase (data fetching) is handled by the method {@link #fetchField(ExecutionContext, ExecutionStrategyParameters)}
 * <p>
 * The second phase (value completion) is handled by the methods {@link #completeField(ExecutionContext, ExecutionStrategyParameters, FetchedValue)}
 * and the other "completeXXX" methods.
 * <p>
 * The order of fields fetching and completion is up to the execution strategy. As the graphql specification
 * <a href="http://facebook.github.io/graphql/#sec-Normal-and-Serial-Execution">http://facebook.github.io/graphql/#sec-Normal-and-Serial-Execution</a> says:
 * <blockquote>
 * Normally the executor can execute the entries in a grouped field set in whatever order it chooses (often in parallel). Because
 * the resolution of fields other than top-level mutation fields must always be side effect-free and idempotent, the
 * execution order must not affect the result, and hence the server has the freedom to execute the
 * field entries in whatever order it deems optimal.
 * </blockquote>
 * <p>
 * So in the case above you could execute the fields depth first ('friends' and its sub fields then do 'enemies' and its sub fields or it
 * could do breadth first ('fiends' and 'enemies' data fetch first and then all the sub fields) or in parallel via asynchronous
 * facilities like {@link CompletableFuture}s.
 * <p>
 * {@link #execute(ExecutionContext, ExecutionStrategyParameters)} is the entry point of the execution strategy.
 */
@PublicSpi
@SuppressWarnings("FutureReturnValueIgnored")
public abstract class ExecutionStrategy {

    private static final Logger log = LoggerFactory.getLogger(ExecutionStrategy.class);

    protected final ValuesResolver valuesResolver = new ValuesResolver();
    protected final FieldCollector fieldCollector = new FieldCollector();
    private final ExecutionStepInfoFactory executionStepInfoFactory = new ExecutionStepInfoFactory();
    private final ResolveType resolvedType = new ResolveType();

    protected final DataFetcherExceptionHandler dataFetcherExceptionHandler;

    /**
     * The default execution strategy constructor uses the {@link SimpleDataFetcherExceptionHandler}
     * for data fetching errors.
     */
    protected ExecutionStrategy() {
        dataFetcherExceptionHandler = new SimpleDataFetcherExceptionHandler();
    }

    /**
     * The consumers of the execution strategy can pass in a {@link DataFetcherExceptionHandler} to better
     * decide what do when a data fetching error happens
     *
     * @param dataFetcherExceptionHandler the callback invoked if an exception happens during data fetching
     */
    protected ExecutionStrategy(DataFetcherExceptionHandler dataFetcherExceptionHandler) {
        this.dataFetcherExceptionHandler = dataFetcherExceptionHandler;
    }

    /**
     * This is the entry point to an execution strategy.  It will be passed the fields to execute and get values for.
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     *
     * @return a promise to an {@link ExecutionResult}
     *
     * @throws NonNullableFieldWasNullException in the future if a non null field resolves to a null value
     */
    public abstract CompletableFuture<ExecutionResult> execute(ExecutionContext executionContext, ExecutionStrategyParameters parameters) throws NonNullableFieldWasNullException;

    /**
     * Called to fetch a value for a field and resolve it further in terms of the graphql query.  This will call
     * #fetchField followed by #completeField and the completed {@link ExecutionResult} is returned.
     * <p>
     * An execution strategy can iterate the fields to be executed and call this method for each one
     * <p>
     * Graphql fragments mean that for any give logical field can have one or more {@link Field} values associated with it
     * in the query, hence the fieldList.  However the first entry is representative of the field for most purposes.
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     *
     * @return a promise to an {@link ExecutionResult}
     *
     * @throws NonNullableFieldWasNullException in the future if a non null field resolves to a null value
     */
    protected CompletableFuture<ExecutionResult> resolveField(ExecutionContext executionContext, ExecutionStrategyParameters parameters) {
        return resolveFieldWithInfo(executionContext, parameters).thenCompose(FieldValueInfo::getFieldValue);
    }

    /**
     * Called to fetch a value for a field and its extra runtime info and resolve it further in terms of the graphql query.  This will call
     * #fetchField followed by #completeField and the completed {@link graphql.execution.FieldValueInfo} is returned.
     * <p>
     * An execution strategy can iterate the fields to be executed and call this method for each one
     * <p>
     * Graphql fragments mean that for any give logical field can have one or more {@link Field} values associated with it
     * in the query, hence the fieldList.  However the first entry is representative of the field for most purposes.
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     *
     * @return a promise to a {@link FieldValueInfo}
     *
     * @throws NonNullableFieldWasNullException in the {@link FieldValueInfo#getFieldValue()} future if a non null field resolves to a null value
     */
    protected CompletableFuture<FieldValueInfo> resolveFieldWithInfo(ExecutionContext executionContext, ExecutionStrategyParameters parameters) {
        GraphQLFieldDefinition fieldDef = getFieldDef(executionContext, parameters, parameters.getField().getSingleField());

        Instrumentation instrumentation = executionContext.getInstrumentation();
        InstrumentationContext<ExecutionResult> fieldCtx = instrumentation.beginField(
                new InstrumentationFieldParameters(executionContext, fieldDef, createExecutionStepInfo(executionContext, parameters, fieldDef, null))
        );

        CompletableFuture<FetchedValue> fetchFieldFuture = fetchField(executionContext, parameters);
        CompletableFuture<FieldValueInfo> result = fetchFieldFuture.thenApply((fetchedValue) ->
                completeField(executionContext, parameters, fetchedValue));

        CompletableFuture<ExecutionResult> executionResultFuture = result.thenCompose(FieldValueInfo::getFieldValue);

        fieldCtx.onDispatched(executionResultFuture);
        executionResultFuture.whenComplete(fieldCtx::onCompleted);
        return result;
    }

    protected CompletableFuture<FieldValueInfo> resolveFieldWithInfoToNull(ExecutionContext executionContext, ExecutionStrategyParameters parameters) {
        FetchedValue fetchedValue = FetchedValue.newFetchedValue().build();
        FieldValueInfo fieldValueInfo = completeField(executionContext, parameters, fetchedValue);
        return CompletableFuture.completedFuture(fieldValueInfo);
    }


    /**
     * Called to fetch a value for a field from the {@link DataFetcher} associated with the field
     * {@link GraphQLFieldDefinition}.
     * <p>
     * Graphql fragments mean that for any give logical field can have one or more {@link Field} values associated with it
     * in the query, hence the fieldList.  However the first entry is representative of the field for most purposes.
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     *
     * @return a promise to a fetched object
     *
     * @throws NonNullableFieldWasNullException in the future if a non null field resolves to a null value
     */
    protected CompletableFuture<FetchedValue> fetchField(ExecutionContext executionContext, ExecutionStrategyParameters parameters) {
        MergedField field = parameters.getField();
        GraphQLObjectType parentType = (GraphQLObjectType) parameters.getExecutionStepInfo().getUnwrappedNonNullType();
        GraphQLFieldDefinition fieldDef = getFieldDef(executionContext.getGraphQLSchema(), parentType, field.getSingleField());

        GraphQLCodeRegistry codeRegistry = executionContext.getGraphQLSchema().getCodeRegistry();
        Map<String, Object> argumentValues = valuesResolver.getArgumentValues(codeRegistry, fieldDef.getArguments(), field.getArguments(), executionContext.getVariables());

        QueryDirectivesImpl queryDirectives = new QueryDirectivesImpl(field, executionContext.getGraphQLSchema(), executionContext.getVariables());

        GraphQLOutputType fieldType = fieldDef.getType();
        DataFetchingFieldSelectionSet fieldCollector = DataFetchingFieldSelectionSetImpl.newCollector(executionContext, fieldType, parameters.getField());
        ExecutionStepInfo executionStepInfo = createExecutionStepInfo(executionContext, parameters, fieldDef, parentType);


        DataFetchingEnvironment environment = newDataFetchingEnvironment(executionContext)
                .source(parameters.getSource())
                .localContext(parameters.getLocalContext())
                .arguments(argumentValues)
                .fieldDefinition(fieldDef)
                .mergedField(parameters.getField())
                .fieldType(fieldType)
                .executionStepInfo(executionStepInfo)
                .parentType(parentType)
                .selectionSet(fieldCollector)
                .queryDirectives(queryDirectives)
                .build();

        DataFetcher dataFetcher = codeRegistry.getDataFetcher(parentType, fieldDef);

        Instrumentation instrumentation = executionContext.getInstrumentation();

        InstrumentationFieldFetchParameters instrumentationFieldFetchParams = new InstrumentationFieldFetchParameters(executionContext, fieldDef, environment, parameters, dataFetcher instanceof TrivialDataFetcher);
        InstrumentationContext<Object> fetchCtx = instrumentation.beginFieldFetch(instrumentationFieldFetchParams);

        CompletableFuture<Object> fetchedValue;
        dataFetcher = instrumentation.instrumentDataFetcher(dataFetcher, instrumentationFieldFetchParams);
        ExecutionId executionId = executionContext.getExecutionId();
        try {
            log.debug("'{}' fetching field '{}' using data fetcher '{}'...", executionId, executionStepInfo.getPath(), dataFetcher.getClass().getName());
            Object fetchedValueRaw = dataFetcher.get(environment);
            log.debug("'{}' field '{}' fetch returned '{}'", executionId, executionStepInfo.getPath(), fetchedValueRaw == null ? "null" : fetchedValueRaw.getClass().getName());

            fetchedValue = Async.toCompletableFuture(fetchedValueRaw);
        } catch (Exception e) {
            log.debug(String.format("'%s', field '%s' fetch threw exception", executionId, executionStepInfo.getPath()), e);

            fetchedValue = new CompletableFuture<>();
            fetchedValue.completeExceptionally(e);
        }
        fetchCtx.onDispatched(fetchedValue);
        return fetchedValue
                .handle((result, exception) -> {
                    fetchCtx.onCompleted(result, exception);
                    if (exception != null) {
                        handleFetchingException(executionContext, parameters, environment, exception);
                        return null;
                    } else {
                        return result;
                    }
                })
                .thenApply(result -> unboxPossibleDataFetcherResult(executionContext, parameters, result));
    }

    FetchedValue unboxPossibleDataFetcherResult(ExecutionContext executionContext,
                                                ExecutionStrategyParameters parameters,
                                                Object result) {

        if (result instanceof DataFetcherResult) {
            //noinspection unchecked
            DataFetcherResult<?> dataFetcherResult = (DataFetcherResult) result;
            if (dataFetcherResult.isMapRelativeErrors()) {
                dataFetcherResult.getErrors().stream()
                        .map(relError -> new AbsoluteGraphQLError(parameters, relError))
                        .forEach(executionContext::addError);
            } else {
                dataFetcherResult.getErrors().forEach(executionContext::addError);
            }

            Object localContext = dataFetcherResult.getLocalContext();
            if (localContext == null) {
                // if the field returns nothing then they get the context of their parent field
                localContext = parameters.getLocalContext();
            }
            return FetchedValue.newFetchedValue()
                    .fetchedValue(UnboxPossibleOptional.unboxPossibleOptional(dataFetcherResult.getData()))
                    .rawFetchedValue(dataFetcherResult.getData())
                    .errors(dataFetcherResult.getErrors())
                    .localContext(localContext)
                    .build();
        } else {
            return FetchedValue.newFetchedValue()
                    .fetchedValue(UnboxPossibleOptional.unboxPossibleOptional(result))
                    .rawFetchedValue(result)
                    .localContext(parameters.getLocalContext())
                    .build();
        }
    }

    private void handleFetchingException(ExecutionContext executionContext,
                                         ExecutionStrategyParameters parameters,
                                         DataFetchingEnvironment environment,
                                         Throwable e) {
        DataFetcherExceptionHandlerParameters handlerParameters = DataFetcherExceptionHandlerParameters.newExceptionParameters()
                .dataFetchingEnvironment(environment)
                .exception(e)
                .build();

        DataFetcherExceptionHandlerResult handlerResult = dataFetcherExceptionHandler.onException(handlerParameters);
        handlerResult.getErrors().forEach(executionContext::addError);

        parameters.deferredErrorSupport().onFetchingException(parameters, e);
    }

    /**
     * Called to complete a field based on the type of the field.
     * <p>
     * If the field is a scalar type, then it will be coerced  and returned.  However if the field type is an complex object type, then
     * the execution strategy will be called recursively again to execute the fields of that type before returning.
     * <p>
     * Graphql fragments mean that for any give logical field can have one or more {@link Field} values associated with it
     * in the query, hence the fieldList.  However the first entry is representative of the field for most purposes.
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     * @param fetchedValue     the fetched raw value
     *
     * @return a {@link FieldValueInfo}
     *
     * @throws NonNullableFieldWasNullException in the {@link FieldValueInfo#getFieldValue()} future if a non null field resolves to a null value
     */
    protected FieldValueInfo completeField(ExecutionContext executionContext, ExecutionStrategyParameters parameters, FetchedValue fetchedValue) {
        Field field = parameters.getField().getSingleField();
        GraphQLObjectType parentType = (GraphQLObjectType) parameters.getExecutionStepInfo().getUnwrappedNonNullType();
        GraphQLFieldDefinition fieldDef = getFieldDef(executionContext.getGraphQLSchema(), parentType, field);
        ExecutionStepInfo executionStepInfo = createExecutionStepInfo(executionContext, parameters, fieldDef, parentType);

        Instrumentation instrumentation = executionContext.getInstrumentation();
        InstrumentationFieldCompleteParameters instrumentationParams = new InstrumentationFieldCompleteParameters(executionContext, parameters, fieldDef, executionStepInfo, fetchedValue);
        InstrumentationContext<ExecutionResult> ctxCompleteField = instrumentation.beginFieldComplete(
                instrumentationParams
        );

        GraphQLCodeRegistry codeRegistry = executionContext.getGraphQLSchema().getCodeRegistry();
        Map<String, Object> argumentValues = valuesResolver.getArgumentValues(codeRegistry, fieldDef.getArguments(), field.getArguments(), executionContext.getVariables());

        NonNullableFieldValidator nonNullableFieldValidator = new NonNullableFieldValidator(executionContext, executionStepInfo);

        ExecutionStrategyParameters newParameters = parameters.transform(builder ->
                builder.executionStepInfo(executionStepInfo)
                        .arguments(argumentValues)
                        .source(fetchedValue.getFetchedValue())
                        .localContext(fetchedValue.getLocalContext())
                        .nonNullFieldValidator(nonNullableFieldValidator)
        );

        log.debug("'{}' completing field '{}'...", executionContext.getExecutionId(), executionStepInfo.getPath());

        FieldValueInfo fieldValueInfo = completeValue(executionContext, newParameters);

        CompletableFuture<ExecutionResult> executionResultFuture = fieldValueInfo.getFieldValue();
        ctxCompleteField.onDispatched(executionResultFuture);
        executionResultFuture.whenComplete(ctxCompleteField::onCompleted);
        return fieldValueInfo;
    }


    /**
     * Called to complete a value for a field based on the type of the field.
     * <p>
     * If the field is a scalar type, then it will be coerced  and returned.  However if the field type is an complex object type, then
     * the execution strategy will be called recursively again to execute the fields of that type before returning.
     * <p>
     * Graphql fragments mean that for any give logical field can have one or more {@link Field} values associated with it
     * in the query, hence the fieldList.  However the first entry is representative of the field for most purposes.
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     *
     * @return a {@link FieldValueInfo}
     *
     * @throws NonNullableFieldWasNullException if a non null field resolves to a null value
     */
    protected FieldValueInfo completeValue(ExecutionContext executionContext, ExecutionStrategyParameters parameters) throws NonNullableFieldWasNullException {
        ExecutionStepInfo executionStepInfo = parameters.getExecutionStepInfo();
        Object result = UnboxPossibleOptional.unboxPossibleOptional(parameters.getSource());
        GraphQLType fieldType = executionStepInfo.getUnwrappedNonNullType();
        CompletableFuture<ExecutionResult> fieldValue;

        if (result == null) {
            fieldValue = completeValueForNull(parameters);
            return FieldValueInfo.newFieldValueInfo(NULL).fieldValue(fieldValue).build();
        } else if (isList(fieldType)) {
            return completeValueForList(executionContext, parameters, result);
        } else if (fieldType instanceof GraphQLScalarType) {
            fieldValue = completeValueForScalar(executionContext, parameters, (GraphQLScalarType) fieldType, result);
            return FieldValueInfo.newFieldValueInfo(SCALAR).fieldValue(fieldValue).build();
        } else if (fieldType instanceof GraphQLEnumType) {
            fieldValue = completeValueForEnum(executionContext, parameters, (GraphQLEnumType) fieldType, result);
            return FieldValueInfo.newFieldValueInfo(ENUM).fieldValue(fieldValue).build();
        }

        // when we are here, we have a complex type: Interface, Union or Object
        // and we must go deeper
        //
        GraphQLObjectType resolvedObjectType;
        try {
            resolvedObjectType = resolveType(executionContext, parameters, fieldType);
            fieldValue = completeValueForObject(executionContext, parameters, resolvedObjectType, result);
        } catch (UnresolvedTypeException ex) {
            // consider the result to be null and add the error on the context
            handleUnresolvedTypeProblem(executionContext, parameters, ex);
            // and validate the field is nullable, if non-nullable throw exception
            parameters.getNonNullFieldValidator().validate(parameters.getPath(), null);
            // complete the field as null
            fieldValue = completedFuture(new ExecutionResultImpl(null, null));
        }
        return FieldValueInfo.newFieldValueInfo(OBJECT).fieldValue(fieldValue).build();
    }

    private void handleUnresolvedTypeProblem(ExecutionContext context, ExecutionStrategyParameters parameters, UnresolvedTypeException e) {
        UnresolvedTypeError error = new UnresolvedTypeError(parameters.getPath(), parameters.getExecutionStepInfo(), e);
        log.warn(error.getMessage(), e);
        context.addError(error);

        parameters.deferredErrorSupport().onError(error);
    }

    private CompletableFuture<ExecutionResult> completeValueForNull(ExecutionStrategyParameters parameters) {
        return Async.tryCatch(() -> {
            Object nullValue = parameters.getNonNullFieldValidator().validate(parameters.getPath(), null);
            return completedFuture(new ExecutionResultImpl(nullValue, null));
        });
    }

    /**
     * Called to complete a list of value for a field based on a list type.  This iterates the values and calls
     * {@link #completeValue(ExecutionContext, ExecutionStrategyParameters)} for each value.
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     * @param result           the result to complete, raw result
     *
     * @return a {@link FieldValueInfo}
     */
    protected FieldValueInfo completeValueForList(ExecutionContext executionContext, ExecutionStrategyParameters parameters, Object result) {
        Iterable<Object> resultIterable = toIterable(executionContext, parameters, result);
        try {
            resultIterable = parameters.getNonNullFieldValidator().validate(parameters.getPath(), resultIterable);
        } catch (NonNullableFieldWasNullException e) {
            return FieldValueInfo.newFieldValueInfo(LIST).fieldValue(exceptionallyCompletedFuture(e)).build();
        }
        if (resultIterable == null) {
            return FieldValueInfo.newFieldValueInfo(LIST).fieldValue(completedFuture(new ExecutionResultImpl(null, null))).build();
        }
        return completeValueForList(executionContext, parameters, resultIterable);
    }

    /**
     * Called to complete a list of value for a field based on a list type.  This iterates the values and calls
     * {@link #completeValue(ExecutionContext, ExecutionStrategyParameters)} for each value.
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     * @param iterableValues   the values to complete, can't be null
     *
     * @return a {@link FieldValueInfo}
     */
    protected FieldValueInfo completeValueForList(ExecutionContext executionContext, ExecutionStrategyParameters parameters, Iterable<Object> iterableValues) {

        Collection<Object> values = FpKit.toCollection(iterableValues);
        ExecutionStepInfo executionStepInfo = parameters.getExecutionStepInfo();
        GraphQLFieldDefinition fieldDef = parameters.getExecutionStepInfo().getFieldDefinition();
        GraphQLObjectType fieldContainer = parameters.getExecutionStepInfo().getFieldContainer();

        InstrumentationFieldCompleteParameters instrumentationParams = new InstrumentationFieldCompleteParameters(executionContext, parameters, fieldDef, createExecutionStepInfo(executionContext, parameters, fieldDef, fieldContainer), values);
        Instrumentation instrumentation = executionContext.getInstrumentation();

        InstrumentationContext<ExecutionResult> completeListCtx = instrumentation.beginFieldListComplete(
                instrumentationParams
        );

        List<FieldValueInfo> fieldValueInfos = new ArrayList<>();
        int index = 0;
        for (Object item : values) {
            ExecutionPath indexedPath = parameters.getPath().segment(index);

            ExecutionStepInfo stepInfoForListElement = executionStepInfoFactory.newExecutionStepInfoForListElement(executionStepInfo, index);

            NonNullableFieldValidator nonNullableFieldValidator = new NonNullableFieldValidator(executionContext, stepInfoForListElement);

            int finalIndex = index;
            FetchedValue value = unboxPossibleDataFetcherResult(executionContext, parameters, item);

            ExecutionStrategyParameters newParameters = parameters.transform(builder ->
                    builder.executionStepInfo(stepInfoForListElement)
                            .nonNullFieldValidator(nonNullableFieldValidator)
                            .listSize(values.size())
                            .localContext(value.getLocalContext())
                            .currentListIndex(finalIndex)
                            .path(indexedPath)
                            .source(value.getFetchedValue())
            );
            fieldValueInfos.add(completeValue(executionContext, newParameters));
            index++;
        }

        CompletableFuture<List<ExecutionResult>> resultsFuture = Async.each(fieldValueInfos, (item, i) -> item.getFieldValue());

        CompletableFuture<ExecutionResult> overallResult = new CompletableFuture<>();
        completeListCtx.onDispatched(overallResult);

        resultsFuture.whenComplete((results, exception) -> {
            if (exception != null) {
                ExecutionResult executionResult = handleNonNullException(executionContext, overallResult, exception);
                completeListCtx.onCompleted(executionResult, exception);
                return;
            }
            List<Object> completedResults = new ArrayList<>();
            for (ExecutionResult completedValue : results) {
                completedResults.add(completedValue.getData());
            }
            ExecutionResultImpl executionResult = new ExecutionResultImpl(completedResults, null);
            overallResult.complete(executionResult);
        });
        overallResult.whenComplete(completeListCtx::onCompleted);

        return FieldValueInfo.newFieldValueInfo(LIST)
                .fieldValue(overallResult)
                .fieldValueInfos(fieldValueInfos)
                .build();
    }

    /**
     * Called to turn an object into a scalar value according to the {@link GraphQLScalarType} by asking that scalar type to coerce the object
     * into a valid value
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     * @param scalarType       the type of the scalar
     * @param result           the result to be coerced
     *
     * @return a promise to an {@link ExecutionResult}
     */
    protected CompletableFuture<ExecutionResult> completeValueForScalar(ExecutionContext executionContext, ExecutionStrategyParameters parameters, GraphQLScalarType scalarType, Object result) {
        Object serialized;
        try {
            serialized = scalarType.getCoercing().serialize(result);
        } catch (CoercingSerializeException e) {
            serialized = handleCoercionProblem(executionContext, parameters, e);
        }

        // TODO: fix that: this should not be handled here
        //6.6.1 http://facebook.github.io/graphql/#sec-Field-entries
        if (serialized instanceof Double && ((Double) serialized).isNaN()) {
            serialized = null;
        }
        try {
            serialized = parameters.getNonNullFieldValidator().validate(parameters.getPath(), serialized);
        } catch (NonNullableFieldWasNullException e) {
            return exceptionallyCompletedFuture(e);
        }
        return completedFuture(new ExecutionResultImpl(serialized, null));
    }

    /**
     * Called to turn an object into a enum value according to the {@link GraphQLEnumType} by asking that enum type to coerce the object into a valid value
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     * @param enumType         the type of the enum
     * @param result           the result to be coerced
     *
     * @return a promise to an {@link ExecutionResult}
     */
    protected CompletableFuture<ExecutionResult> completeValueForEnum(ExecutionContext executionContext, ExecutionStrategyParameters parameters, GraphQLEnumType enumType, Object result) {
        Object serialized;
        try {
            serialized = enumType.getCoercing().serialize(result);
        } catch (CoercingSerializeException e) {
            serialized = handleCoercionProblem(executionContext, parameters, e);
        }
        try {
            serialized = parameters.getNonNullFieldValidator().validate(parameters.getPath(), serialized);
        } catch (NonNullableFieldWasNullException e) {
            return exceptionallyCompletedFuture(e);
        }
        return completedFuture(new ExecutionResultImpl(serialized, null));
    }

    /**
     * Called to turn an java object value into an graphql object value
     *
     * @param executionContext   contains the top level execution parameters
     * @param parameters         contains the parameters holding the fields to be executed and source object
     * @param resolvedObjectType the resolved object type
     * @param result             the result to be coerced
     *
     * @return a promise to an {@link ExecutionResult}
     */
    protected CompletableFuture<ExecutionResult> completeValueForObject(ExecutionContext executionContext, ExecutionStrategyParameters parameters, GraphQLObjectType resolvedObjectType, Object result) {
        ExecutionStepInfo executionStepInfo = parameters.getExecutionStepInfo();

        FieldCollectorParameters collectorParameters = newParameters()
                .schema(executionContext.getGraphQLSchema())
                .objectType(resolvedObjectType)
                .fragments(executionContext.getFragmentsByName())
                .variables(executionContext.getVariables())
                .build();

        MergedSelectionSet subFields = fieldCollector.collectFields(collectorParameters, parameters.getField());

        ExecutionStepInfo newExecutionStepInfo = executionStepInfo.changeTypeWithPreservedNonNull(resolvedObjectType);
        NonNullableFieldValidator nonNullableFieldValidator = new NonNullableFieldValidator(executionContext, newExecutionStepInfo);

        ExecutionStrategyParameters newParameters = parameters.transform(builder ->
                builder.executionStepInfo(newExecutionStepInfo)
                        .fields(subFields)
                        .nonNullFieldValidator(nonNullableFieldValidator)
                        .source(result)
        );

        // Calling this from the executionContext to ensure we shift back from mutation strategy to the query strategy.

        return executionContext.getQueryStrategy().execute(executionContext, newParameters);
    }

    @SuppressWarnings("SameReturnValue")
    private Object handleCoercionProblem(ExecutionContext context, ExecutionStrategyParameters parameters, CoercingSerializeException e) {
        SerializationError error = new SerializationError(parameters.getPath(), e);
        log.warn(error.getMessage(), e);
        context.addError(error);

        parameters.deferredErrorSupport().onError(error);

        return null;
    }


    /**
     * Converts an object that is known to should be an Iterable into one
     *
     * @param result the result object
     *
     * @return an Iterable from that object
     *
     * @throws java.lang.ClassCastException if its not an Iterable
     */
    @SuppressWarnings("unchecked")
    protected Iterable<Object> toIterable(Object result) {
        return FpKit.toCollection(result);
    }

    protected GraphQLObjectType resolveType(ExecutionContext executionContext, ExecutionStrategyParameters parameters, GraphQLType fieldType) {
        return resolvedType.resolveType(executionContext, parameters.getField(), parameters.getSource(), parameters.getArguments(), fieldType);
    }


    protected Iterable<Object> toIterable(ExecutionContext context, ExecutionStrategyParameters parameters, Object result) {
        if (result.getClass().isArray() || result instanceof Iterable) {
            return toIterable(result);
        }

        handleTypeMismatchProblem(context, parameters, result);
        return null;
    }

    private void handleTypeMismatchProblem(ExecutionContext context, ExecutionStrategyParameters parameters, Object result) {
        TypeMismatchError error = new TypeMismatchError(parameters.getPath(), parameters.getExecutionStepInfo().getUnwrappedNonNullType());
        log.warn("{} got {}", error.getMessage(), result.getClass());
        context.addError(error);

        parameters.deferredErrorSupport().onError(error);
    }


    /**
     * Called to discover the field definition give the current parameters and the AST {@link Field}
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     * @param field            the field to find the definition of
     *
     * @return a {@link GraphQLFieldDefinition}
     */
    protected GraphQLFieldDefinition getFieldDef(ExecutionContext executionContext, ExecutionStrategyParameters parameters, Field field) {
        GraphQLObjectType parentType = (GraphQLObjectType) parameters.getExecutionStepInfo().getUnwrappedNonNullType();
        return getFieldDef(executionContext.getGraphQLSchema(), parentType, field);
    }

    /**
     * Called to discover the field definition give the current parameters and the AST {@link Field}
     *
     * @param schema     the schema in play
     * @param parentType the parent type of the field
     * @param field      the field to find the definition of
     *
     * @return a {@link GraphQLFieldDefinition}
     */
    protected GraphQLFieldDefinition getFieldDef(GraphQLSchema schema, GraphQLObjectType parentType, Field field) {
        return Introspection.getFieldDef(schema, parentType, field.getName());
    }

    /**
     * See (http://facebook.github.io/graphql/#sec-Errors-and-Non-Nullability),
     * <p>
     * If a non nullable child field type actually resolves to a null value and the parent type is nullable
     * then the parent must in fact become null
     * so we use exceptions to indicate this special case.  However if the parent is in fact a non nullable type
     * itself then we need to bubble that upwards again until we get to the root in which case the result
     * is meant to be null.
     *
     * @param e this indicates that a null value was returned for a non null field, which needs to cause the parent field
     *          to become null OR continue on as an exception
     *
     * @throws NonNullableFieldWasNullException if a non null field resolves to a null value
     */
    protected void assertNonNullFieldPrecondition(NonNullableFieldWasNullException e) throws NonNullableFieldWasNullException {
        ExecutionStepInfo executionStepInfo = e.getExecutionStepInfo();
        if (executionStepInfo.hasParent() && executionStepInfo.getParent().isNonNullType()) {
            throw new NonNullableFieldWasNullException(e);
        }
    }

    protected void assertNonNullFieldPrecondition(NonNullableFieldWasNullException e, CompletableFuture<?> completableFuture) throws NonNullableFieldWasNullException {
        ExecutionStepInfo executionStepInfo = e.getExecutionStepInfo();
        if (executionStepInfo.hasParent() && executionStepInfo.getParent().isNonNullType()) {
            completableFuture.completeExceptionally(new NonNullableFieldWasNullException(e));
        }
    }

    protected ExecutionResult handleNonNullException(ExecutionContext executionContext, CompletableFuture<ExecutionResult> result, Throwable e) {
        ExecutionResult executionResult = null;
        List<GraphQLError> errors = new ArrayList<>(executionContext.getErrors());
        Throwable underlyingException = e;
        if (e instanceof CompletionException) {
            underlyingException = e.getCause();
        }
        if (underlyingException instanceof NonNullableFieldWasNullException) {
            assertNonNullFieldPrecondition((NonNullableFieldWasNullException) underlyingException, result);
            if (!result.isDone()) {
                executionResult = new ExecutionResultImpl(null, errors);
                result.complete(executionResult);
            }
        } else if (underlyingException instanceof AbortExecutionException) {
            AbortExecutionException abortException = (AbortExecutionException) underlyingException;
            executionResult = abortException.toExecutionResult();
            result.complete(executionResult);
        } else {
            result.completeExceptionally(e);
        }
        return executionResult;
    }


    /**
     * Builds the type info hierarchy for the current field
     *
     * @param executionContext the execution context  in play
     * @param parameters       contains the parameters holding the fields to be executed and source object
     * @param fieldDefinition  the field definition to build type info for
     * @param fieldContainer   the field container
     *
     * @return a new type info
     */
    protected ExecutionStepInfo createExecutionStepInfo(ExecutionContext executionContext,
                                                        ExecutionStrategyParameters parameters,
                                                        GraphQLFieldDefinition fieldDefinition,
                                                        GraphQLObjectType fieldContainer) {
        GraphQLOutputType fieldType = fieldDefinition.getType();
        MergedField field = parameters.getField();
        List<Argument> fieldArgs = field.getArguments();
        GraphQLCodeRegistry codeRegistry = executionContext.getGraphQLSchema().getCodeRegistry();
        Map<String, Object> argumentValues = valuesResolver.getArgumentValues(codeRegistry, fieldDefinition.getArguments(), fieldArgs, executionContext.getVariables());

        return newExecutionStepInfo()
                .type(fieldType)
                .fieldDefinition(fieldDefinition)
                .fieldContainer(fieldContainer)
                .field(field)
                .path(parameters.getPath())
                .parentInfo(parameters.getExecutionStepInfo())
                .arguments(argumentValues)
                .build();

    }


    @Internal
    public static String mkNameForPath(Field currentField) {
        return mkNameForPath(Collections.singletonList(currentField));
    }

    @Internal
    public static String mkNameForPath(MergedField mergedField) {
        return mkNameForPath(mergedField.getFields());
    }


    @Internal
    public static String mkNameForPath(List<Field> currentField) {
        Field field = currentField.get(0);
        return field.getAlias() != null ? field.getAlias() : field.getName();
    }
}
