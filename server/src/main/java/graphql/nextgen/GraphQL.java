package graphql.nextgen;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.Internal;
import graphql.ParseResult;
import graphql.execution.AbortExecutionException;
import graphql.execution.ExecutionId;
import graphql.execution.ExecutionIdProvider;
import graphql.execution.instrumentation.DocumentAndVariables;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.nextgen.Instrumentation;
import graphql.execution.instrumentation.nextgen.InstrumentationCreateStateParameters;
import graphql.execution.instrumentation.nextgen.InstrumentationExecutionParameters;
import graphql.execution.instrumentation.nextgen.InstrumentationValidationParameters;
import graphql.execution.nextgen.DefaultExecutionStrategy;
import graphql.execution.nextgen.Execution;
import graphql.execution.nextgen.ExecutionStrategy;
import graphql.execution.preparsed.NoOpPreparsedDocumentProvider;
import graphql.execution.preparsed.PreparsedDocumentEntry;
import graphql.execution.preparsed.PreparsedDocumentProvider;
import graphql.language.Document;
import graphql.parser.InvalidSyntaxException;
import graphql.parser.Parser;
import graphql.schema.GraphQLSchema;
import graphql.validation.ValidationError;
import graphql.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static graphql.Assert.assertNotNull;
import static graphql.execution.instrumentation.DocumentAndVariables.newDocumentAndVariables;

@SuppressWarnings("Duplicates")
@Internal
public class GraphQL {
    private static final Logger log = LoggerFactory.getLogger(graphql.GraphQL.class);

    private final GraphQLSchema graphQLSchema;
    private final ExecutionStrategy executionStrategy;
    private final ExecutionIdProvider idProvider;
    private final Instrumentation instrumentation;
    private final PreparsedDocumentProvider preparsedDocumentProvider;

    public GraphQL(Builder builder) {
        this.graphQLSchema = builder.graphQLSchema;
        this.executionStrategy = builder.executionStrategy;
        this.idProvider = builder.idProvider;
        this.preparsedDocumentProvider = builder.preparsedDocumentProvider;
        this.instrumentation = builder.instrumentation;
    }

    /**
     * Executes the graphql query using the provided input object builder
     * <p>
     * This will return a completed {@link ExecutionResult}
     * which is the result of executing the provided query.
     *
     * @param executionInputBuilder {@link ExecutionInput.Builder}
     *
     * @return an {@link ExecutionResult} which can include errors
     */
    public ExecutionResult execute(ExecutionInput.Builder executionInputBuilder) {
        return executeAsync(executionInputBuilder.build()).join();
    }

    /**
     * Executes the graphql query using the provided input object builder
     * <p>
     * This will return a completed {@link ExecutionResult}
     * which is the result of executing the provided query.
     * <p>
     * This allows a lambda style like :
     * <pre>
     * {@code
     *    ExecutionResult result = graphql.execute(input -> input.query("{hello}").root(startingObj).context(contextObj));
     * }
     * </pre>
     *
     * @param builderFunction a function that is given a {@link ExecutionInput.Builder}
     *
     * @return a promise to an {@link ExecutionResult} which can include errors
     */
    public CompletableFuture<ExecutionResult> execute(UnaryOperator<ExecutionInput.Builder> builderFunction) {
        return executeAsync(builderFunction.apply(ExecutionInput.newExecutionInput()).build());
    }

    /**
     * Executes the graphql query using the provided input object
     * <p>
     * This will return a completed {@link ExecutionResult}
     * which is the result of executing the provided query.
     *
     * @param executionInput {@link ExecutionInput}
     *
     * @return a promise to an {@link ExecutionResult} which can include errors
     */
    public ExecutionResult execute(ExecutionInput executionInput) {
        return executeAsync(executionInput).join();
    }

    /**
     * Executes the graphql query using the provided input object builder
     * <p>
     * This will return a promise (aka {@link CompletableFuture}) to provide a {@link ExecutionResult}
     * which is the result of executing the provided query.
     *
     * @param executionInputBuilder {@link ExecutionInput.Builder}
     *
     * @return a promise to an {@link ExecutionResult} which can include errors
     */
    public CompletableFuture<ExecutionResult> executeAsync(ExecutionInput.Builder executionInputBuilder) {
        return executeAsync(executionInputBuilder.build());
    }

    /**
     * Executes the graphql query using the provided input object builder
     * <p>
     * This will return a promise (aka {@link CompletableFuture}) to provide a {@link ExecutionResult}
     * which is the result of executing the provided query.
     * <p>
     * This allows a lambda style like :
     * <pre>
     * {@code
     *    ExecutionResult result = graphql.executeAsync(input -> input.query("{hello}").root(startingObj).context(contextObj));
     * }
     * </pre>
     *
     * @param builderFunction a function that is given a {@link ExecutionInput.Builder}
     *
     * @return a promise to an {@link ExecutionResult} which can include errors
     */
    public CompletableFuture<ExecutionResult> executeAsync(UnaryOperator<ExecutionInput.Builder> builderFunction) {
        return executeAsync(builderFunction.apply(ExecutionInput.newExecutionInput()).build());
    }

    /**
     * Executes the graphql query using the provided input object
     * <p>
     * This will return a promise (aka {@link CompletableFuture}) to provide a {@link ExecutionResult}
     * which is the result of executing the provided query.
     *
     * @param executionInput {@link ExecutionInput}
     *
     * @return a promise to an {@link ExecutionResult} which can include errors
     */
    public CompletableFuture<ExecutionResult> executeAsync(ExecutionInput executionInput) {
        try {
            log.debug("Executing request. operation name: '{}'. query: '{}'. variables '{}'", executionInput.getOperationName(), executionInput.getQuery(), executionInput.getVariables());

            InstrumentationState instrumentationState = instrumentation.createState(new InstrumentationCreateStateParameters(this.graphQLSchema, executionInput));

            InstrumentationExecutionParameters inputInstrumentationParameters = new InstrumentationExecutionParameters(executionInput, this.graphQLSchema, instrumentationState);
            executionInput = instrumentation.instrumentExecutionInput(executionInput, inputInstrumentationParameters);

            InstrumentationExecutionParameters instrumentationParameters = new InstrumentationExecutionParameters(executionInput, this.graphQLSchema, instrumentationState);
            InstrumentationContext<ExecutionResult> executionInstrumentation = instrumentation.beginExecution(instrumentationParameters);

            GraphQLSchema graphQLSchema = instrumentation.instrumentSchema(this.graphQLSchema, instrumentationParameters);

            CompletableFuture<ExecutionResult> executionResult = parseValidateAndExecute(executionInput, graphQLSchema, instrumentationState);
            //
            // finish up instrumentation
            executionResult = executionResult.whenComplete(executionInstrumentation::onCompleted);
            //
            // allow instrumentation to tweak the result
            executionResult = executionResult.thenApply(result -> instrumentation.instrumentExecutionResult(result, instrumentationParameters));
            return executionResult;
        } catch (AbortExecutionException abortException) {
            return CompletableFuture.completedFuture(abortException.toExecutionResult());
        }
    }

    private CompletableFuture<ExecutionResult> parseValidateAndExecute(ExecutionInput executionInput, GraphQLSchema graphQLSchema, InstrumentationState instrumentationState) {
        AtomicReference<ExecutionInput> executionInputRef = new AtomicReference<>(executionInput);
        Function<ExecutionInput, PreparsedDocumentEntry> computeFunction = transformedInput -> {
            // if they change the original query in the pre-parser, then we want to see it downstream from then on
            executionInputRef.set(transformedInput);
            return parseAndValidate(executionInputRef.get(), graphQLSchema, instrumentationState);
        };
        PreparsedDocumentEntry preparsedDoc = preparsedDocumentProvider.getDocument(executionInput,computeFunction);
        if (preparsedDoc.hasErrors()) {
            return CompletableFuture.completedFuture(new ExecutionResultImpl(preparsedDoc.getErrors()));
        }
        return execute(executionInputRef.get(), preparsedDoc.getDocument(), graphQLSchema, instrumentationState);
    }

    private PreparsedDocumentEntry parseAndValidate(ExecutionInput executionInput, GraphQLSchema graphQLSchema, InstrumentationState instrumentationState) {
        log.debug("Parsing query: '{}'...", executionInput.getQuery());
        ParseResult parseResult = parse(executionInput, graphQLSchema, instrumentationState);
        if (parseResult.isFailure()) {
            log.warn("Query failed to parse : '{}'", executionInput.getQuery());
            return new PreparsedDocumentEntry(parseResult.getException().toInvalidSyntaxError());
        } else {
            final Document document = parseResult.getDocument();

            log.debug("Validating query: '{}'", executionInput.getQuery());
            final List<ValidationError> errors = validate(executionInput, document, graphQLSchema, instrumentationState);
            if (!errors.isEmpty()) {
                log.warn("Query failed to validate : '{}'", executionInput.getQuery());
                return new PreparsedDocumentEntry(errors);
            }

            return new PreparsedDocumentEntry(document);
        }
    }

    private ParseResult parse(ExecutionInput executionInput, GraphQLSchema graphQLSchema, InstrumentationState instrumentationState) {
        InstrumentationExecutionParameters parameters = new InstrumentationExecutionParameters(executionInput, graphQLSchema, instrumentationState);
        InstrumentationContext<Document> parseInstrumentation = instrumentation.beginParse(parameters);

        Parser parser = new Parser();
        Document document;
        DocumentAndVariables documentAndVariables;
        try {
            document = parser.parseDocument(executionInput.getQuery());
            documentAndVariables = newDocumentAndVariables()
                    .document(document).variables(executionInput.getVariables()).build();
            documentAndVariables = instrumentation.instrumentDocumentAndVariables(documentAndVariables, parameters);
        } catch (InvalidSyntaxException e) {
            parseInstrumentation.onCompleted(null, e);
            return ParseResult.ofError(e);
        }

        parseInstrumentation.onCompleted(documentAndVariables.getDocument(), null);
        return ParseResult.of(documentAndVariables);
    }

    private List<ValidationError> validate(ExecutionInput executionInput, Document document, GraphQLSchema graphQLSchema, InstrumentationState instrumentationState) {
        InstrumentationContext<List<ValidationError>> validationCtx = instrumentation.beginValidation(new InstrumentationValidationParameters(executionInput, document, graphQLSchema, instrumentationState));

        Validator validator = new Validator();
        List<ValidationError> validationErrors = validator.validateDocument(graphQLSchema, document);

        validationCtx.onCompleted(validationErrors, null);
        return validationErrors;
    }

    private CompletableFuture<ExecutionResult> execute(ExecutionInput executionInput, Document document, GraphQLSchema graphQLSchema, InstrumentationState instrumentationState) {
        String query = executionInput.getQuery();
        String operationName = executionInput.getOperationName();
        Object context = executionInput.getContext();

        Execution execution = new Execution();
        ExecutionId executionId = idProvider.provide(query, operationName, context);

        log.debug("Executing '{}'. operation name: '{}'. query: '{}'. variables '{}'", executionId, executionInput.getOperationName(), executionInput.getQuery(), executionInput.getVariables());
        CompletableFuture<ExecutionResult> future = execution.execute(executionStrategy, document, graphQLSchema, executionId, executionInput, instrumentationState);
        future = future.whenComplete((result, throwable) -> {
            if (throwable != null) {
                log.error(String.format("Execution '%s' threw exception when executing : query : '%s'. variables '%s'", executionId, executionInput.getQuery(), executionInput.getVariables()), throwable);
            } else {
                int errorCount = result.getErrors().size();
                if (errorCount > 0) {
                    log.debug("Execution '{}' completed with '{}' errors", executionId, errorCount);
                } else {
                    log.debug("Execution '{}' completed with zero errors", executionId);
                }
            }
        });
        return future;
    }

    /**
     * Helps you build a GraphQL object ready to execute queries
     *
     * @param graphQLSchema the schema to use
     *
     * @return a builder of GraphQL objects
     */
    public static Builder newGraphQL(GraphQLSchema graphQLSchema) {
        return new Builder(graphQLSchema);
    }

    /**
     * This helps you transform the current GraphQL object into another one by starting a builder with all
     * the current values and allows you to transform it how you want.
     *
     * @param builderConsumer the consumer code that will be given a builder to transform
     *
     * @return a new GraphQL object based on calling build on that builder
     */
    public GraphQL transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }


    public static class Builder {
        private GraphQLSchema graphQLSchema;
        private ExecutionStrategy executionStrategy = new DefaultExecutionStrategy();
        private ExecutionIdProvider idProvider = ExecutionIdProvider.DEFAULT_EXECUTION_ID_PROVIDER;
        private Instrumentation instrumentation = new Instrumentation() {
        };
        private PreparsedDocumentProvider preparsedDocumentProvider = NoOpPreparsedDocumentProvider.INSTANCE;


        public Builder(GraphQLSchema graphQLSchema) {
            this.graphQLSchema = graphQLSchema;
        }

        public Builder(GraphQL graphQL) {
            this.graphQLSchema = graphQL.graphQLSchema;
            this.executionStrategy = graphQL.executionStrategy;
            this.idProvider = graphQL.idProvider;
            this.instrumentation = graphQL.instrumentation;
        }

        public Builder schema(GraphQLSchema graphQLSchema) {
            this.graphQLSchema = assertNotNull(graphQLSchema, "GraphQLSchema must be non null");
            return this;
        }

        public Builder executionStrategy(ExecutionStrategy executionStrategy) {
            this.executionStrategy = assertNotNull(executionStrategy, "ExecutionStrategy must be non null");
            return this;
        }

        public Builder instrumentation(Instrumentation instrumentation) {
            this.instrumentation = assertNotNull(instrumentation, "Instrumentation must be non null");
            return this;
        }

        public Builder preparsedDocumentProvider(PreparsedDocumentProvider preparsedDocumentProvider) {
            this.preparsedDocumentProvider = assertNotNull(preparsedDocumentProvider, "PreparsedDocumentProvider must be non null");
            return this;
        }

        public Builder executionIdProvider(ExecutionIdProvider executionIdProvider) {
            this.idProvider = assertNotNull(executionIdProvider, "ExecutionIdProvider must be non null");
            return this;
        }

        public GraphQL build() {
            assertNotNull(graphQLSchema, "graphQLSchema must be non null");
            return new GraphQL(this);
        }
    }
}
