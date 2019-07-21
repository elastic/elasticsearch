package graphql.execution;

import graphql.GraphQLError;
import graphql.Internal;
import graphql.PublicApi;
import graphql.cachecontrol.CacheControl;
import graphql.execution.instrumentation.Instrumentation;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.language.Document;
import graphql.language.FragmentDefinition;
import graphql.language.OperationDefinition;
import graphql.schema.GraphQLSchema;
import org.dataloader.DataLoaderRegistry;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static graphql.Assert.assertNotNull;

@PublicApi
public class ExecutionContextBuilder {

    private Instrumentation instrumentation;
    private ExecutionId executionId;
    private InstrumentationState instrumentationState;
    private GraphQLSchema graphQLSchema;
    private ExecutionStrategy queryStrategy;
    private ExecutionStrategy mutationStrategy;
    private ExecutionStrategy subscriptionStrategy;
    private Object context;
    private Object root;
    private Document document;
    private OperationDefinition operationDefinition;
    private Map<String, Object> variables = new LinkedHashMap<>();
    private Map<String, FragmentDefinition> fragmentsByName = new LinkedHashMap<>();
    private DataLoaderRegistry dataLoaderRegistry;
    private CacheControl cacheControl;
    private List<GraphQLError> errors = new ArrayList<>();

    /**
     * @return a new builder of {@link graphql.execution.ExecutionContext}s
     */
    public static ExecutionContextBuilder newExecutionContextBuilder() {
        return new ExecutionContextBuilder();
    }

    /**
     * Creates a new builder based on a previous execution context
     *
     * @param other the previous execution to clone
     *
     * @return a new builder of {@link graphql.execution.ExecutionContext}s
     */
    public static ExecutionContextBuilder newExecutionContextBuilder(ExecutionContext other) {
        return new ExecutionContextBuilder(other);
    }

    @Internal
    public ExecutionContextBuilder() {
    }

    @Internal
    ExecutionContextBuilder(ExecutionContext other) {
        instrumentation = other.getInstrumentation();
        executionId = other.getExecutionId();
        instrumentationState = other.getInstrumentationState();
        graphQLSchema = other.getGraphQLSchema();
        queryStrategy = other.getQueryStrategy();
        mutationStrategy = other.getMutationStrategy();
        subscriptionStrategy = other.getSubscriptionStrategy();
        context = other.getContext();
        root = other.getRoot();
        document = other.getDocument();
        operationDefinition = other.getOperationDefinition();
        variables = new LinkedHashMap<>(other.getVariables());
        fragmentsByName = new LinkedHashMap<>(other.getFragmentsByName());
        dataLoaderRegistry = other.getDataLoaderRegistry();
        cacheControl = other.getCacheControl();
        errors = new ArrayList<>(other.getErrors());
    }

    public ExecutionContextBuilder instrumentation(Instrumentation instrumentation) {
        this.instrumentation = instrumentation;
        return this;
    }

    public ExecutionContextBuilder instrumentationState(InstrumentationState instrumentationState) {
        this.instrumentationState = instrumentationState;
        return this;
    }

    public ExecutionContextBuilder executionId(ExecutionId executionId) {
        this.executionId = executionId;
        return this;
    }

    public ExecutionContextBuilder graphQLSchema(GraphQLSchema graphQLSchema) {
        this.graphQLSchema = graphQLSchema;
        return this;
    }

    public ExecutionContextBuilder queryStrategy(ExecutionStrategy queryStrategy) {
        this.queryStrategy = queryStrategy;
        return this;
    }

    public ExecutionContextBuilder mutationStrategy(ExecutionStrategy mutationStrategy) {
        this.mutationStrategy = mutationStrategy;
        return this;
    }

    public ExecutionContextBuilder subscriptionStrategy(ExecutionStrategy subscriptionStrategy) {
        this.subscriptionStrategy = subscriptionStrategy;
        return this;
    }

    public ExecutionContextBuilder context(Object context) {
        this.context = context;
        return this;
    }

    public ExecutionContextBuilder root(Object root) {
        this.root = root;
        return this;
    }

    public ExecutionContextBuilder variables(Map<String, Object> variables) {
        this.variables = variables;
        return this;
    }

    public ExecutionContextBuilder fragmentsByName(Map<String, FragmentDefinition> fragmentsByName) {
        this.fragmentsByName = fragmentsByName;
        return this;
    }

    public ExecutionContextBuilder document(Document document) {
        this.document = document;
        return this;
    }

    public ExecutionContextBuilder operationDefinition(OperationDefinition operationDefinition) {
        this.operationDefinition = operationDefinition;
        return this;
    }

    public ExecutionContextBuilder dataLoaderRegistry(DataLoaderRegistry dataLoaderRegistry) {
        this.dataLoaderRegistry = assertNotNull(dataLoaderRegistry);
        return this;
    }

    public ExecutionContextBuilder cacheControl(CacheControl cacheControl) {
        this.cacheControl = cacheControl;
        return this;
    }

    public ExecutionContext build() {
        // preconditions
        assertNotNull(executionId, "You must provide a query identifier");

        return new ExecutionContext(
                instrumentation,
                executionId,
                graphQLSchema,
                instrumentationState,
                queryStrategy,
                mutationStrategy,
                subscriptionStrategy,
                fragmentsByName,
                document,
                operationDefinition,
                variables,
                context,
                root,
                dataLoaderRegistry,
                cacheControl,
                errors
        );
    }

}
