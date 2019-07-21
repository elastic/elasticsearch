package graphql;

import graphql.cachecontrol.CacheControl;
import graphql.execution.ExecutionId;
import org.dataloader.DataLoaderRegistry;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static graphql.Assert.assertNotNull;

/**
 * This represents the series of values that can be input on a graphql query execution
 */
@PublicApi
public class ExecutionInput {
    private final String query;
    private final String operationName;
    private final Object context;
    private final Object root;
    private final Map<String, Object> variables;
    private final DataLoaderRegistry dataLoaderRegistry;
    private final CacheControl cacheControl;
    private final ExecutionId executionId;


    @Internal
    private ExecutionInput(String query, String operationName, Object context, Object root, Map<String, Object> variables, DataLoaderRegistry dataLoaderRegistry, CacheControl cacheControl, ExecutionId executionId) {
        this.query = assertNotNull(query, "query can't be null");
        this.operationName = operationName;
        this.context = context;
        this.root = root;
        this.variables = variables;
        this.dataLoaderRegistry = dataLoaderRegistry;
        this.cacheControl = cacheControl;
        this.executionId = executionId;
    }

    /**
     * @return the query text
     */
    public String getQuery() {
        return query;
    }

    /**
     * @return the name of the query operation
     */
    public String getOperationName() {
        return operationName;
    }

    /**
     * @return the context object to pass to all data fetchers
     */
    public Object getContext() {
        return context;
    }

    /**
     * @return the root object to start the query execution on
     */
    public Object getRoot() {
        return root;
    }

    /**
     * @return a map of variables that can be referenced via $syntax in the query
     */
    public Map<String, Object> getVariables() {
        return variables;
    }

    /**
     * @return the data loader registry associated with this execution
     */
    public DataLoaderRegistry getDataLoaderRegistry() {
        return dataLoaderRegistry;
    }

    /**
     * @return the cache control helper associated with this execution
     */
    public CacheControl getCacheControl() {
        return cacheControl;
    }

    /**
     * @return Id that will be/was used to execute this operation.
     */
    public ExecutionId getExecutionId() {
        return executionId;
    }

    /**
     * This helps you transform the current ExecutionInput object into another one by starting a builder with all
     * the current values and allows you to transform it how you want.
     *
     * @param builderConsumer the consumer code that will be given a builder to transform
     *
     * @return a new ExecutionInput object based on calling build on that builder
     */
    public ExecutionInput transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder()
                .query(this.query)
                .operationName(this.operationName)
                .context(this.context)
                .root(this.root)
                .dataLoaderRegistry(this.dataLoaderRegistry)
                .cacheControl(this.cacheControl)
                .variables(this.variables)
                .executionId(executionId);

        builderConsumer.accept(builder);

        return builder.build();
    }


    @Override
    public String toString() {
        return "ExecutionInput{" +
                "query='" + query + '\'' +
                ", operationName='" + operationName + '\'' +
                ", context=" + context +
                ", root=" + root +
                ", variables=" + variables +
                ", dataLoaderRegistry=" + dataLoaderRegistry +
                ", executionId= " + executionId +
                '}';
    }

    /**
     * @return a new builder of ExecutionInput objects
     */
    public static Builder newExecutionInput() {
        return new Builder();
    }

    /**
     * Creates a new builder of ExecutionInput objects with the given query
     *
     * @param query the query to execute
     *
     * @return a new builder of ExecutionInput objects
     */
    public static Builder newExecutionInput(String query) {
        return new Builder().query(query);
    }

    public static class Builder {

        private String query;
        private String operationName;
        private Object context = GraphQLContext.newContext().build();
        private Object root;
        private Map<String, Object> variables = Collections.emptyMap();
        private DataLoaderRegistry dataLoaderRegistry = new DataLoaderRegistry();
        private CacheControl cacheControl = CacheControl.newCacheControl();
        private ExecutionId executionId = null;

        public Builder query(String query) {
            this.query = assertNotNull(query, "query can't be null");
            return this;
        }

        public Builder operationName(String operationName) {
            this.operationName = operationName;
            return this;
        }

        /**
         * A default one will be assigned, but you can set your own.
         *
         * @param executionId an execution id object
         *
         * @return this builder
         */
        public Builder executionId(ExecutionId executionId) {
            this.executionId = executionId;
            return this;
        }

        /**
         * By default you will get a {@link GraphQLContext} object but you can set your own.
         *
         * @param context the context object to use
         *
         * @return this builder
         */
        public Builder context(Object context) {
            this.context = context;
            return this;
        }

        public Builder context(GraphQLContext.Builder contextBuilder) {
            this.context = contextBuilder.build();
            return this;
        }

        public Builder context(UnaryOperator<GraphQLContext.Builder> contextBuilderFunction) {
            GraphQLContext.Builder builder = GraphQLContext.newContext();
            builder = contextBuilderFunction.apply(builder);
            return context(builder.build());
        }

        public Builder root(Object root) {
            this.root = root;
            return this;
        }

        public Builder variables(Map<String, Object> variables) {
            this.variables = assertNotNull(variables, "variables map can't be null");
            return this;
        }

        /**
         * You should create new {@link org.dataloader.DataLoaderRegistry}s and new {@link org.dataloader.DataLoader}s for each execution.  Do not
         * re-use
         * instances as this will create unexpected results.
         *
         * @param dataLoaderRegistry a registry of {@link org.dataloader.DataLoader}s
         *
         * @return this builder
         */
        public Builder dataLoaderRegistry(DataLoaderRegistry dataLoaderRegistry) {
            this.dataLoaderRegistry = assertNotNull(dataLoaderRegistry);
            return this;
        }

        public Builder cacheControl(CacheControl cacheControl) {
            this.cacheControl = assertNotNull(cacheControl);
            return this;
        }

        public ExecutionInput build() {
            return new ExecutionInput(query, operationName, context, root, variables, dataLoaderRegistry, cacheControl, executionId);
        }
    }
}