package graphql.execution;

import graphql.PublicApi;
import graphql.language.SourceLocation;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;

import java.util.Map;

/**
 * The parameters available to {@link DataFetcherExceptionHandler}s
 */
@PublicApi
public class DataFetcherExceptionHandlerParameters {

    private final DataFetchingEnvironment dataFetchingEnvironment;
    private final Throwable exception;

    private DataFetcherExceptionHandlerParameters(Builder builder) {
        this.exception = builder.exception;
        this.dataFetchingEnvironment = builder.dataFetchingEnvironment;
    }

    public Throwable getException() {
        return exception;
    }

    public ExecutionPath getPath() {
        return dataFetchingEnvironment.getExecutionStepInfo().getPath();
    }

    public DataFetchingEnvironment getDataFetchingEnvironment() {
        return dataFetchingEnvironment;
    }

    public MergedField getField() {
        return dataFetchingEnvironment.getMergedField();
    }

    public GraphQLFieldDefinition getFieldDefinition() {
        return dataFetchingEnvironment.getFieldDefinition();
    }

    public Map<String, Object> getArgumentValues() {
        return dataFetchingEnvironment.getArguments();
    }

    public SourceLocation getSourceLocation() {
        return getField().getSingleField().getSourceLocation();
    }

    public static Builder newExceptionParameters() {
        return new Builder();
    }

    public static class Builder {
        DataFetchingEnvironment dataFetchingEnvironment;
        Throwable exception;

        private Builder() {
        }

        public Builder dataFetchingEnvironment(DataFetchingEnvironment dataFetchingEnvironment) {
            this.dataFetchingEnvironment = dataFetchingEnvironment;
            return this;
        }

        public Builder exception(Throwable exception) {
            this.exception = exception;
            return this;
        }

        public DataFetcherExceptionHandlerParameters build() {
            return new DataFetcherExceptionHandlerParameters(this);
        }
    }
}
