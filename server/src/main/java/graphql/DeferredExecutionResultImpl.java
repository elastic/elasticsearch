package graphql;

import graphql.execution.ExecutionPath;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static graphql.Assert.assertNotNull;

/**
 * Results that come back from @defer fields have an extra path property that tells you where
 * that deferred result came in the original query
 */
@PublicApi
public class DeferredExecutionResultImpl extends ExecutionResultImpl implements DeferredExecutionResult {

    private final List<Object> path;

    private DeferredExecutionResultImpl(List<Object> path, ExecutionResultImpl executionResult) {
        super(executionResult);
        this.path = assertNotNull(path);
    }

    /**
     * @return the execution path of this deferred result in the original query
     */
    public List<Object> getPath() {
        return path;
    }

    @Override
    public Map<String, Object> toSpecification() {
        Map<String, Object> map = new LinkedHashMap<>(super.toSpecification());
        map.put("path", path);
        return map;
    }

    public static Builder newDeferredExecutionResult() {
        return new Builder();
    }

    public static class Builder {
        private List<Object> path = Collections.emptyList();
        private ExecutionResultImpl.Builder builder = ExecutionResultImpl.newExecutionResult();

        public Builder path(ExecutionPath path) {
            this.path = assertNotNull(path).toList();
            return this;
        }

        public Builder from(ExecutionResult executionResult) {
            builder.from((ExecutionResultImpl) executionResult);
            return this;
        }

        public Builder addErrors(List<GraphQLError> errors) {
            builder.addErrors(errors);
            return this;
        }

        public DeferredExecutionResult build() {
            ExecutionResultImpl build = builder.build();
            return new DeferredExecutionResultImpl(path, build);
        }
    }
}
