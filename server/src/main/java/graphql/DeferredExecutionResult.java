package graphql;

import java.util.List;

/**
 * Results that come back from @defer fields have an extra path property that tells you where
 * that deferred result came in the original query
 */
@PublicApi
public interface DeferredExecutionResult extends ExecutionResult {

    /**
     * @return the execution path of this deferred result in the original query
     */
    List<Object> getPath();
}
