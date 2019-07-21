package graphql.util;

import graphql.Internal;

@Internal
public class TraverserResult {

    private final Object accumulatedResult;

    public TraverserResult(Object accumulatedResult) {
        this.accumulatedResult = accumulatedResult;
    }

    public Object getAccumulatedResult() {
        return accumulatedResult;
    }

}
