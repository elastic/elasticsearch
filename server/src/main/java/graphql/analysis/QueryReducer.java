package graphql.analysis;

import graphql.PublicApi;

/**
 * Used by {@link QueryTraverser} to reduce the fields of a Document (or part of it) to a single value.
 * <p>
 * How this happens in detail (pre vs post-order for example) is defined by {@link QueryTraverser}.
 * <p>
 * See {@link QueryTraverser#reducePostOrder(QueryReducer, Object)} and {@link QueryTraverser#reducePreOrder(QueryReducer, Object)}
 */
@PublicApi
@FunctionalInterface
public interface QueryReducer<T> {

    /**
     * Called each time a field is visited.
     *
     * @param fieldEnvironment the environment to this call
     * @param acc              the previous result
     *
     * @return the new result
     */
    T reduceField(QueryVisitorFieldEnvironment fieldEnvironment, T acc);
}
