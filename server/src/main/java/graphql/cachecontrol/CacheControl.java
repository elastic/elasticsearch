package graphql.cachecontrol;

import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.PublicApi;
import graphql.execution.ExecutionPath;
import graphql.schema.DataFetchingEnvironment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static graphql.Assert.assertNotNull;
import static graphql.util.FpKit.map;

/**
 * This class implements the graphql Cache Control specification as outlined in https://github.com/apollographql/apollo-cache-control
 * <p>
 * To best use this class you need to pass a CacheControl object to each {@link graphql.schema.DataFetcher} and have them decide on
 * the caching hint values.
 * <p>
 * The easiest why to do this is create a CacheControl object at query start and pass it in as a "context" object via {@link graphql.ExecutionInput#getContext()} and then have
 * each {@link graphql.schema.DataFetcher} thats wants to make cache control hints use that.
 * <p>
 * Then at the end of the query you would call {@link #addTo(graphql.ExecutionResult)} to record the cache control hints into the {@link graphql.ExecutionResult}
 * extensions map as per the specification.
 */
@PublicApi
public class CacheControl {

    public static final String CACHE_CONTROL_EXTENSION_KEY = "cacheControl";

    /**
     * If the scope is set to PRIVATE, this indicates anything under this path should only be cached per-user,
     * unless the value is overridden on a sub path. PUBLIC is the default and means anything under this path
     * can be stored in a shared cache.
     */
    public enum Scope {
        PUBLIC, PRIVATE
    }

    private class Hint {
        private final List<Object> path;
        private final Integer maxAge;
        private final Scope scope;

        private Hint(List<Object> path, Integer maxAge, Scope scope) {
            this.path = assertNotNull(path);
            this.maxAge = maxAge;
            this.scope = scope;
        }

        Map<String, Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("path", path);
            if (maxAge != null) {
                map.put("maxAge", maxAge);
            }
            if (scope != null) {
                map.put("scope", scope.name());
            }
            return map;
        }
    }

    private final List<Hint> hints;

    private CacheControl() {
        hints = new CopyOnWriteArrayList<>();
    }


    /**
     * This creates a cache control hint for the specified path
     *
     * @param path   the path to the field that has the cache control hint
     * @param maxAge the caching time in seconds
     * @param scope  the scope of the cache control hint
     *
     * @return this object builder style
     */
    public CacheControl hint(ExecutionPath path, Integer maxAge, Scope scope) {
        assertNotNull(path);
        assertNotNull(scope);
        hints.add(new Hint(path.toList(), maxAge, scope));
        return this;
    }

    /**
     * This creates a cache control hint for the specified path
     *
     * @param path  the path to the field that has the cache control hint
     * @param scope the scope of the cache control hint
     *
     * @return this object builder style
     */
    public CacheControl hint(ExecutionPath path, Scope scope) {
        return hint(path, null, scope);
    }

    /**
     * This creates a cache control hint for the specified path
     *
     * @param path   the path to the field that has the cache control hint
     * @param maxAge the caching time in seconds
     *
     * @return this object builder style
     */
    public CacheControl hint(ExecutionPath path, Integer maxAge) {
        return hint(path, maxAge, Scope.PUBLIC);
    }

    /**
     * This creates a cache control hint for the specified field being fetched
     *
     * @param dataFetchingEnvironment the path to the field that has the cache control hint
     * @param maxAge                  the caching time in seconds
     * @param scope                   the scope of the cache control hint
     *
     * @return this object builder style
     */
    public CacheControl hint(DataFetchingEnvironment dataFetchingEnvironment, Integer maxAge, Scope scope) {
        assertNotNull(dataFetchingEnvironment);
        assertNotNull(scope);
        hint(dataFetchingEnvironment.getExecutionStepInfo().getPath(), maxAge, scope);
        return this;
    }

    /**
     * This creates a cache control hint for the specified field being fetched with a PUBLIC scope
     *
     * @param dataFetchingEnvironment the path to the field that has the cache control hint
     * @param maxAge                  the caching time in seconds
     *
     * @return this object builder style
     */
    public CacheControl hint(DataFetchingEnvironment dataFetchingEnvironment, Integer maxAge) {
        hint(dataFetchingEnvironment, maxAge, Scope.PUBLIC);
        return this;
    }

    /**
     * This creates a cache control hint for the specified field being fetched with a specified scope
     *
     * @param dataFetchingEnvironment the path to the field that has the cache control hint
     * @param scope                   the scope of the cache control hint
     *
     * @return this object builder style
     */
    public CacheControl hint(DataFetchingEnvironment dataFetchingEnvironment, Scope scope) {
        return hint(dataFetchingEnvironment, null, scope);
    }

    /**
     * Creates a new CacheControl object that can be used to trick caching hints
     *
     * @return the new object
     */
    public static CacheControl newCacheControl() {
        return new CacheControl();
    }

    /**
     * This will record the values in the cache control object into the provided execution result object which creates a new {@link graphql.ExecutionResult}
     * object back out
     *
     * @param executionResult the starting execution result object
     *
     * @return a new execution result with the hints in the extensions map.
     */
    public ExecutionResult addTo(ExecutionResult executionResult) {
        return ExecutionResultImpl.newExecutionResult()
                                  .from(executionResult)
                                  .addExtension(CACHE_CONTROL_EXTENSION_KEY, hintsToCacheControlProperties())
                                  .build();
    }

    private Map<String, Object> hintsToCacheControlProperties() {
        List<Map<String, Object>> recordedHints = map(hints, Hint::toMap);

        Map<String, Object> cacheControl = new LinkedHashMap<>();
        cacheControl.put("version", 1);
        cacheControl.put("hints", recordedHints);
        return cacheControl;
    }
}
