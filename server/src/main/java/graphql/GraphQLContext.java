package graphql;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import static graphql.Assert.assertNotNull;


/**
 * This context object can be used to contain key values that can be useful as "context" when executing
 * {@link graphql.schema.DataFetcher}s
 *
 * <pre>
 * {@code
 *     DataFetcher df = new DataFetcher() {
 *        public Object get(DataFetchingEnvironment env) {
 *            GraphQLContext ctx = env.getContext()
 *            User currentUser = ctx.getOrDefault("userKey",new AnonymousUser())
 *            ...
 *        }
 *     }
 * }
 * </pre>
 *
 * You can set this up via {@link ExecutionInput.Builder#context(graphql.GraphQLContext.Builder)}
 */
@PublicApi
@ThreadSafe
@SuppressWarnings("unchecked")
public class GraphQLContext {

    private final ConcurrentMap<Object, Object> map;

    private GraphQLContext(ConcurrentMap<Object, Object> map) {
        this.map = map;
    }

    public void delete(Object key) {
        map.remove(assertNotNull(key));
    }

    public <T> T get(Object key) {
        return (T) map.get(assertNotNull(key));
    }

    public <T> T getOrDefault(Object key, T defaultValue) {
        return (T) map.getOrDefault(assertNotNull(key), defaultValue);
    }

    public <T> Optional<T> getOrEmpty(Object key) {
        T t = (T) map.get(assertNotNull(key));
        return Optional.ofNullable(t);
    }

    public boolean hasKey(Object key) {
        return map.containsKey(assertNotNull(key));
    }

    public void put(Object key, Object value) {
        map.put(assertNotNull(key), assertNotNull(value));
    }

    public void putAll(GraphQLContext context) {
        assertNotNull(context);
        for (Map.Entry<Object, Object> entry : context.map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    public Stream<Map.Entry<Object, Object>> stream() {
        return map.entrySet().stream();
    }

    public static Builder newContext() {
        return new Builder();
    }

    public static class Builder {
        private final ConcurrentMap<Object, Object> map = new ConcurrentHashMap<>();

        public Builder of(
                Object key1, Object value1
        ) {
            return putImpl(
                    key1, value1
            );
        }

        public Builder of(
                Object key1, Object value1,
                Object key2, Object value2
        ) {
            return putImpl(
                    key1, value1,
                    key2, value2
            );
        }

        public Builder of(
                Object key1, Object value1,
                Object key2, Object value2,
                Object key3, Object value3
        ) {
            return putImpl(
                    key1, value1,
                    key2, value2,
                    key3, value3
            );
        }

        public Builder of(
                Object key1, Object value1,
                Object key2, Object value2,
                Object key3, Object value3,
                Object key4, Object value4
        ) {
            return putImpl(
                    key1, value1,
                    key2, value2,
                    key3, value3,
                    key4, value4
            );
        }

        public Builder of(
                Object key1, Object value1,
                Object key2, Object value2,
                Object key3, Object value3,
                Object key4, Object value4,
                Object key5, Object value5
        ) {
            return putImpl(
                    key1, value1,
                    key2, value2,
                    key3, value3,
                    key4, value4,
                    key5, value5
            );
        }

        private Builder putImpl(Object... kvs) {
            for (int i = 0; i < kvs.length; i = i + 2) {
                Object k = kvs[i];
                Object v = kvs[i + 1];
                map.put(assertNotNull(k), assertNotNull(v));
            }
            return this;
        }

        public GraphQLContext build() {
            return new GraphQLContext(map);
        }
    }
}
