package org.elasticsearch.gradle.test;

import groovy.lang.Closure;
import org.gradle.api.tasks.testing.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ESTest extends Test {

    private final Map<String, Supplier<String>> systemProperties = new HashMap<>();

    public ESTest() {
        super();
        // disable the build cache for rest test tasks
        // there are a number of inputs we aren't properly tracking here so we'll just not cache these for now
        getOutputs().doNotCacheIf(
            "Caching is disabled for REST integration tests",
            (task) -> false
        );
        getJvmArgumentProviders().add(() -> systemProperties.entrySet().stream()
            .map(entry -> "-D" + entry.getKey() + "=" + entry.getValue().get())
            .collect(Collectors.toUnmodifiableList())
        );
    }

    @Override
    public Test systemProperties(Map<String, ?> properties) {
        properties.forEach((key, value) -> systemProperty(key, value));
        return this;
    }

    @Override
    public Test systemProperty(String name, Object value) {
        if (value instanceof Supplier) {
            systemProperties.put(name, (Supplier<String>) value);
        } else if (value instanceof Closure) {
            systemProperties.put(name, () -> ((Closure) value).call().toString());
        } else {
            systemProperties.put(name, () -> value.toString());
        }
        return this;
    }
}
