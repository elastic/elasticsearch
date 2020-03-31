package org.elasticsearch.gradle;

import org.gradle.api.tasks.Input;
import org.gradle.process.CommandLineArgumentProvider;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SystemPropertyCommandLineArgumentProvider implements CommandLineArgumentProvider {
    private final Map<String, Object> systemProperties = new LinkedHashMap<>();

    public void systemProperty(String key, Object value) {
        systemProperties.put(key, value);
    }

    @Override
    public Iterable<String> asArguments() {
        return systemProperties.entrySet()
            .stream()
            .map(entry -> "-D" + entry.getKey() + "=" +
                (entry.getValue() instanceof Supplier ? ((Supplier)entry.getValue()).get() : entry.getValue()))
            .collect(Collectors.toList());
    }

    // Track system property keys as an input so our build cache key will change if we add properties but values are still ignored
    @Input
    public Iterable<String> getPropertyNames() {
        return systemProperties.keySet();
    }
}
