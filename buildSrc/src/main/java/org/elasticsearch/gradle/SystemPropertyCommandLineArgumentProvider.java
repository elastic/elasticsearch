package org.elasticsearch.gradle;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.gradle.api.tasks.Input;
import org.gradle.process.CommandLineArgumentProvider;

public class SystemPropertyCommandLineArgumentProvider implements CommandLineArgumentProvider {
    private final Map<String, Object> systemProperties = new LinkedHashMap<>();

    public void systemProperty(String key, Object value) {
        systemProperties.put(key, value);
    }

    @Override
    public Iterable<String> asArguments() {
        return systemProperties.entrySet()
            .stream()
            .map(entry -> "-D" + entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.toList());
    }

    // Track system property keys as an input so our build cache key will change if we add
    // properties but values are still ignored
    @Input
    public Iterable<String> getPropertyNames() {
        return systemProperties.keySet();
    }
}
