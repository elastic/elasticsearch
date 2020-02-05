package org.elasticsearch.gradle.test;

import java.util.ArrayList;
import java.util.List;

public class RestApiSpecExtension {
    private List<String> includesCoreSpec = new ArrayList<>();
    private List<String> includesCoreTests = new ArrayList<>();
    private List<String> includesXpackSpec = new ArrayList<>();
    private List<String> includesXpackTests = new ArrayList<>();
    private boolean alwaysCopySpec = false;
    private boolean copyTests = false;
    private boolean copyXpackTests = false;

    public void includeCoreSpec(String include) {
        includesCoreSpec.add(include);
    }

    public List<String> getIncludesCoreSpec() {
        return includesCoreSpec;
    }

    public void includeCoreTests(String include) {
        includesCoreTests.add(include);
    }

    public List<String> getIncludesCoreTests() {
        return includesCoreTests;
    }

    public void includeXpackSpec(String include) {
        includesXpackSpec.add(include);
    }

    public List<String> getIncludesXpackSpec() {
        return includesXpackSpec;
    }

    public void copyCoreTests(boolean copyTests) {
        this.copyTests = copyTests;
    }

    public boolean shouldCopyCoreTests() {
        return copyTests;
    }

    public void includeXpackTests(String include) {
        includesXpackTests.add(include);
    }

    public List<String> getIncludesXpackTests() {
        return includesXpackTests;
    }

    public void copyXpackTests(boolean copyTests) {
        this.copyXpackTests = copyTests;
    }

    public boolean shouldCopyXpackTests() {
        return copyXpackTests;
    }

    public boolean shouldAlwaysCopySpec() {
        return alwaysCopySpec;
    }

    public void alwaysCopySpec(boolean alwaysCopySpec) {
        this.alwaysCopySpec = alwaysCopySpec;
    }
}
