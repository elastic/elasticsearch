package com.carrotsearch.gradle.randomizedtesting

class StackTraceFiltersConfiguration {
    List<String> patterns = new ArrayList<>()
    List<String> contains = new ArrayList<>()

    void regex(String pattern) {
        patterns.add(pattern)
    }

    void contains(String contain) {
        contains.add(contain)
    }
}
