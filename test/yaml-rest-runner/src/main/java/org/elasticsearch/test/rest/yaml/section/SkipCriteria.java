/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SkipCriteria {

    public static final Predicate<ClientYamlTestExecutionContext> SKIP_ALWAYS = context -> true;

    private SkipCriteria() {}

    static Predicate<ClientYamlTestExecutionContext> fromVersionRange(String versionRange) {
        final List<VersionRange> versionRanges = VersionRange.parseVersionRanges(versionRange);
        assert versionRanges.isEmpty() == false;
        return new Predicate<>() {
            @Override
            public boolean test(ClientYamlTestExecutionContext context) {
                return versionRanges.stream().anyMatch(range -> range.contains(context.esVersion()));
            }

            @Override
            public String toString() {
                return versionRanges.stream().map(VersionRange::toString).collect(Collectors.joining(";"));
            }
        };
    }

    static Predicate<ClientYamlTestExecutionContext> fromOsList(List<String> operatingSystems) {
        return context -> operatingSystems.stream().anyMatch(osName -> osName.equals(context.os()));
    }
}
