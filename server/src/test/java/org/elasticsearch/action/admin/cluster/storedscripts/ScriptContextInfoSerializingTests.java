/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.script.ScriptContextInfo;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class ScriptContextInfoSerializingTests extends AbstractXContentSerializingTestCase<ScriptContextInfo> {
    private static final int MIN_LENGTH = 1;
    private static final int MAX_LENGTH = 16;

    @Override
    protected ScriptContextInfo doParseInstance(XContentParser parser) throws IOException {
        return ScriptContextInfo.fromXContent(parser);
    }

    @Override
    protected ScriptContextInfo createTestInstance() {
        return randomInstance();
    }

    @Override
    protected Writeable.Reader<ScriptContextInfo> instanceReader() {
        return ScriptContextInfo::new;
    }

    @Override
    protected ScriptContextInfo mutateInstance(ScriptContextInfo instance) {
        return mutate(instance, null);
    }

    private static ScriptContextInfo mutate(ScriptContextInfo instance, Set<String> names) {
        if (names == null) {
            names = new HashSet<>();
            names.add(instance.name);
        }
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new ScriptContextInfo(
                randomValueOtherThanMany(names::contains, () -> randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH)),
                instance.execute,
                instance.getters
            );
            case 1 -> new ScriptContextInfo(instance.name, ScriptMethodInfoSerializingTests.mutate(instance.execute), instance.getters);
            default -> new ScriptContextInfo(
                instance.name,
                instance.execute,
                ScriptMethodInfoSerializingTests.mutateOneGetter(instance.getters)
            );
        };
    }

    static Set<ScriptContextInfo> mutateOne(Collection<ScriptContextInfo> instances) {
        if (instances.size() == 0) {
            return Collections.unmodifiableSet(Set.of(randomInstance()));
        }
        ArrayList<ScriptContextInfo> mutated = new ArrayList<>(instances);
        int mutateIndex = randomIntBetween(0, instances.size() - 1);
        mutated.set(mutateIndex, mutate(mutated.get(mutateIndex), instances.stream().map(i -> i.name).collect(Collectors.toSet())));
        return Set.copyOf(mutated);
    }

    static ScriptContextInfo randomInstance() {
        return new ScriptContextInfo(
            randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH),
            ScriptMethodInfoSerializingTests.randomInstance(ScriptMethodInfoSerializingTests.NameType.EXECUTE),
            ScriptMethodInfoSerializingTests.randomGetterInstances()
        );
    }

    static Set<ScriptContextInfo> randomInstances() {
        Set<String> names = new HashSet<>();
        int size = randomIntBetween(0, MAX_LENGTH);
        Set<ScriptContextInfo> instances = Sets.newHashSetWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            String name = randomValueOtherThanMany(names::contains, () -> randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH));
            names.add(name);
            instances.add(
                new ScriptContextInfo(
                    name,
                    ScriptMethodInfoSerializingTests.randomInstance(ScriptMethodInfoSerializingTests.NameType.EXECUTE),
                    ScriptMethodInfoSerializingTests.randomGetterInstances()
                )
            );
        }
        return Collections.unmodifiableSet(instances);
    }
}
