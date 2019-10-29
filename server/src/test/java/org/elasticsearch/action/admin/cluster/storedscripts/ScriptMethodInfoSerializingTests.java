/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptContextInfo.ScriptMethodInfo;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ScriptMethodInfoSerializingTests extends AbstractSerializingTestCase<ScriptMethodInfo> {
    private static final String EXECUTE = "execute";
    private static final String GET_PREFIX = "get";
    private static final int MIN_LENGTH = 1;
    private static final int MAX_LENGTH = 16;

    enum NameType {
        EXECUTE,
        GETTER,
        OTHER;
        static NameType fromName(String name) {
            if (name.equals(ScriptMethodInfoSerializingTests.EXECUTE)) {
                return EXECUTE;
            } else if (name.startsWith(GET_PREFIX)) {
                return GETTER;
            }
            return OTHER;
        }
    }

    @Override
    protected ScriptMethodInfo doParseInstance(XContentParser parser) throws IOException {
        return ScriptMethodInfo.fromXContent(parser);
    }

    @Override
    protected ScriptMethodInfo createTestInstance() {
        return randomInstance(NameType.OTHER);
    }

    @Override
    protected Writeable.Reader<ScriptMethodInfo> instanceReader() { return ScriptMethodInfo::new; }

    @Override
    protected ScriptMethodInfo mutateInstance(ScriptMethodInfo instance) throws IOException {
        return mutate(instance);
    }

    static ScriptMethodInfo randomInstance(NameType type) {
        switch (type) {
            case EXECUTE:
                return new ScriptMethodInfo(
                    EXECUTE,
                    randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH),
                    ScriptParameterInfoSerializingTests.randomInstances()
                );
            case GETTER:
                return new ScriptMethodInfo(
                    GET_PREFIX + randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH),
                    randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH),
                    Collections.unmodifiableList(new ArrayList<>())
                );
            default:
                return new ScriptMethodInfo(
                    randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH),
                    randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH),
                    ScriptParameterInfoSerializingTests.randomInstances()
                );
        }
    }

    static ScriptMethodInfo mutate(ScriptMethodInfo instance) {
        switch (NameType.fromName(instance.name)) {
            case EXECUTE:
                if (randomBoolean()) {
                    return new ScriptMethodInfo(
                        instance.name,
                        instance.returnType + randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH),
                        instance.parameters
                    );
                }
                return new ScriptMethodInfo(
                    instance.name,
                    instance.returnType,
                    ScriptParameterInfoSerializingTests.mutateOne(instance.parameters)
                );
            case GETTER:
                return new ScriptMethodInfo(
                    instance.name,
                    instance.returnType + randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH),
                    instance.parameters
                );
            default:
                switch (randomIntBetween(0, 2)) {
                    case 0:
                        return new ScriptMethodInfo(
                            instance.name + randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH),
                            instance.returnType,
                            instance.parameters
                        );
                    case 1:
                        return new ScriptMethodInfo(
                            instance.name,
                            instance.returnType + randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH),
                            instance.parameters
                        );
                    default:
                        return new ScriptMethodInfo(
                            instance.name,
                            instance.returnType,
                            ScriptParameterInfoSerializingTests.mutateOne(instance.parameters)
                        );
                }
        }
    }

    static Set<ScriptMethodInfo> mutateOneGetter(Set<ScriptMethodInfo> instances) {
        if (instances.size() == 0) {
            return Set.of(randomInstance(NameType.GETTER));
        }
        ArrayList<ScriptMethodInfo> mutated = new ArrayList<>(instances);
        int mutateIndex = randomIntBetween(0, instances.size() - 1);
        mutated.set(mutateIndex, mutate(mutated.get(mutateIndex)));
        return Set.copyOf(mutated);
    }

    static Set<ScriptMethodInfo> randomGetterInstances() {
        Set<String> suffixes = new HashSet<>();
        int numGetters = randomIntBetween(0, MAX_LENGTH);
        Set<ScriptMethodInfo> getters = new HashSet<>(numGetters);
        for (int i = 0; i < numGetters; i++) {
            String suffix = randomValueOtherThanMany(suffixes::contains, () -> randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH));
            suffixes.add(suffix);
            getters.add(new ScriptMethodInfo(
                GET_PREFIX + suffix,
                randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH),
                Collections.unmodifiableList(new ArrayList<>())
            ));
        }
        return Collections.unmodifiableSet(getters);
    }
}
