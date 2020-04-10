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
import org.elasticsearch.script.ScriptContextInfo;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class ScriptContextInfoSerializingTests extends AbstractSerializingTestCase<ScriptContextInfo> {
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
    protected Writeable.Reader<ScriptContextInfo> instanceReader() { return ScriptContextInfo::new; }


    @Override
    protected ScriptContextInfo mutateInstance(ScriptContextInfo instance) throws IOException {
        return mutate(instance, null);
    }

    private static ScriptContextInfo mutate(ScriptContextInfo instance, Set<String> names) {
        if (names == null) {
            names = new HashSet<>();
            names.add(instance.name);
        }
        switch (randomIntBetween(0, 2)) {
            case 0:
                return new ScriptContextInfo(
                    randomValueOtherThanMany(names::contains, () -> randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH)),
                    instance.execute,
                    instance.getters
                );
            case 1:
                return new ScriptContextInfo(
                    instance.name,
                    ScriptMethodInfoSerializingTests.mutate(instance.execute),
                    instance.getters
                );
            default:
                return new ScriptContextInfo(
                    instance.name,
                    instance.execute,
                    ScriptMethodInfoSerializingTests.mutateOneGetter(instance.getters)
                );
        }
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
        HashSet<ScriptContextInfo> instances = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            String name = randomValueOtherThanMany(names::contains, () -> randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH));
            names.add(name);
            instances.add(new ScriptContextInfo(
                name,
                ScriptMethodInfoSerializingTests.randomInstance(ScriptMethodInfoSerializingTests.NameType.EXECUTE),
                ScriptMethodInfoSerializingTests.randomGetterInstances()
            ));
        }
        return Collections.unmodifiableSet(instances);
    }
}
