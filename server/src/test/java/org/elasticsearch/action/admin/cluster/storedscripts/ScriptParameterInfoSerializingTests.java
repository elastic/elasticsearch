/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.script.ScriptContextInfo.ScriptMethodInfo.ParameterInfo;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ScriptParameterInfoSerializingTests extends AbstractXContentSerializingTestCase<ParameterInfo> {
    private static int minLength = 1;
    private static int maxLength = 8;
    private static String baseType = "type-";
    private static String baseName = "name-";

    @Override
    protected ParameterInfo doParseInstance(XContentParser parser) throws IOException {
        return ParameterInfo.fromXContent(parser);
    }

    @Override
    protected ParameterInfo createTestInstance() {
        return randomInstance();
    }

    @Override
    protected Writeable.Reader<ParameterInfo> instanceReader() {
        return ParameterInfo::new;
    }

    @Override
    protected ParameterInfo mutateInstance(ParameterInfo instance) {
        return mutate(instance);
    }

    private static ParameterInfo mutate(ParameterInfo instance) {
        if (randomBoolean()) {
            return new ParameterInfo(instance.type + randomAlphaOfLengthBetween(minLength, maxLength), instance.name);
        }
        return new ParameterInfo(instance.type, instance.name + randomAlphaOfLengthBetween(minLength, maxLength));
    }

    static List<ParameterInfo> mutateOne(List<ParameterInfo> instances) {
        if (instances.size() == 0) {
            return Collections.unmodifiableList(List.of(randomInstance()));
        }
        ArrayList<ParameterInfo> mutated = new ArrayList<>(instances);
        int mutateIndex = randomIntBetween(0, instances.size() - 1);
        mutated.set(mutateIndex, mutate(instances.get(mutateIndex)));
        return Collections.unmodifiableList(mutated);
    }

    static ParameterInfo randomInstance() {
        return new ParameterInfo(
            baseType + randomAlphaOfLengthBetween(minLength, maxLength),
            baseName + randomAlphaOfLengthBetween(minLength, maxLength)
        );
    }

    static List<ParameterInfo> randomInstances() {
        Set<String> suffixes = new HashSet<>();
        int size = randomIntBetween(0, maxLength);
        ArrayList<ParameterInfo> instances = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String suffix = randomValueOtherThanMany(suffixes::contains, () -> randomAlphaOfLengthBetween(minLength, maxLength));
            suffixes.add(suffix);
            instances.add(new ParameterInfo(baseType + randomAlphaOfLengthBetween(minLength, maxLength), baseName + suffix));
        }
        return Collections.unmodifiableList(instances);
    }
}
