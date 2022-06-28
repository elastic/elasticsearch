/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.action.InternalInferModelAction.Request;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ResultsFieldUpdateTests;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InternalInferModelActionRequestTests extends AbstractBWCWireSerializationTestCase<Request> {

    @Override
    @SuppressWarnings("unchecked")
    protected Request createTestInstance() {
        return randomBoolean()
            ? new Request(
                randomAlphaOfLength(10),
                Stream.generate(InternalInferModelActionRequestTests::randomMap).limit(randomInt(10)).collect(Collectors.toList()),
                randomInferenceConfigUpdate(),
                randomBoolean()
            )
            : new Request(randomAlphaOfLength(10), randomMap(), randomInferenceConfigUpdate(), randomBoolean());
    }

    private static InferenceConfigUpdate randomInferenceConfigUpdate() {
        return randomFrom(
            RegressionConfigUpdateTests.randomRegressionConfigUpdate(),
            ClassificationConfigUpdateTests.randomClassificationConfigUpdate(),
            ResultsFieldUpdateTests.randomUpdate(),
            EmptyConfigUpdateTests.testInstance()
        );
    }

    private static Map<String, Object> randomMap() {
        return Stream.generate(() -> randomAlphaOfLength(10))
            .limit(randomInt(10))
            .collect(Collectors.toMap(Function.identity(), (v) -> randomAlphaOfLength(10)));
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected boolean isCompatible(Request instance, Version version) {
        if (version.before(Version.V_7_8_0)) {
            boolean isSupportedType = instance.getUpdate() instanceof RegressionConfigUpdate
                || instance.getUpdate() instanceof ClassificationConfigUpdate;
            return isSupportedType;
        }
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Request mutateInstanceForVersion(Request instance, Version version) {
        if (version.before(Version.V_7_8_0)) {
            InferenceConfigUpdate update = null;
            if (instance.getUpdate() instanceof ClassificationConfigUpdate) {
                update = ClassificationConfigUpdate.fromConfig(
                    ClassificationConfigTests.mutateForVersion((ClassificationConfig) instance.getUpdate().toConfig(), version)
                );
            } else if (instance.getUpdate() instanceof RegressionConfigUpdate) {
                update = RegressionConfigUpdate.fromConfig(
                    RegressionConfigTests.mutateForVersion((RegressionConfig) instance.getUpdate().toConfig(), version)
                );
            } else {
                fail("unknown update type " + instance.getUpdate().getName());
            }
            return new Request(instance.getModelId(), instance.getObjectsToInfer(), update, instance.isPreviouslyLicensed());
        }
        return instance;
    }
}
