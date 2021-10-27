/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.runtimefields;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Base64;

public class RuntimeFieldsFeatureSetUsageTests extends ESTestCase {

    public void testSerializationBWC() throws IOException {
        String stats = "DnJ1bnRpbWVfZmllbGRzAQEBBHR5cGUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==";
        byte[] bytes = Base64.getDecoder().decode(stats);
        RuntimeFieldsFeatureSetUsage deserialized = deserialize(
            bytes,
            VersionUtils.randomVersionBetween(random(), RuntimeFieldsFeatureSetUsage.MINIMAL_SUPPORTED_VERSION, Version.V_7_12_0)
        );

        Version version = VersionUtils.randomVersionBetween(
            random(),
            RuntimeFieldsFeatureSetUsage.MINIMAL_SUPPORTED_VERSION,
            Version.CURRENT
        );
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(version);
        deserialized.writeTo(out);
        deserialize(out.bytes().toBytesRef().bytes, version);
    }

    private static RuntimeFieldsFeatureSetUsage deserialize(byte[] bytes, Version version) throws IOException {
        StreamInput in = StreamInput.wrap(bytes);
        in.setVersion(version);
        RuntimeFieldsFeatureSetUsage deserialized = new RuntimeFieldsFeatureSetUsage(in);
        assertNotNull(deserialized);
        return deserialized;
    }
}
